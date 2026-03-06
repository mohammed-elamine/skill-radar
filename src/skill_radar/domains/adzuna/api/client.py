"""Adzuna REST API client — synchronous, pagination-aware.

Responsibilities:
- Construct authenticated Adzuna requests
- Handle pagination across result pages
- Retry transient failures with bounded exponential backoff
- Return parsed JSON payloads
- Capture request parameters for lineage

This module has **no** Spark dependency and **no** persistence logic.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from collections.abc import Iterator

from .errors import (
    AdzunaAuthError,
    AdzunaClientError,
    AdzunaResponseError,
    AdzunaServerError,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------


@dataclass
class PageResult:
    """Result of fetching a single page from the Adzuna API."""

    page: int
    results: list[dict[str, Any]]
    result_count: int
    http_status: int
    request_params: dict[str, Any]
    request_started_at_utc: str
    request_finished_at_utc: str
    duration_ms: int
    success: bool
    error_message: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SearchResult:
    """Aggregated result of a multi-page search."""

    country: str
    preset: str
    search_key: str
    pages_fetched: int = 0
    total_results: int = 0
    all_results: list[dict[str, Any]] = field(default_factory=list)
    page_results: list[PageResult] = field(default_factory=list)
    success: bool = True
    error_message: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "country": self.country,
            "preset": self.preset,
            "search_key": self.search_key,
            "pages_fetched": self.pages_fetched,
            "total_results": self.total_results,
            "result_count": len(self.all_results),
            "success": self.success,
            "error_message": self.error_message,
        }


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class AdzunaClient:
    """Synchronous HTTP client for the Adzuna Search API.

    Parameters
    ----------
    app_id:
        Adzuna application ID.
    app_key:
        Adzuna application key.
    base_url:
        API base URL (e.g. ``https://api.adzuna.com/v1/api``).
    results_per_page:
        Number of results per page (max 50).
    timeout_seconds:
        HTTP request timeout.
    max_retries:
        Maximum retries for transient failures.
    backoff_seconds:
        Base delay between retries (multiplied by attempt number).
    """

    def __init__(
        self,
        *,
        app_id: str,
        app_key: str,
        base_url: str = "https://api.adzuna.com/v1/api",
        results_per_page: int = 50,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        backoff_seconds: int = 2,
    ) -> None:
        self._app_id = app_id
        self._app_key = app_key
        self._base_url = base_url.rstrip("/")
        self._results_per_page = results_per_page
        self._timeout = timeout_seconds
        self._max_retries = max_retries
        self._backoff = backoff_seconds
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # -- public API -------------------------------------------------------

    def search_jobs(
        self,
        country: str,
        *,
        page: int = 1,
        what: str = "",
        where: str = "",
        category: str = "",
        max_days_old: int | None = None,
        sort_by: str | None = None,
        full_time: bool | None = None,
        part_time: bool | None = None,
        salary_min: float | None = None,
        salary_max: float | None = None,
    ) -> PageResult:
        """Fetch a single page of job search results.

        Returns a :class:`PageResult` with parsed results and lineage metadata.
        """
        params = self._build_params(
            what=what,
            where=where,
            category=category,
            max_days_old=max_days_old,
            sort_by=sort_by,
            full_time=full_time,
            part_time=part_time,
            salary_min=salary_min,
            salary_max=salary_max,
        )
        url = f"{self._base_url}/jobs/{country}/search/{page}"

        logger.info(
            "Fetching Adzuna page: country=%s page=%d results_per_page=%d",
            country,
            page,
            self._results_per_page,
        )

        start = datetime.now(UTC)
        data, status = self._request_with_retry(url, params)
        end = datetime.now(UTC)
        duration_ms = int((end - start).total_seconds() * 1000)

        results = data.get("results", [])
        result_count = data.get("count", len(results))

        logger.info(
            "Fetched Adzuna page: country=%s page=%d results=%d total=%d duration=%dms",
            country,
            page,
            len(results),
            result_count,
            duration_ms,
        )

        return PageResult(
            page=page,
            results=results,
            result_count=result_count,
            http_status=status,
            request_params={
                "country": country,
                "page": page,
                "results_per_page": self._results_per_page,
                **{k: v for k, v in params.items() if k not in ("app_id", "app_key")},
            },
            request_started_at_utc=start.isoformat(),
            request_finished_at_utc=end.isoformat(),
            duration_ms=duration_ms,
            success=True,
        )

    def iter_search_pages(
        self,
        country: str,
        *,
        max_pages: int = 20,
        what: str = "",
        where: str = "",
        category: str = "",
        max_days_old: int | None = None,
        sort_by: str | None = None,
        full_time: bool | None = None,
        part_time: bool | None = None,
        salary_min: float | None = None,
        salary_max: float | None = None,
    ) -> Iterator[PageResult]:
        """Iterate over pages of search results up to ``max_pages``."""
        for page_num in range(1, max_pages + 1):
            page_result = self.search_jobs(
                country,
                page=page_num,
                what=what,
                where=where,
                category=category,
                max_days_old=max_days_old,
                sort_by=sort_by,
                full_time=full_time,
                part_time=part_time,
                salary_min=salary_min,
                salary_max=salary_max,
            )
            yield page_result

            # Stop if we got fewer results than requested (last page).
            if len(page_result.results) < self._results_per_page:
                logger.info(
                    "Received %d results (< %d): assuming last page, stopping.",
                    len(page_result.results),
                    self._results_per_page,
                )
                break

    def search_all(
        self,
        country: str,
        preset: str,
        *,
        max_pages: int = 20,
        what: str = "",
        where: str = "",
        category: str = "",
        max_days_old: int | None = None,
        sort_by: str | None = None,
        full_time: bool | None = None,
        part_time: bool | None = None,
        salary_min: float | None = None,
        salary_max: float | None = None,
    ) -> SearchResult:
        """Run a full multi-page search and return an aggregated result."""
        search_key = self._make_search_key(country, preset, what, where, category)
        result = SearchResult(country=country, preset=preset, search_key=search_key)

        try:
            for page_result in self.iter_search_pages(
                country,
                max_pages=max_pages,
                what=what,
                where=where,
                category=category,
                max_days_old=max_days_old,
                sort_by=sort_by,
                full_time=full_time,
                part_time=part_time,
                salary_min=salary_min,
                salary_max=salary_max,
            ):
                result.page_results.append(page_result)
                result.all_results.extend(page_result.results)
                result.pages_fetched += 1
                result.total_results = page_result.result_count
        except (AdzunaAuthError, AdzunaClientError, AdzunaServerError) as exc:
            result.success = False
            result.error_message = str(exc)
            logger.error("Search failed: %s", exc)

        return result

    # -- internals --------------------------------------------------------

    def _build_params(
        self,
        *,
        what: str,
        where: str,
        category: str,
        max_days_old: int | None,
        sort_by: str | None,
        full_time: bool | None,
        part_time: bool | None,
        salary_min: float | None,
        salary_max: float | None,
    ) -> dict[str, Any]:
        """Build query parameters for the API request."""
        params: dict[str, Any] = {
            "app_id": self._app_id,
            "app_key": self._app_key,
            "results_per_page": self._results_per_page,
            "content-type": "application/json",
        }
        if what:
            params["what"] = what
        if where:
            params["where"] = where
        if category:
            params["category"] = category
        if max_days_old is not None:
            params["max_days_old"] = max_days_old
        if sort_by:
            params["sort_by"] = sort_by
        if full_time is not None:
            params["full_time"] = "1" if full_time else "0"
        if part_time is not None:
            params["part_time"] = "1" if part_time else "0"
        if salary_min is not None:
            params["salary_min"] = salary_min
        if salary_max is not None:
            params["salary_max"] = salary_max
        return params

    def _request_with_retry(
        self,
        url: str,
        params: dict[str, Any],
    ) -> tuple[dict[str, Any], int]:
        """Execute an HTTP GET with retry logic for transient failures."""
        last_exc: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
                return self._handle_response(resp)
            except (AdzunaAuthError, AdzunaClientError):
                raise
            except AdzunaServerError as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    delay = self._backoff * attempt
                    logger.warning(
                        "Transient error (attempt %d/%d), retrying in %ds: %s",
                        attempt,
                        self._max_retries,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
            except requests.RequestException as exc:
                last_exc = AdzunaServerError(
                    f"Network error on attempt {attempt}/{self._max_retries}: {exc}"
                )
                if attempt < self._max_retries:
                    delay = self._backoff * attempt
                    logger.warning(
                        "Network error (attempt %d/%d), retrying in %ds: %s",
                        attempt,
                        self._max_retries,
                        delay,
                        exc,
                    )
                    time.sleep(delay)

        raise last_exc or AdzunaServerError("Request failed after all retries")

    def _handle_response(
        self,
        resp: requests.Response,
    ) -> tuple[dict[str, Any], int]:
        """Parse and validate an HTTP response."""
        status = resp.status_code

        if status == 401 or status == 403:
            raise AdzunaAuthError(
                f"Authentication failed (HTTP {status}). Check ADZUNA_APP_ID and ADZUNA_APP_KEY."
            )

        if 400 <= status < 500:
            raise AdzunaClientError(f"Client error (HTTP {status}): {resp.text[:500]}")

        if status >= 500:
            raise AdzunaServerError(f"Server error (HTTP {status}): {resp.text[:300]}")

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise AdzunaResponseError(
                f"Malformed JSON response: {exc}. Body prefix: {resp.text[:200]}"
            ) from exc

        if not isinstance(data, dict):
            raise AdzunaResponseError(f"Expected JSON object, got {type(data).__name__}")

        return data, status

    @staticmethod
    def _make_search_key(
        country: str,
        preset: str,
        what: str,
        where: str,
        category: str,
    ) -> str:
        """Build a deterministic search key for lineage tracking."""
        parts = [f"country={country}", f"preset={preset}"]
        if what:
            parts.append(f"what={what}")
        if where:
            parts.append(f"where={where}")
        if category:
            parts.append(f"category={category}")
        return "|".join(parts)
