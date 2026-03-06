"""Tests for the Adzuna API client — parameter construction, pagination, and error handling."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from skill_radar.domains.adzuna.api.client import AdzunaClient, PageResult, SearchResult
from skill_radar.domains.adzuna.api.errors import (
    AdzunaAuthError,
    AdzunaClientError,
    AdzunaResponseError,
    AdzunaServerError,
)

FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent.parent / "fixtures" / "adzuna"


@pytest.fixture
def valid_response_data() -> dict[str, Any]:
    """Load the valid search response fixture."""
    with (FIXTURES_DIR / "valid_search_response.json").open() as f:
        return json.load(f)


@pytest.fixture
def null_fields_data() -> dict[str, Any]:
    """Load the null fields response fixture."""
    with (FIXTURES_DIR / "null_fields_response.json").open() as f:
        return json.load(f)


@pytest.fixture
def client() -> AdzunaClient:
    """Create an AdzunaClient with test credentials."""
    return AdzunaClient(
        app_id="test_id",
        app_key="test_key",
        base_url="https://api.adzuna.com/v1/api",
        results_per_page=50,
        timeout_seconds=5,
        max_retries=2,
        backoff_seconds=0,  # No delay in tests
    )


def _mock_response(data: dict[str, Any], status_code: int = 200) -> MagicMock:
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = data
    resp.text = json.dumps(data)
    return resp


class TestBuildParams:
    """_build_params constructs correct query parameters."""

    def test_minimal_params(self, client: AdzunaClient) -> None:
        params = client._build_params(
            what="",
            where="",
            category="",
            max_days_old=None,
            sort_by=None,
            full_time=None,
            part_time=None,
            salary_min=None,
            salary_max=None,
        )
        assert params["app_id"] == "test_id"
        assert params["app_key"] == "test_key"
        assert params["results_per_page"] == 50
        # Optional keys should not be present
        assert "what" not in params
        assert "where" not in params
        assert "category" not in params

    def test_full_params(self, client: AdzunaClient) -> None:
        params = client._build_params(
            what="python",
            where="paris",
            category="it-jobs",
            max_days_old=7,
            sort_by="date",
            full_time=True,
            part_time=False,
            salary_min=30000,
            salary_max=60000,
        )
        assert params["what"] == "python"
        assert params["where"] == "paris"
        assert params["category"] == "it-jobs"
        assert params["max_days_old"] == 7
        assert params["sort_by"] == "date"
        assert params["full_time"] == "1"
        assert params["part_time"] == "0"
        assert params["salary_min"] == 30000
        assert params["salary_max"] == 60000

    def test_secrets_present_in_params(self, client: AdzunaClient) -> None:
        params = client._build_params(
            what="",
            where="",
            category="",
            max_days_old=None,
            sort_by=None,
            full_time=None,
            part_time=None,
            salary_min=None,
            salary_max=None,
        )
        assert "app_id" in params
        assert "app_key" in params


class TestSearchJobs:
    """search_jobs calls the API and returns PageResult."""

    def test_returns_page_result(
        self,
        client: AdzunaClient,
        valid_response_data: dict[str, Any],
    ) -> None:
        mock_resp = _mock_response(valid_response_data)
        with patch.object(client._session, "get", return_value=mock_resp):
            result = client.search_jobs("fr", page=1)

        assert isinstance(result, PageResult)
        assert result.success is True
        assert result.page == 1
        assert len(result.results) == 3
        assert result.result_count == 3
        assert result.http_status == 200

    def test_request_params_exclude_secrets(
        self,
        client: AdzunaClient,
        valid_response_data: dict[str, Any],
    ) -> None:
        mock_resp = _mock_response(valid_response_data)
        with patch.object(client._session, "get", return_value=mock_resp):
            result = client.search_jobs("fr")

        # Lineage params in result should NOT contain secrets
        assert "app_id" not in result.request_params
        assert "app_key" not in result.request_params
        assert result.request_params["country"] == "fr"

    def test_null_fields_page(
        self,
        client: AdzunaClient,
        null_fields_data: dict[str, Any],
    ) -> None:
        mock_resp = _mock_response(null_fields_data)
        with patch.object(client._session, "get", return_value=mock_resp):
            result = client.search_jobs("fr")

        assert result.success is True
        assert len(result.results) == 2


class TestErrorHandling:
    """HTTP error classification and retry behavior."""

    def test_401_raises_auth_error(self, client: AdzunaClient) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 401
        resp.text = "Unauthorized"
        with (
            patch.object(client._session, "get", return_value=resp),
            pytest.raises(AdzunaAuthError, match="Authentication failed"),
        ):
            client.search_jobs("fr")

    def test_403_raises_auth_error(self, client: AdzunaClient) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 403
        resp.text = "Forbidden"
        with (
            patch.object(client._session, "get", return_value=resp),
            pytest.raises(AdzunaAuthError),
        ):
            client.search_jobs("fr")

    def test_400_raises_client_error(self, client: AdzunaClient) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 400
        resp.text = "Bad Request"
        with (
            patch.object(client._session, "get", return_value=resp),
            pytest.raises(AdzunaClientError, match="Client error"),
        ):
            client.search_jobs("fr")

    def test_500_retries_and_raises_server_error(self, client: AdzunaClient) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 500
        resp.text = "Internal Server Error"
        with (
            patch.object(client._session, "get", return_value=resp),
            pytest.raises(AdzunaServerError, match="Server error"),
        ):
            client.search_jobs("fr")

    def test_malformed_json_raises_response_error(self, client: AdzunaClient) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.side_effect = json.JSONDecodeError("Fail", "", 0)
        resp.text = "not json"
        with (
            patch.object(client._session, "get", return_value=resp),
            pytest.raises(AdzunaResponseError, match="Malformed JSON"),
        ):
            client.search_jobs("fr")

    def test_non_dict_response_raises_response_error(self, client: AdzunaClient) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = [1, 2, 3]  # list instead of dict
        resp.text = "[1,2,3]"
        with (
            patch.object(client._session, "get", return_value=resp),
            pytest.raises(AdzunaResponseError, match="Expected JSON object"),
        ):
            client.search_jobs("fr")


class TestPagination:
    """iter_search_pages and search_all handle multi-page retrieval."""

    def _page_factory(self, total: int, rpp: int) -> list[dict[str, Any]]:
        """Create sequential mock response data split across pages."""
        pages = []
        for page_idx in range(0, total, rpp):
            chunk_size = min(rpp, total - page_idx)
            results = [
                {"id": str(page_idx + i), "title": f"Job {page_idx + i}"} for i in range(chunk_size)
            ]
            pages.append({"count": total, "results": results})
        return pages

    def test_iter_stops_on_partial_page(self, client: AdzunaClient) -> None:
        """When a page returns fewer results than rpp, iteration stops."""
        pages_data = self._page_factory(total=80, rpp=50)
        call_idx = 0

        def mock_get(*_args: Any, **_kwargs: Any) -> MagicMock:
            nonlocal call_idx
            resp = _mock_response(pages_data[call_idx])
            call_idx += 1
            return resp

        with patch.object(client._session, "get", side_effect=mock_get):
            results = list(client.iter_search_pages("fr", max_pages=5))

        assert len(results) == 2  # page 1: 50, page 2: 30 (partial → stop)
        assert len(results[0].results) == 50
        assert len(results[1].results) == 30

    def test_iter_respects_max_pages(self, client: AdzunaClient) -> None:
        """Pagination stops after max_pages even if pages are full."""
        full_page = {"count": 10000, "results": [{"id": str(i)} for i in range(50)]}
        mock_resp = _mock_response(full_page)

        with patch.object(client._session, "get", return_value=mock_resp):
            results = list(client.iter_search_pages("fr", max_pages=3))

        assert len(results) == 3

    def test_search_all_aggregates(self, client: AdzunaClient) -> None:
        pages_data = self._page_factory(total=80, rpp=50)
        call_idx = 0

        def mock_get(*_args: Any, **_kwargs: Any) -> MagicMock:
            nonlocal call_idx
            resp = _mock_response(pages_data[call_idx])
            call_idx += 1
            return resp

        with patch.object(client._session, "get", side_effect=mock_get):
            result = client.search_all("fr", "default_fr", max_pages=5)

        assert isinstance(result, SearchResult)
        assert result.success is True
        assert result.pages_fetched == 2
        assert len(result.all_results) == 80
        assert result.country == "fr"
        assert result.preset == "default_fr"

    def test_search_all_captures_error(self, client: AdzunaClient) -> None:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 401
        resp.text = "Unauthorized"

        with patch.object(client._session, "get", return_value=resp):
            result = client.search_all("fr", "default_fr", max_pages=1)

        assert result.success is False
        assert "Authentication" in result.error_message


class TestSearchKey:
    """_make_search_key builds deterministic lineage keys."""

    def test_minimal(self) -> None:
        key = AdzunaClient._make_search_key("fr", "default_fr", "", "", "")
        assert key == "country=fr|preset=default_fr"

    def test_with_filters(self) -> None:
        key = AdzunaClient._make_search_key("gb", "custom", "python", "london", "it-jobs")
        assert "country=gb" in key
        assert "what=python" in key
        assert "where=london" in key
        assert "category=it-jobs" in key
