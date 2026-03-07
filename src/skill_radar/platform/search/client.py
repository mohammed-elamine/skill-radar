"""Thin Elasticsearch client wrapper.

Encapsulates cluster lifecycle operations behind a minimal API so that
domain code never imports the ``elasticsearch`` library directly.

All operations use the ``requests`` library for HTTP calls to keep
dependencies minimal (``requests`` is already a project dependency).
"""

from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


class SearchClient:
    """Lightweight Elasticsearch client using plain HTTP.

    Parameters
    ----------
    base_url:
        Elasticsearch base URL, e.g. ``http://localhost:9200``.
    timeout:
        Request timeout in seconds.
    """

    def __init__(self, base_url: str, *, timeout: int = 30) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    # ── Cluster health ────────────────────────────────────────────────

    def is_reachable(self) -> bool:
        """Return True if the cluster responds to a basic ping."""
        try:
            r = requests.get(self._base_url, timeout=self._timeout)
            return r.status_code == 200
        except Exception:
            return False

    def cluster_health(self) -> dict[str, Any]:
        """Return cluster health as a dict."""
        r = requests.get(
            f"{self._base_url}/_cluster/health",
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    def cluster_status(self) -> str:
        """Return cluster health status string (green/yellow/red)."""
        status: str = self.cluster_health().get("status", "unknown")
        return status

    # ── Index lifecycle ───────────────────────────────────────────────

    def index_exists(self, index: str) -> bool:
        """Check whether an index or alias exists."""
        r = requests.head(
            f"{self._base_url}/{index}",
            timeout=self._timeout,
        )
        return r.status_code == 200

    def create_index(self, index: str, body: dict) -> dict[str, Any]:
        """Create an index with explicit settings and mappings.

        Parameters
        ----------
        index:
            Physical index name.
        body:
            Index creation body containing ``settings`` and ``mappings``.
        """
        r = requests.put(
            f"{self._base_url}/{index}",
            json=body,
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    def delete_index(self, index: str) -> dict[str, Any]:
        """Delete an index."""
        r = requests.delete(
            f"{self._base_url}/{index}",
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    def get_mapping(self, index: str) -> dict[str, Any]:
        """Return the mapping for an index."""
        r = requests.get(
            f"{self._base_url}/{index}/_mapping",
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    def refresh_index(self, index: str) -> dict[str, Any]:
        """Force a refresh so indexed documents become searchable."""
        r = requests.post(
            f"{self._base_url}/{index}/_refresh",
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    def get_doc_count(self, index: str) -> int:
        """Return the document count for an index."""
        r = requests.get(
            f"{self._base_url}/{index}/_count",
            timeout=self._timeout,
        )
        r.raise_for_status()
        count: int = r.json().get("count", 0)
        return count

    # ── Alias management ──────────────────────────────────────────────

    def update_aliases(self, actions: list[dict[str, Any]]) -> dict[str, Any]:
        """Execute atomic alias swap via ``_aliases`` API.

        Parameters
        ----------
        actions:
            List of add/remove actions, e.g.::

                [
                    {"remove": {"index": "old-index", "alias": "my-alias"}},
                    {"add":    {"index": "new-index", "alias": "my-alias"}},
                ]
        """
        r = requests.post(
            f"{self._base_url}/_aliases",
            json={"actions": actions},
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    def get_alias(self, alias: str) -> dict[str, Any]:
        """Return indices associated with an alias."""
        r = requests.get(
            f"{self._base_url}/_alias/{alias}",
            timeout=self._timeout,
        )
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    # ── Bulk operations ───────────────────────────────────────────────

    def bulk(self, body: str) -> dict[str, Any]:
        """Submit a bulk request body (NDJSON string).

        Parameters
        ----------
        body:
            Newline-delimited JSON bulk request body.
        """
        r = requests.post(
            f"{self._base_url}/_bulk",
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    # ── Delete by query ───────────────────────────────────────────────

    def delete_by_query(self, index: str, query: dict) -> dict[str, Any]:
        """Delete documents matching a query."""
        r = requests.post(
            f"{self._base_url}/{index}/_delete_by_query",
            json={"query": query},
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result

    # ── Search ────────────────────────────────────────────────────────

    def search(self, index: str, body: dict) -> dict[str, Any]:
        """Execute a search query."""
        r = requests.post(
            f"{self._base_url}/{index}/_search",
            json=body,
            timeout=self._timeout,
        )
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result
