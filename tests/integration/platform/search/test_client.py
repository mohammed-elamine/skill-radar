"""Integration tests for the Elasticsearch search client.

These tests require a running Elasticsearch instance.
Run with: make search-up && uv run pytest tests/integration/platform/search/ -v
"""

from __future__ import annotations

import os
import uuid

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def _es_available() -> bool:
    """Check if Elasticsearch is reachable."""
    from skill_radar.config.models import SearchConfig

    cfg = SearchConfig()
    # Use env override if set
    es_url = os.environ.get("SKILLRADAR_SEARCH_ELASTICSEARCH_URL", cfg.elasticsearch_url)

    from skill_radar.platform.search.client import SearchClient

    client = SearchClient(es_url, timeout=5)
    try:
        return client.is_reachable()
    except Exception:
        return False


# Skip all tests if ES not available
skip_if_no_es = pytest.mark.skipif(
    not _es_available(),
    reason="Elasticsearch not available. Run `make search-up` first.",
)


@pytest.fixture(scope="module")
def search_client():
    """Create a SearchClient for testing."""
    from skill_radar.config.models import SearchConfig
    from skill_radar.platform.search.client import SearchClient

    cfg = SearchConfig()
    es_url = os.environ.get("SKILLRADAR_SEARCH_ELASTICSEARCH_URL", cfg.elasticsearch_url)
    return SearchClient(es_url, timeout=30)


@pytest.fixture(scope="module")
def test_index_name() -> str:
    """Generate a unique test index name."""
    return f"skillradar-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module", autouse=True)
def cleanup_test_index(search_client, test_index_name):
    """Clean up test index after all tests."""
    import contextlib

    yield
    # Cleanup
    with contextlib.suppress(Exception):
        search_client.delete_index(test_index_name)


@skip_if_no_es
class TestSearchClientIntegration:
    """Integration tests for SearchClient."""

    def test_is_reachable(self, search_client) -> None:
        """Test that Elasticsearch is reachable."""
        assert search_client.is_reachable() is True

    def test_cluster_health(self, search_client) -> None:
        """Test cluster health returns valid status."""
        health = search_client.cluster_health()
        assert "cluster_name" in health
        assert health.get("status") in ("green", "yellow", "red")

    def test_cluster_status(self, search_client) -> None:
        """Test cluster status returns a valid value."""
        status = search_client.cluster_status()
        assert status in ("green", "yellow", "red")

    def test_create_and_delete_index(self, search_client, test_index_name) -> None:
        """Test creating and deleting an index."""
        # Create
        mapping = {
            "properties": {
                "doc_id": {"type": "keyword"},
                "name": {"type": "text"},
            }
        }
        body = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": mapping,
        }
        search_client.create_index(test_index_name, body)
        assert search_client.index_exists(test_index_name) is True

        # Cleanup (done by fixture, but verify delete works)
        search_client.delete_index(test_index_name)
        assert search_client.index_exists(test_index_name) is False

    def test_bulk_index_and_search(self, search_client, test_index_name) -> None:
        """Test bulk indexing and searching documents."""
        # Create index
        body = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "name": {"type": "text"},
                    "count": {"type": "long"},
                }
            },
        }
        search_client.create_index(test_index_name, body)

        # Build bulk payload
        import json

        docs = [
            {"doc_id": "doc1", "name": "Python", "count": 100},
            {"doc_id": "doc2", "name": "Java", "count": 80},
            {"doc_id": "doc3", "name": "JavaScript", "count": 120},
        ]
        lines = []
        for doc in docs:
            action = {"index": {"_index": test_index_name, "_id": doc["doc_id"]}}
            lines.append(json.dumps(action))
            lines.append(json.dumps(doc))
        lines.append("")
        payload = "\n".join(lines)

        # Bulk index
        result = search_client.bulk(payload)
        assert result.get("errors", True) is False

        # Refresh to make docs searchable
        search_client.refresh_index(test_index_name)

        # Search
        query = {"query": {"match_all": {}}}
        search_result = search_client.search(test_index_name, query)
        hits = search_result.get("hits", {}).get("hits", [])
        assert len(hits) == 3

        # Cleanup
        search_client.delete_index(test_index_name)
