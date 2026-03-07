"""Unit tests for search models (BulkIndexResult, SearchExportResult)."""

from __future__ import annotations

from skill_radar.platform.search.models import BulkIndexResult, SearchExportResult


class TestBulkIndexResult:
    """Tests for BulkIndexResult dataclass."""

    def test_defaults(self) -> None:
        r = BulkIndexResult(index_name="test-index")
        assert r.index_name == "test-index"
        assert r.total_documents == 0
        assert r.success_count == 0
        assert r.error_count == 0
        assert r.errors == []

    def test_success_property_all_ok(self) -> None:
        r = BulkIndexResult(
            index_name="test-idx",
            total_documents=100,
            success_count=100,
            error_count=0,
        )
        assert r.success is True

    def test_success_property_with_failures(self) -> None:
        r = BulkIndexResult(
            index_name="test-idx",
            total_documents=100,
            success_count=95,
            error_count=5,
            errors=["doc_42: mapper_parsing_exception"],
        )
        assert r.success is False


class TestSearchExportResult:
    """Tests for SearchExportResult dataclass."""

    def test_defaults(self) -> None:
        r = SearchExportResult()
        assert r.success is True
        assert r.datasets_exported == []
        assert r.run_id == ""
        assert r.results == {}
        assert r.error == ""

    def test_success_flag(self) -> None:
        r = SearchExportResult(success=True, datasets_exported=["a", "b"])
        assert r.success is True

    def test_failure_with_error(self) -> None:
        r = SearchExportResult(
            success=False,
            error="skill_demand_daily: bulk indexing failed",
        )
        assert r.success is False
        assert r.error != ""
