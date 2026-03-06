"""Tests for Bronze row-mapping functions (no Spark dependency)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from skill_radar.domains.adzuna.api.client import PageResult
from skill_radar.domains.adzuna.bronze.extract import (
    _map_job_to_bronze_row,
    _map_page_to_request_log_row,
    _safe_float,
)

FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent.parent / "fixtures" / "adzuna"


@pytest.fixture
def sample_job() -> dict[str, Any]:
    """Load the first job from the valid search response fixture."""
    with (FIXTURES_DIR / "valid_search_response.json").open() as f:
        data = json.load(f)
    return data["results"][0]


@pytest.fixture
def null_job() -> dict[str, Any]:
    """Load the sparse job (most fields null) from the null-fields fixture."""
    with (FIXTURES_DIR / "null_fields_response.json").open() as f:
        data = json.load(f)
    return data["results"][1]


LINEAGE_KWARGS = {
    "source_system": "adzuna",
    "country": "fr",
    "preset": "default_fr",
    "search_key": "country=fr|preset=default_fr",
    "search_params_json": "{}",
    "page": 1,
    "results_per_page": 50,
    "position": 0,
    "extracted_at_utc": "2025-01-15T12:00:00+00:00",
    "ingestion_date": "2025-01-15",
    "run_id": "test-run-001",
}


class TestMapJobToBronzeRow:
    """_map_job_to_bronze_row maps API JSON to flat dict."""

    def test_all_identity_fields(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        assert row["job_id"] == "4001234567"
        assert row["adref"] == "abc123def456"

    def test_text_fields(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        assert "Python" in row["title"]
        assert "développeur" in row["description"].lower()
        assert row["redirect_url"].startswith("https://")

    def test_numeric_fields(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        assert row["salary_min"] == 45000.0
        assert row["salary_max"] == 65000.0
        assert row["latitude"] == pytest.approx(48.8566, abs=0.01)
        assert row["longitude"] == pytest.approx(2.3522, abs=0.01)

    def test_nested_location_fields(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        assert row["location_display_name"] == "Paris, Île-de-France"
        area = json.loads(row["location_area_json"])
        assert "France" in area
        assert "Paris" in area

    def test_nested_category_fields(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        assert row["category_tag"] == "it-jobs"
        assert row["category_label"] == "IT Jobs"

    def test_company_fields(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        assert row["company_display_name"] == "DataTech Solutions"

    def test_raw_payload_json_is_valid(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        parsed = json.loads(row["raw_payload_json"])
        assert parsed["id"] == "4001234567"

    def test_lineage_columns(self, sample_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(sample_job, **LINEAGE_KWARGS)
        assert row["source_system"] == "adzuna"
        assert row["country"] == "fr"
        assert row["preset"] == "default_fr"
        assert row["run_id"] == "test-run-001"
        assert row["ingestion_date"] == "2025-01-15"
        assert row["page"] == 1
        assert row["results_per_page"] == 50
        assert row["api_result_position"] == 0

    def test_null_fields_dont_crash(self, null_job: dict[str, Any]) -> None:
        row = _map_job_to_bronze_row(null_job, **LINEAGE_KWARGS)
        assert row["salary_min"] is None
        assert row["salary_max"] is None
        assert row["latitude"] is None
        assert row["longitude"] is None
        assert row["contract_time_raw"] is None or row["contract_time_raw"] == ""
        assert row["job_id"] == "4009999902"


class TestSafeFloat:
    """_safe_float coercion utility."""

    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            (45000.0, 45000.0),
            ("45000", 45000.0),
            (0, 0.0),
            (None, None),
            ("", None),
            ("not_a_number", None),
            (True, 1.0),
        ],
    )
    def test_coercion(self, input_val: Any, expected: float | None) -> None:
        result = _safe_float(input_val)
        if expected is None:
            assert result is None
        else:
            assert result == expected


class TestMapPageToRequestLogRow:
    """_map_page_to_request_log_row maps PageResult to log row."""

    def test_log_row_fields(self) -> None:
        pr = PageResult(
            page=2,
            results=[{"id": "1"}, {"id": "2"}],
            result_count=200,
            http_status=200,
            request_params={"country": "fr", "page": 2},
            request_started_at_utc="2025-01-15T12:00:00+00:00",
            request_finished_at_utc="2025-01-15T12:00:01+00:00",
            duration_ms=1000,
            success=True,
        )
        row = _map_page_to_request_log_row(
            pr,
            source_system="adzuna",
            country="fr",
            preset="default_fr",
            run_id="run-123",
            ingestion_date="2025-01-15",
        )
        assert row["page"] == 2
        assert row["response_count"] == 2
        assert row["http_status"] == 200
        assert row["success"] is True
        assert row["run_id"] == "run-123"
        assert row["ingestion_date"] == "2025-01-15"
        assert row["country"] == "fr"
        # request_params_json should be valid JSON
        parsed = json.loads(row["request_params_json"])
        assert parsed["country"] == "fr"
