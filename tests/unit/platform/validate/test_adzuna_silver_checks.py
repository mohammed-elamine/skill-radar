"""Unit tests for Adzuna Silver partition-scoped validation checks.

Tests verify:
- Check factory returns correctly wired NamedCheck objects.
- Partition-filter helpers produce correct SQL fragments.
- DQ checks use the correct uniqueness grain (country, ingestion_date, job_id).
- Same job_id on different dates must NOT be flagged as a duplicate.
- Duplicate within the same partition MUST be flagged.
- Non-empty and key-population checks respect partition scope.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from skill_radar.config.models import PlatformSettings
from skill_radar.platform.validate.checks.adzuna import (
    _SILVER_LINEAGE_COLS,
    SILVER_JOBS_REQUIRED,
    _check_coordinate_sanity,
    _check_salary_consistency,
    _check_silver_duplicates,
    _check_silver_key_population,
    _check_silver_partition_non_empty,
    _partition_where_clause,
    get_silver_checks,
)
from skill_radar.platform.validate.models import CheckStatus, NamedCheck

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_row(mapping: dict) -> MagicMock:
    """Create a Row-like mock that supports [] access."""
    row = MagicMock()
    row.__getitem__ = lambda _self, key: mapping[key]
    return row


def _mock_spark_sql(return_rows: list[dict]) -> MagicMock:
    """Build a mock SparkSession whose .sql().collect() returns *return_rows*."""
    spark = MagicMock()
    rows = [_mock_row(r) for r in return_rows]
    spark.sql.return_value.collect.return_value = rows
    return spark


# ---------------------------------------------------------------------------
# _partition_where_clause
# ---------------------------------------------------------------------------


class TestPartitionWhereClause:
    """Tests for the SQL WHERE-fragment builder."""

    def test_no_filters(self) -> None:
        assert _partition_where_clause() == ""

    def test_country_only(self) -> None:
        result = _partition_where_clause(country="fr")
        assert result == " AND country = 'fr'"

    def test_ingestion_date_only(self) -> None:
        result = _partition_where_clause(ingestion_date="2025-01-15")
        assert result == " AND ingestion_date = '2025-01-15'"

    def test_both(self) -> None:
        result = _partition_where_clause(country="gb", ingestion_date="2025-06-01")
        assert "country = 'gb'" in result
        assert "ingestion_date = '2025-06-01'" in result
        # Must start with " AND "
        assert result.startswith(" AND ")


# ---------------------------------------------------------------------------
# get_silver_checks factory
# ---------------------------------------------------------------------------


class TestGetSilverChecksFactory:
    """get_silver_checks returns properly-wired NamedCheck objects."""

    @pytest.fixture
    def mock_spark(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def config(self) -> PlatformSettings:
        return PlatformSettings()

    def test_returns_list_of_named_checks(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_silver_checks(mock_spark, config, country="fr", ingestion_date="2025-01-15")
        assert isinstance(checks, list)
        assert all(isinstance(c, NamedCheck) for c in checks)

    def test_check_count(self, mock_spark: MagicMock, config: PlatformSettings) -> None:
        """Expect 4 structural + 6 partition-scoped = 10 checks."""
        checks = get_silver_checks(mock_spark, config, country="fr", ingestion_date="2025-01-15")
        assert len(checks) == 10

    def test_all_names_unique(self, mock_spark: MagicMock, config: PlatformSettings) -> None:
        checks = get_silver_checks(mock_spark, config, country="fr", ingestion_date="2025-01-15")
        names = [c.name for c in checks]
        assert len(names) == len(set(names)), f"Duplicate names: {names}"

    def test_all_checks_prefixed_with_adzuna_silver(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_silver_checks(mock_spark, config)
        for c in checks:
            assert c.name.startswith("adzuna.silver."), f"Bad prefix: {c.name}"

    def test_structural_checks_present(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_silver_checks(mock_spark, config, country="fr")
        names = {c.name for c in checks}
        assert "adzuna.silver.namespace_exists" in names
        assert "adzuna.silver.table_exists" in names
        assert "adzuna.silver.non_empty" in names
        assert "adzuna.silver.schema" in names

    def test_partition_scoped_checks_present(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_silver_checks(mock_spark, config, country="fr", ingestion_date="2025-01-15")
        names = {c.name for c in checks}
        assert "adzuna.silver.partition_non_empty" in names
        assert "adzuna.silver.key_population" in names
        assert "adzuna.silver.salary_consistency" in names
        assert "adzuna.silver.coordinate_sanity" in names
        assert "adzuna.silver.no_duplicates" in names
        assert "adzuna.silver.lineage" in names


# ---------------------------------------------------------------------------
# _check_silver_duplicates — correct uniqueness grain
# ---------------------------------------------------------------------------


class TestSilverDuplicateCheck:
    """Verifies the uniqueness grain is (country, ingestion_date, job_id)."""

    def test_no_duplicates_passes(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        result = _check_silver_duplicates(spark, "sr.sr_silver.adzuna_jobs")
        assert result.status == CheckStatus.PASS

    def test_duplicates_within_partition_fails(self) -> None:
        spark = _mock_spark_sql([{"cnt": 3}])
        result = _check_silver_duplicates(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        assert result.status == CheckStatus.FAIL
        assert result.metrics["duplicate_keys"] == 3

    def test_sql_groups_by_country_ingestion_date_job_id(self) -> None:
        """The SQL must GROUP BY (country, ingestion_date, job_id)."""
        spark = _mock_spark_sql([{"cnt": 0}])
        _check_silver_duplicates(spark, "sr.sr_silver.adzuna_jobs")
        sql_text = spark.sql.call_args[0][0]
        assert "GROUP BY country, ingestion_date, job_id" in sql_text

    def test_same_job_id_different_dates_not_duplicate(self) -> None:
        """GROUP BY includes ingestion_date → same job on two dates is two groups."""
        spark = _mock_spark_sql([{"cnt": 0}])
        result = _check_silver_duplicates(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        # When scoped to a single partition, duplicates of job_id within that
        # partition are caught; the same job_id on a *different* date won't
        # appear in this partition at all → cnt == 0 → PASS
        assert result.status == CheckStatus.PASS

    def test_partition_filter_appended_to_sql(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        _check_silver_duplicates(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        sql_text = spark.sql.call_args[0][0]
        assert "country = 'fr'" in sql_text
        assert "ingestion_date = '2025-01-15'" in sql_text

    def test_no_partition_no_filter(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        _check_silver_duplicates(spark, "sr.sr_silver.adzuna_jobs")
        sql_text = spark.sql.call_args[0][0]
        assert "country = " not in sql_text
        assert "ingestion_date = " not in sql_text

    def test_exception_returns_fail(self) -> None:
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("boom")
        result = _check_silver_duplicates(spark, "sr.sr_silver.adzuna_jobs")
        assert result.status == CheckStatus.FAIL
        assert "boom" in (result.detail or "")


# ---------------------------------------------------------------------------
# _check_silver_partition_non_empty
# ---------------------------------------------------------------------------


class TestSilverPartitionNonEmpty:
    """Partition-scoped non-empty check."""

    def test_non_empty_passes(self) -> None:
        spark = _mock_spark_sql([{"cnt": 42}])
        result = _check_silver_partition_non_empty(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        assert result.status == CheckStatus.PASS
        assert result.metrics["row_count"] == 42

    def test_empty_partition_fails(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        result = _check_silver_partition_non_empty(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        assert result.status == CheckStatus.FAIL

    def test_partition_filter_in_sql(self) -> None:
        spark = _mock_spark_sql([{"cnt": 1}])
        _check_silver_partition_non_empty(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="gb",
            ingestion_date="2025-06-01",
        )
        sql_text = spark.sql.call_args[0][0]
        assert "country = 'gb'" in sql_text
        assert "ingestion_date = '2025-06-01'" in sql_text


# ---------------------------------------------------------------------------
# _check_silver_key_population (partition-scoped)
# ---------------------------------------------------------------------------


class TestSilverKeyPopulation:
    """Key-population check uses partition filter."""

    def test_full_population_passes(self) -> None:
        # Two SQL calls: total, then with_keys
        spark = MagicMock()
        spark.sql.return_value.collect.side_effect = [
            [_mock_row({"cnt": 100})],
            [_mock_row({"cnt": 100})],
        ]
        result = _check_silver_key_population(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        assert result.status == CheckStatus.PASS

    def test_low_population_fails(self) -> None:
        spark = MagicMock()
        spark.sql.return_value.collect.side_effect = [
            [_mock_row({"cnt": 100})],
            [_mock_row({"cnt": 50})],
        ]
        result = _check_silver_key_population(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        assert result.status == CheckStatus.FAIL

    def test_partition_filter_in_both_queries(self) -> None:
        spark = MagicMock()
        spark.sql.return_value.collect.side_effect = [
            [_mock_row({"cnt": 10})],
            [_mock_row({"cnt": 10})],
        ]
        _check_silver_key_population(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        # Both SQL calls should include the partition filter
        for call in spark.sql.call_args_list:
            sql_text = call[0][0]
            assert "country = 'fr'" in sql_text
            assert "ingestion_date = '2025-01-15'" in sql_text


# ---------------------------------------------------------------------------
# _check_salary_consistency (partition-scoped)
# ---------------------------------------------------------------------------


class TestSalaryConsistency:
    """salary_min <= salary_max check is partition-scoped."""

    def test_no_violations_passes(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        result = _check_salary_consistency(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        assert result.status == CheckStatus.PASS

    def test_violations_fail(self) -> None:
        spark = _mock_spark_sql([{"cnt": 5}])
        result = _check_salary_consistency(spark, "sr.sr_silver.adzuna_jobs")
        assert result.status == CheckStatus.FAIL
        assert result.metrics["violations"] == 5

    def test_partition_filter_in_sql(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        _check_salary_consistency(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="gb",
            ingestion_date="2025-06-01",
        )
        sql_text = spark.sql.call_args[0][0]
        assert "country = 'gb'" in sql_text


# ---------------------------------------------------------------------------
# _check_coordinate_sanity (partition-scoped)
# ---------------------------------------------------------------------------


class TestCoordinateSanity:
    """Coordinate-range check is partition-scoped."""

    def test_no_violations_passes(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        result = _check_coordinate_sanity(
            spark,
            "sr.sr_silver.adzuna_jobs",
            country="fr",
            ingestion_date="2025-01-15",
        )
        assert result.status == CheckStatus.PASS

    def test_violations_fail(self) -> None:
        spark = _mock_spark_sql([{"cnt": 2}])
        result = _check_coordinate_sanity(spark, "sr.sr_silver.adzuna_jobs")
        assert result.status == CheckStatus.FAIL

    def test_partition_filter_in_sql(self) -> None:
        spark = _mock_spark_sql([{"cnt": 0}])
        _check_coordinate_sanity(
            spark,
            "sr.sr_silver.adzuna_jobs",
            ingestion_date="2025-06-01",
        )
        sql_text = spark.sql.call_args[0][0]
        assert "ingestion_date = '2025-06-01'" in sql_text


# ---------------------------------------------------------------------------
# Required columns list sanity
# ---------------------------------------------------------------------------


class TestSilverRequiredColumns:
    """SILVER_JOBS_REQUIRED list is well-formed."""

    def test_non_empty(self) -> None:
        assert len(SILVER_JOBS_REQUIRED) > 0

    def test_contains_business_keys(self) -> None:
        assert "job_id" in SILVER_JOBS_REQUIRED
        assert "country" in SILVER_JOBS_REQUIRED
        assert "ingestion_date" in SILVER_JOBS_REQUIRED

    def test_lineage_columns_included(self) -> None:
        for col in _SILVER_LINEAGE_COLS:
            assert col in SILVER_JOBS_REQUIRED, f"Missing lineage col: {col}"
