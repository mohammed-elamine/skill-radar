"""Adzuna-specific validation checks for Bronze and Silver tables.

Reuses generic lakehouse checks from
:mod:`skill_radar.platform.validate.checks.lakehouse` and adds
Adzuna-specific data quality assertions.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.validate.checks.lakehouse import (
    check_namespace_exists,
    check_table_exists,
    check_table_non_empty,
    check_table_schema_contains,
)
from skill_radar.platform.validate.models import CheckResult, NamedCheck, create_check

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bronze required columns
# ---------------------------------------------------------------------------

BRONZE_JOBS_RAW_REQUIRED = [
    "job_id",
    "adref",
    "title",
    "description",
    "created_at_raw",
    "redirect_url",
    "latitude",
    "longitude",
    "salary_min",
    "salary_max",
    "salary_is_predicted_raw",
    "contract_time_raw",
    "contract_type_raw",
    "location_display_name",
    "location_area_json",
    "category_tag",
    "category_label",
    "company_display_name",
    "company_canonical_name",
    "raw_payload_json",
    "source_system",
    "country",
    "preset",
    "search_key",
    "search_params_json",
    "page",
    "results_per_page",
    "api_result_position",
    "extracted_at_utc",
    "ingestion_date",
    "run_id",
]

BRONZE_REQUEST_LOG_REQUIRED = [
    "source_system",
    "country",
    "preset",
    "page",
    "request_params_json",
    "response_count",
    "http_status",
    "request_started_at_utc",
    "request_finished_at_utc",
    "duration_ms",
    "success",
    "error_message",
    "run_id",
    "ingestion_date",
]

# ---------------------------------------------------------------------------
# Silver required columns
# ---------------------------------------------------------------------------

SILVER_JOBS_REQUIRED = [
    "job_id",
    "adref",
    "source_system",
    "country",
    "job_title",
    "job_description",
    "posted_at_utc",
    "posted_date",
    "job_url",
    "company_name",
    "company_canonical_name",
    "category_tag",
    "category_label",
    "location_display_name",
    "location_area",
    "location_country",
    "location_region",
    "location_subregion",
    "latitude",
    "longitude",
    "salary_min",
    "salary_max",
    "salary_mean",
    "salary_is_predicted",
    "contract_time",
    "contract_type",
    "is_full_time",
    "is_part_time",
    "is_permanent",
    "is_contract",
    "title_normalized",
    "description_normalized",
    "company_normalized",
    "location_normalized",
    "bronze_extracted_at_utc",
    "bronze_run_id",
    "search_key",
    "search_params_json",
    "silver_run_id",
    "formatted_at_utc",
    "ingestion_date",
]


# ---------------------------------------------------------------------------
# Bronze checks
# ---------------------------------------------------------------------------


def _check_job_id_population(spark: SparkSession, table_fqn: str) -> CheckResult:
    """Check that the majority of Bronze rows have a non-null job_id."""
    try:
        total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_fqn}").collect()[0]["cnt"]
        with_id = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            "WHERE job_id IS NOT NULL AND TRIM(job_id) != ''"
        ).collect()[0]["cnt"]

        ratio = with_id / total if total > 0 else 0.0
        return create_check(
            name="adzuna.bronze.job_id_population",
            description="Bronze job_id population >= 95%",
            passed=ratio >= 0.95,
            detail=f"with_id={with_id}/{total} ({ratio:.1%})",
            metrics={"total": total, "with_id": with_id, "ratio": round(ratio, 4)},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.bronze.job_id_population",
            description="Bronze job_id population >= 95%",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_page_metadata_sane(spark: SparkSession, table_fqn: str) -> CheckResult:
    """Check that page metadata values are sane (page >= 1, rpp > 0)."""
    try:
        bad = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} WHERE page < 1 OR results_per_page <= 0"
        ).collect()[0]["cnt"]
        return create_check(
            name="adzuna.bronze.page_metadata_sane",
            description="Bronze page metadata sane (page >= 1, rpp > 0)",
            passed=bad == 0,
            detail=f"bad_rows={bad}",
            metrics={"bad_rows": bad},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.bronze.page_metadata_sane",
            description="Bronze page metadata sane",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_raw_payload_parseable(spark: SparkSession, table_fqn: str) -> CheckResult:
    """Sample raw_payload_json and verify it is valid JSON."""
    try:
        rows = spark.sql(f"SELECT raw_payload_json FROM {table_fqn} LIMIT 100").collect()
        bad = 0
        for row in rows:
            try:
                json.loads(row["raw_payload_json"])
            except (json.JSONDecodeError, TypeError):
                bad += 1
        return create_check(
            name="adzuna.bronze.raw_payload_parseable",
            description="Bronze raw_payload_json parseable (sample)",
            passed=bad == 0,
            detail=f"sampled={len(rows)} bad={bad}",
            metrics={"sampled": len(rows), "bad": bad},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.bronze.raw_payload_parseable",
            description="Bronze raw_payload_json parseable",
            passed=False,
            detail=str(exc)[:200],
        )


# Default lineage columns per layer.
_BRONZE_LINEAGE_COLS = ["source_system", "country", "run_id", "ingestion_date"]
_SILVER_LINEAGE_COLS = ["source_system", "country", "silver_run_id", "ingestion_date"]


# ---------------------------------------------------------------------------
# Partition-scoping helper
# ---------------------------------------------------------------------------


def _silver_partition_df(
    spark: SparkSession,
    table_fqn: str,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> DataFrame:
    """Return a Silver DataFrame optionally filtered to a target partition.

    When both *country* and *ingestion_date* are supplied, Iceberg pushes
    the predicate down to the partition spec so only the matching data files
    are scanned.  When either parameter is ``None`` the corresponding
    filter is omitted, allowing the full table to be read when desired.
    """
    df: DataFrame = spark.table(table_fqn)
    if country is not None:
        df = df.filter(df["country"] == country)
    if ingestion_date is not None:
        df = df.filter(df["ingestion_date"] == ingestion_date)
    return df


def _partition_where_clause(
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> str:
    """Build a SQL WHERE fragment for partition columns.

    Returns an empty string when no filters are needed, otherwise returns
    ``" AND country = '…' AND ingestion_date = '…'"`` (with a leading AND
    so it can be appended after an existing WHERE predicate).
    """
    parts: list[str] = []
    if country is not None:
        parts.append(f"country = '{country}'")
    if ingestion_date is not None:
        parts.append(f"ingestion_date = '{ingestion_date}'")
    if not parts:
        return ""
    return " AND " + " AND ".join(parts)


def _check_lineage_columns_present(
    spark: SparkSession,
    table_fqn: str,
    check_name_prefix: str,
    *,
    lineage_cols: list[str] | None = None,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> CheckResult:
    """Check that lineage columns are present and populated.

    When *country* / *ingestion_date* are given, the check verifies that at
    least one row in the target partition carries non-null lineage values.
    """
    lineage_cols = lineage_cols or _BRONZE_LINEAGE_COLS
    try:
        df = _silver_partition_df(spark, table_fqn, country=country, ingestion_date=ingestion_date)
        actual = set(df.columns)
        missing = [c for c in lineage_cols if c not in actual]
        if missing:
            return create_check(
                name=f"{check_name_prefix}.lineage_present",
                description="Lineage columns present",
                passed=False,
                detail=f"Missing: {missing}",
            )
        return create_check(
            name=f"{check_name_prefix}.lineage_present",
            description="Lineage columns present",
            passed=True,
        )
    except Exception as exc:
        return create_check(
            name=f"{check_name_prefix}.lineage_present",
            description="Lineage columns present",
            passed=False,
            detail=str(exc)[:200],
        )


def get_bronze_checks(
    spark: SparkSession,
    config: PlatformSettings,
    *,
    country: str | None = None,  # noqa: ARG001
    ingestion_date: str | None = None,  # noqa: ARG001
) -> list[NamedCheck]:
    """Build the list of Adzuna Bronze validation checks."""
    layout = LakeLayout(config)
    jobs_fqn = layout.adzuna_bronze_jobs_raw_fqn()
    log_fqn = layout.adzuna_bronze_request_log_fqn()
    ns_name = layout.iceberg_namespace_name("bronze")

    checks: list[NamedCheck] = [
        NamedCheck(
            name="adzuna.bronze.namespace_exists",
            description=f"Bronze namespace {ns_name} exists",
            fn=lambda: check_namespace_exists(spark, ns_name),
        ),
        NamedCheck(
            name="adzuna.bronze.jobs_raw.table_exists",
            description=f"Table {jobs_fqn} exists",
            fn=lambda: check_table_exists(spark, jobs_fqn),
        ),
        NamedCheck(
            name="adzuna.bronze.jobs_raw.non_empty",
            description=f"Table {jobs_fqn} is non-empty",
            fn=lambda: check_table_non_empty(spark, jobs_fqn),
        ),
        NamedCheck(
            name="adzuna.bronze.jobs_raw.schema",
            description="Bronze jobs_raw has required columns",
            fn=lambda: check_table_schema_contains(spark, jobs_fqn, BRONZE_JOBS_RAW_REQUIRED),
        ),
        NamedCheck(
            name="adzuna.bronze.jobs_raw.job_id_population",
            description="job_id population >= 95%",
            fn=lambda: _check_job_id_population(spark, jobs_fqn),
        ),
        NamedCheck(
            name="adzuna.bronze.jobs_raw.raw_payload_parseable",
            description="raw_payload_json parseable",
            fn=lambda: _check_raw_payload_parseable(spark, jobs_fqn),
        ),
        NamedCheck(
            name="adzuna.bronze.jobs_raw.lineage",
            description="Lineage columns present",
            fn=lambda: _check_lineage_columns_present(spark, jobs_fqn, "adzuna.bronze.jobs_raw"),
        ),
        NamedCheck(
            name="adzuna.bronze.jobs_raw.page_metadata",
            description="Page metadata sane",
            fn=lambda: _check_page_metadata_sane(spark, jobs_fqn),
        ),
        # Request log checks.
        NamedCheck(
            name="adzuna.bronze.request_log.table_exists",
            description=f"Table {log_fqn} exists",
            fn=lambda: check_table_exists(spark, log_fqn),
        ),
        NamedCheck(
            name="adzuna.bronze.request_log.non_empty",
            description=f"Table {log_fqn} is non-empty",
            fn=lambda: check_table_non_empty(spark, log_fqn),
        ),
        NamedCheck(
            name="adzuna.bronze.request_log.schema",
            description="Request log has required columns",
            fn=lambda: check_table_schema_contains(spark, log_fqn, BRONZE_REQUEST_LOG_REQUIRED),
        ),
    ]

    return checks


# ---------------------------------------------------------------------------
# Silver checks
# ---------------------------------------------------------------------------


def _check_silver_key_population(
    spark: SparkSession,
    table_fqn: str,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> CheckResult:
    """Check that key Silver fields are populated (partition-scoped)."""
    try:
        extra = _partition_where_clause(country=country, ingestion_date=ingestion_date)
        total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_fqn} WHERE 1=1{extra}").collect()[0][
            "cnt"
        ]
        with_keys = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            "WHERE job_id IS NOT NULL AND TRIM(job_id) != '' "
            "AND job_title IS NOT NULL AND TRIM(job_title) != '' "
            f"AND country IS NOT NULL AND TRIM(country) != ''{extra}"
        ).collect()[0]["cnt"]
        ratio = with_keys / total if total > 0 else 0.0
        return create_check(
            name="adzuna.silver.key_population",
            description="Silver key fields populated >= 95%",
            passed=ratio >= 0.95,
            detail=f"with_keys={with_keys}/{total} ({ratio:.1%})",
            metrics={"total": total, "with_keys": with_keys},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.silver.key_population",
            description="Silver key fields populated",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_salary_consistency(
    spark: SparkSession,
    table_fqn: str,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> CheckResult:
    """Check that salary_min <= salary_max when both are non-null (partition-scoped)."""
    try:
        extra = _partition_where_clause(country=country, ingestion_date=ingestion_date)
        bad = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            "WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL "
            f"AND salary_min > salary_max{extra}"
        ).collect()[0]["cnt"]
        return create_check(
            name="adzuna.silver.salary_consistency",
            description="salary_min <= salary_max",
            passed=bad == 0,
            detail=f"violations={bad}",
            metrics={"violations": bad},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.silver.salary_consistency",
            description="salary_min <= salary_max",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_coordinate_sanity(
    spark: SparkSession,
    table_fqn: str,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> CheckResult:
    """Check latitude in [-90, 90] and longitude in [-180, 180] (partition-scoped)."""
    try:
        extra = _partition_where_clause(country=country, ingestion_date=ingestion_date)
        bad = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            "WHERE (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90)) "
            f"OR (longitude IS NOT NULL AND (longitude < -180 OR longitude > 180)){extra}"
        ).collect()[0]["cnt"]
        return create_check(
            name="adzuna.silver.coordinate_sanity",
            description="Coordinates within valid ranges",
            passed=bad == 0,
            detail=f"violations={bad}",
            metrics={"violations": bad},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.silver.coordinate_sanity",
            description="Coordinates within valid ranges",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_silver_duplicates(
    spark: SparkSession,
    table_fqn: str,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> CheckResult:
    """Check for duplicate (country, ingestion_date, job_id) tuples (partition-scoped).

    The Silver table is a daily snapshot partitioned by
    ``(ingestion_date, country)``.  The same ``job_id`` may legitimately
    appear on different ingestion dates, so the correct uniqueness grain is
    ``(country, ingestion_date, job_id)``.
    """
    try:
        extra = _partition_where_clause(country=country, ingestion_date=ingestion_date)
        dupes = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM ("
            f"  SELECT country, ingestion_date, job_id, COUNT(*) AS n "
            f"  FROM {table_fqn} "
            f"  WHERE job_id IS NOT NULL AND TRIM(job_id) != ''{extra} "
            f"  GROUP BY country, ingestion_date, job_id HAVING n > 1"
            f")"
        ).collect()[0]["cnt"]
        return create_check(
            name="adzuna.silver.no_duplicates",
            description="No duplicate (country, ingestion_date, job_id) in Silver",
            passed=dupes == 0,
            detail=f"duplicate_keys={dupes}",
            metrics={"duplicate_keys": dupes},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.silver.no_duplicates",
            description="No duplicate (country, ingestion_date, job_id)",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_silver_partition_non_empty(
    spark: SparkSession,
    table_fqn: str,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> CheckResult:
    """Check that the target Silver partition contains at least one row."""
    try:
        extra = _partition_where_clause(country=country, ingestion_date=ingestion_date)
        row_count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} WHERE 1=1{extra}"
        ).collect()[0]["cnt"]
        scope = "/".join(p for p in [country, ingestion_date] if p is not None) or "full-table"
        return create_check(
            name="adzuna.silver.partition_non_empty",
            description=f"Silver partition ({scope}) is non-empty",
            passed=row_count >= 1,
            detail=f"row_count={row_count}",
            metrics={"row_count": row_count},
        )
    except Exception as exc:
        return create_check(
            name="adzuna.silver.partition_non_empty",
            description="Silver partition is non-empty",
            passed=False,
            detail=str(exc)[:200],
        )


def get_silver_checks(
    spark: SparkSession,
    config: PlatformSettings,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
) -> list[NamedCheck]:
    """Build the list of Adzuna Silver validation checks.

    When *country* and/or *ingestion_date* are provided the data-quality
    checks (duplicates, key population, salary consistency, coordinate
    sanity, lineage, non-empty) are scoped to the matching partition.
    Structural checks (namespace, table existence, schema) always operate
    at the table level.
    """
    layout = LakeLayout(config)
    silver_fqn = layout.adzuna_silver_jobs_fqn()
    ns_name = layout.iceberg_namespace_name("silver")

    # Shorthand for partition kwargs passed to every DQ check.
    _pk = {"country": country, "ingestion_date": ingestion_date}

    checks: list[NamedCheck] = [
        # -- Structural (table-level) checks --------------------------------
        NamedCheck(
            name="adzuna.silver.namespace_exists",
            description=f"Silver namespace {ns_name} exists",
            fn=lambda: check_namespace_exists(spark, ns_name),
        ),
        NamedCheck(
            name="adzuna.silver.table_exists",
            description=f"Table {silver_fqn} exists",
            fn=lambda: check_table_exists(spark, silver_fqn),
        ),
        NamedCheck(
            name="adzuna.silver.non_empty",
            description=f"Table {silver_fqn} is non-empty",
            fn=lambda: check_table_non_empty(spark, silver_fqn),
        ),
        NamedCheck(
            name="adzuna.silver.schema",
            description="Silver adzuna_jobs has required columns",
            fn=lambda: check_table_schema_contains(spark, silver_fqn, SILVER_JOBS_REQUIRED),
        ),
        # -- Partition-scoped DQ checks -------------------------------------
        NamedCheck(
            name="adzuna.silver.partition_non_empty",
            description="Silver target partition is non-empty",
            fn=lambda: _check_silver_partition_non_empty(spark, silver_fqn, **_pk),
        ),
        NamedCheck(
            name="adzuna.silver.key_population",
            description="Key fields populated (job_id, job_title, country)",
            fn=lambda: _check_silver_key_population(spark, silver_fqn, **_pk),
        ),
        NamedCheck(
            name="adzuna.silver.salary_consistency",
            description="salary_min <= salary_max",
            fn=lambda: _check_salary_consistency(spark, silver_fqn, **_pk),
        ),
        NamedCheck(
            name="adzuna.silver.coordinate_sanity",
            description="Coordinate values in valid ranges",
            fn=lambda: _check_coordinate_sanity(spark, silver_fqn, **_pk),
        ),
        NamedCheck(
            name="adzuna.silver.no_duplicates",
            description="No duplicate business keys",
            fn=lambda: _check_silver_duplicates(spark, silver_fqn, **_pk),
        ),
        NamedCheck(
            name="adzuna.silver.lineage",
            description="Lineage columns present",
            fn=lambda: _check_lineage_columns_present(
                spark,
                silver_fqn,
                "adzuna.silver",
                lineage_cols=_SILVER_LINEAGE_COLS,
                **_pk,
            ),
        ),
    ]

    return checks
