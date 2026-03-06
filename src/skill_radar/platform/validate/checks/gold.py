"""Gold-layer validation checks for all 5 Gold Iceberg tables.

Reuses generic lakehouse checks from
:mod:`skill_radar.platform.validate.checks.lakehouse` and adds
Gold-specific data quality assertions (score ranges, uniqueness,
salary coherence, referential integrity).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from skill_radar.domains.gold import schema as gold_schema
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.validate.checks.lakehouse import (
    check_namespace_exists,
    check_table_exists,
    check_table_non_empty,
    check_table_schema_contains,
)
from skill_radar.platform.validate.models import CheckResult, NamedCheck, create_check

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Re-export required column lists from gold.schema (single source of truth)
# ---------------------------------------------------------------------------

SKILL_MATCHES_REQUIRED = gold_schema.SKILL_MATCHES_REQUIRED
OCCUPATION_MATCHES_REQUIRED = gold_schema.OCCUPATION_MATCHES_REQUIRED
SKILL_DEMAND_DAILY_REQUIRED = gold_schema.SKILL_DEMAND_DAILY_REQUIRED
SALARY_BY_SKILL_DAILY_REQUIRED = gold_schema.SALARY_BY_SKILL_DAILY_REQUIRED
OCCUPATION_SKILL_GRAPH_REQUIRED = gold_schema.OCCUPATION_SKILL_GRAPH_REQUIRED

# Lineage columns common to all Gold tables
_GOLD_LINEAGE_COLS = gold_schema.GOLD_LINEAGE_COLS


# ---------------------------------------------------------------------------
# Gold-specific check helpers
# ---------------------------------------------------------------------------


def _check_partition_non_empty(
    spark: SparkSession,
    table_fqn: str,
    *,
    ingestion_date: str,
    country: str,
) -> CheckResult:
    """Check that a specific (ingestion_date, country) partition has rows."""
    short = table_fqn.split(".")[-1]
    try:
        cnt = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            f"WHERE ingestion_date = '{ingestion_date}' AND country = '{country}'"
        ).collect()[0]["cnt"]
        return create_check(
            name=f"gold.{short}.partition_non_empty",
            description=f"{short} partition ({ingestion_date}, {country}) non-empty",
            passed=cnt > 0,
            detail=f"rows={cnt}",
            metrics={"row_count": cnt},
        )
    except Exception as exc:
        return create_check(
            name=f"gold.{short}.partition_non_empty",
            description=f"{short} partition non-empty",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_score_range(
    spark: SparkSession,
    table_fqn: str,
    score_col: str = "match_score",
) -> CheckResult:
    """Check that score values are in [0, 1]."""
    short = table_fqn.split(".")[-1]
    try:
        bad = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            f"WHERE {score_col} IS NOT NULL AND ({score_col} < 0.0 OR {score_col} > 1.0)"
        ).collect()[0]["cnt"]
        return create_check(
            name=f"gold.{short}.score_range",
            description=f"{short} {score_col} in [0, 1]",
            passed=bad == 0,
            detail=f"violations={bad}",
            metrics={"violations": bad},
        )
    except Exception as exc:
        return create_check(
            name=f"gold.{short}.score_range",
            description=f"{short} {score_col} in [0, 1]",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_no_duplicate_keys(
    spark: SparkSession,
    table_fqn: str,
    key_cols: list[str],
    check_name: str,
) -> CheckResult:
    """Check that (key_cols) has no duplicate rows."""
    short = table_fqn.split(".")[-1]
    key_expr = ", ".join(key_cols)
    try:
        dupes = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM ("
            f"  SELECT {key_expr}, COUNT(*) AS n "
            f"  FROM {table_fqn} "
            f"  GROUP BY {key_expr} HAVING n > 1"
            f")"
        ).collect()[0]["cnt"]
        return create_check(
            name=f"gold.{short}.{check_name}",
            description=f"No duplicate ({key_expr}) in {short}",
            passed=dupes == 0,
            detail=f"duplicate_keys={dupes}",
            metrics={"duplicate_keys": dupes},
        )
    except Exception as exc:
        return create_check(
            name=f"gold.{short}.{check_name}",
            description=f"No duplicate keys in {short}",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_lineage_present(
    spark: SparkSession,
    table_fqn: str,
) -> CheckResult:
    """Check that Gold lineage columns are present."""
    short = table_fqn.split(".")[-1]
    try:
        df = spark.table(table_fqn)
        actual = set(df.columns)
        missing = [c for c in _GOLD_LINEAGE_COLS if c not in actual]
        if missing:
            return create_check(
                name=f"gold.{short}.lineage_present",
                description="Gold lineage columns present",
                passed=False,
                detail=f"Missing: {missing}",
            )
        return create_check(
            name=f"gold.{short}.lineage_present",
            description="Gold lineage columns present",
            passed=True,
        )
    except Exception as exc:
        return create_check(
            name=f"gold.{short}.lineage_present",
            description="Gold lineage columns present",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_salary_coherence(
    spark: SparkSession,
    table_fqn: str,
) -> CheckResult:
    """Check min_salary_min <= avg_salary_mean <= max_salary_max."""
    _min = "min_salary_min"
    _avg = "avg_salary_mean"
    _max = "max_salary_max"
    short = table_fqn.split(".")[-1]
    try:
        bad = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            f"WHERE {_min} IS NOT NULL "
            f"AND {_max} IS NOT NULL "
            f"AND {_avg} IS NOT NULL "
            f"AND ({_min} > {_avg} OR {_avg} > {_max})"
        ).collect()[0]["cnt"]
        return create_check(
            name=f"gold.{short}.salary_coherence",
            description="Salary stats coherent (min <= avg <= max)",
            passed=bad == 0,
            detail=f"violations={bad}",
            metrics={"violations": bad},
        )
    except Exception as exc:
        return create_check(
            name=f"gold.{short}.salary_coherence",
            description="Salary stats coherent",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_positive_counts(
    spark: SparkSession,
    table_fqn: str,
    count_cols: list[str],
) -> CheckResult:
    """Check that count columns are non-negative."""
    short = table_fqn.split(".")[-1]
    conditions = " OR ".join(f"({c} IS NOT NULL AND {c} < 0)" for c in count_cols)
    try:
        bad = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_fqn} WHERE {conditions}").collect()[0][
            "cnt"
        ]
        return create_check(
            name=f"gold.{short}.positive_counts",
            description=f"Count columns non-negative in {short}",
            passed=bad == 0,
            detail=f"cols={count_cols} violations={bad}",
            metrics={"violations": bad},
        )
    except Exception as exc:
        return create_check(
            name=f"gold.{short}.positive_counts",
            description=f"Count columns non-negative in {short}",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_co_occurrence_positive(
    spark: SparkSession,
    table_fqn: str,
) -> CheckResult:
    """Check that matched_jobs_count >= 1 (graph edges are meaningful)."""
    col = gold_schema.matched("jobs_count")
    short = table_fqn.split(".")[-1]
    try:
        bad = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} WHERE {col} IS NULL OR {col} < 1"
        ).collect()[0]["cnt"]
        return create_check(
            name=f"gold.{short}.co_occurrence_positive",
            description=f"{col} >= 1",
            passed=bad == 0,
            detail=f"violations={bad}",
            metrics={"violations": bad},
        )
    except Exception as exc:
        return create_check(
            name=f"gold.{short}.co_occurrence_positive",
            description=f"{col} >= 1",
            passed=False,
            detail=str(exc)[:200],
        )


# ---------------------------------------------------------------------------
# Public check factory
# ---------------------------------------------------------------------------


def get_gold_checks(
    spark: SparkSession,
    config: PlatformSettings,
    *,
    ingestion_date: str,
    country: str,
    esco_version: str,  # noqa: ARG001
    esco_lang: str,  # noqa: ARG001
) -> list[NamedCheck]:
    """Build the list of Gold validation checks for all 5 tables.

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Platform settings.
    ingestion_date:
        Target partition date.
    country:
        Target partition country.
    esco_version:
        ESCO version used for matching.
    esco_lang:
        ESCO language used for matching.
    """
    layout = LakeLayout(config)
    ns = layout.iceberg_namespace_name("gold")

    skill_fqn = layout.gold_job_skill_matches_fqn()
    occ_fqn = layout.gold_job_occupation_matches_fqn()
    demand_fqn = layout.gold_skill_demand_daily_fqn()
    salary_fqn = layout.gold_salary_by_skill_daily_fqn()
    graph_fqn = layout.gold_occupation_skill_graph_fqn()

    checks: list[NamedCheck] = []

    # ── Namespace ──
    checks.append(
        NamedCheck(
            name="gold.namespace_exists",
            description=f"Gold namespace {ns} exists",
            fn=lambda: check_namespace_exists(spark, ns),
        )
    )

    # ── gold_job_skill_matches ──
    checks.extend(
        [
            NamedCheck(
                name="gold.skill_matches.table_exists",
                description=f"Table {skill_fqn} exists",
                fn=lambda: check_table_exists(spark, skill_fqn),
            ),
            NamedCheck(
                name="gold.skill_matches.non_empty",
                description=f"Table {skill_fqn} is non-empty",
                fn=lambda: check_table_non_empty(spark, skill_fqn),
            ),
            NamedCheck(
                name="gold.skill_matches.schema",
                description="gold_job_skill_matches has required columns",
                fn=lambda: check_table_schema_contains(spark, skill_fqn, SKILL_MATCHES_REQUIRED),
            ),
            NamedCheck(
                name="gold.skill_matches.partition_non_empty",
                description=f"skill_matches partition ({ingestion_date}, {country}) non-empty",
                fn=lambda: _check_partition_non_empty(
                    spark,
                    skill_fqn,
                    ingestion_date=ingestion_date,
                    country=country,
                ),
            ),
            NamedCheck(
                name="gold.skill_matches.score_range",
                description="match_score in [0, 1]",
                fn=lambda: _check_score_range(spark, skill_fqn),
            ),
            NamedCheck(
                name="gold.skill_matches.no_duplicates",
                description=f"No duplicate ({', '.join(gold_schema.SKILL_MATCHES_KEY)})",
                fn=lambda: _check_no_duplicate_keys(
                    spark,
                    skill_fqn,
                    gold_schema.SKILL_MATCHES_KEY,
                    "no_duplicates",
                ),
            ),
            NamedCheck(
                name="gold.skill_matches.lineage",
                description="Gold lineage columns present",
                fn=lambda: _check_lineage_present(spark, skill_fqn),
            ),
        ]
    )

    # ── gold_job_occupation_matches ──
    checks.extend(
        [
            NamedCheck(
                name="gold.occ_matches.table_exists",
                description=f"Table {occ_fqn} exists",
                fn=lambda: check_table_exists(spark, occ_fqn),
            ),
            NamedCheck(
                name="gold.occ_matches.non_empty",
                description=f"Table {occ_fqn} is non-empty",
                fn=lambda: check_table_non_empty(spark, occ_fqn),
            ),
            NamedCheck(
                name="gold.occ_matches.schema",
                description="gold_job_occupation_matches has required columns",
                fn=lambda: check_table_schema_contains(spark, occ_fqn, OCCUPATION_MATCHES_REQUIRED),
            ),
            NamedCheck(
                name="gold.occ_matches.partition_non_empty",
                description=f"occ_matches partition ({ingestion_date}, {country}) non-empty",
                fn=lambda: _check_partition_non_empty(
                    spark,
                    occ_fqn,
                    ingestion_date=ingestion_date,
                    country=country,
                ),
            ),
            NamedCheck(
                name="gold.occ_matches.score_range",
                description="match_score in [0, 1]",
                fn=lambda: _check_score_range(spark, occ_fqn),
            ),
            NamedCheck(
                name="gold.occ_matches.no_duplicates",
                description=f"No duplicate ({', '.join(gold_schema.OCCUPATION_MATCHES_KEY)})",
                fn=lambda: _check_no_duplicate_keys(
                    spark,
                    occ_fqn,
                    gold_schema.OCCUPATION_MATCHES_KEY,
                    "no_duplicates",
                ),
            ),
            NamedCheck(
                name="gold.occ_matches.lineage",
                description="Gold lineage columns present",
                fn=lambda: _check_lineage_present(spark, occ_fqn),
            ),
        ]
    )

    # ── gold_skill_demand_daily ──
    checks.extend(
        [
            NamedCheck(
                name="gold.demand.table_exists",
                description=f"Table {demand_fqn} exists",
                fn=lambda: check_table_exists(spark, demand_fqn),
            ),
            NamedCheck(
                name="gold.demand.non_empty",
                description=f"Table {demand_fqn} is non-empty",
                fn=lambda: check_table_non_empty(spark, demand_fqn),
            ),
            NamedCheck(
                name="gold.demand.schema",
                description="gold_skill_demand_daily has required columns",
                fn=lambda: check_table_schema_contains(
                    spark, demand_fqn, SKILL_DEMAND_DAILY_REQUIRED
                ),
            ),
            NamedCheck(
                name="gold.demand.partition_non_empty",
                description=f"demand partition ({ingestion_date}, {country}) non-empty",
                fn=lambda: _check_partition_non_empty(
                    spark,
                    demand_fqn,
                    ingestion_date=ingestion_date,
                    country=country,
                ),
            ),
            NamedCheck(
                name="gold.demand.positive_counts",
                description="Count columns non-negative",
                fn=lambda: _check_positive_counts(
                    spark,
                    demand_fqn,
                    gold_schema.DEMAND_COUNT_COLS,
                ),
            ),
            NamedCheck(
                name="gold.demand.no_duplicates",
                description=f"No duplicate ({', '.join(gold_schema.SKILL_DEMAND_KEY)})",
                fn=lambda: _check_no_duplicate_keys(
                    spark,
                    demand_fqn,
                    gold_schema.SKILL_DEMAND_KEY,
                    "no_duplicates",
                ),
            ),
            NamedCheck(
                name="gold.demand.lineage",
                description="Gold lineage columns present",
                fn=lambda: _check_lineage_present(spark, demand_fqn),
            ),
        ]
    )

    # ── gold_salary_by_skill_daily ──
    checks.extend(
        [
            NamedCheck(
                name="gold.salary.table_exists",
                description=f"Table {salary_fqn} exists",
                fn=lambda: check_table_exists(spark, salary_fqn),
            ),
            NamedCheck(
                name="gold.salary.non_empty",
                description=f"Table {salary_fqn} is non-empty",
                fn=lambda: check_table_non_empty(spark, salary_fqn),
            ),
            NamedCheck(
                name="gold.salary.schema",
                description="gold_salary_by_skill_daily has required columns",
                fn=lambda: check_table_schema_contains(
                    spark, salary_fqn, SALARY_BY_SKILL_DAILY_REQUIRED
                ),
            ),
            NamedCheck(
                name="gold.salary.partition_non_empty",
                description=f"salary partition ({ingestion_date}, {country}) non-empty",
                fn=lambda: _check_partition_non_empty(
                    spark,
                    salary_fqn,
                    ingestion_date=ingestion_date,
                    country=country,
                ),
            ),
            NamedCheck(
                name="gold.salary.salary_coherence",
                description="Salary stats coherent (min <= avg <= max)",
                fn=lambda: _check_salary_coherence(spark, salary_fqn),
            ),
            NamedCheck(
                name="gold.salary.positive_counts",
                description="salary_jobs_count non-negative",
                fn=lambda: _check_positive_counts(spark, salary_fqn, gold_schema.SALARY_COUNT_COLS),
            ),
            NamedCheck(
                name="gold.salary.no_duplicates",
                description=f"No duplicate ({', '.join(gold_schema.SALARY_BY_SKILL_KEY)})",
                fn=lambda: _check_no_duplicate_keys(
                    spark,
                    salary_fqn,
                    gold_schema.SALARY_BY_SKILL_KEY,
                    "no_duplicates",
                ),
            ),
            NamedCheck(
                name="gold.salary.lineage",
                description="Gold lineage columns present",
                fn=lambda: _check_lineage_present(spark, salary_fqn),
            ),
        ]
    )

    # ── gold_occupation_skill_graph ──
    checks.extend(
        [
            NamedCheck(
                name="gold.graph.table_exists",
                description=f"Table {graph_fqn} exists",
                fn=lambda: check_table_exists(spark, graph_fqn),
            ),
            NamedCheck(
                name="gold.graph.non_empty",
                description=f"Table {graph_fqn} is non-empty",
                fn=lambda: check_table_non_empty(spark, graph_fqn),
            ),
            NamedCheck(
                name="gold.graph.schema",
                description="gold_occupation_skill_graph has required columns",
                fn=lambda: check_table_schema_contains(
                    spark, graph_fqn, OCCUPATION_SKILL_GRAPH_REQUIRED
                ),
            ),
            NamedCheck(
                name="gold.graph.partition_non_empty",
                description=f"graph partition ({ingestion_date}, {country}) non-empty",
                fn=lambda: _check_partition_non_empty(
                    spark,
                    graph_fqn,
                    ingestion_date=ingestion_date,
                    country=country,
                ),
            ),
            NamedCheck(
                name="gold.graph.co_occurrence_positive",
                description=f"{gold_schema.matched('jobs_count')} >= 1",
                fn=lambda: _check_co_occurrence_positive(spark, graph_fqn),
            ),
            NamedCheck(
                name="gold.graph.no_duplicates",
                description=f"No duplicate ({', '.join(gold_schema.OCCUPATION_SKILL_GRAPH_KEY)})",
                fn=lambda: _check_no_duplicate_keys(
                    spark,
                    graph_fqn,
                    gold_schema.OCCUPATION_SKILL_GRAPH_KEY,
                    "no_duplicates",
                ),
            ),
            NamedCheck(
                name="gold.graph.lineage",
                description="Gold lineage columns present",
                fn=lambda: _check_lineage_present(spark, graph_fqn),
            ),
        ]
    )

    return checks
