"""Gold analytics orchestrator — sequences KPI and graph computations.

Reads Gold matching outputs and Adzuna Silver jobs to compute:
1. Daily skill demand KPIs.
2. Daily salary-by-skill KPIs.
3. Occupation-skill graph with market evidence.

Writes results to Gold Iceberg tables with partition overwrite.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.logging.context import get_context

from .. import schema as gold_schema
from ..matching.models import GoldAnalyticsResult
from .occupation_kpis import compute_occupation_skill_graph
from .salary_kpis import compute_salary_by_skill_daily
from .skill_kpis import compute_skill_demand_daily

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ── Iceberg write helper ─────────────────────────────────────────────────


def _write_gold_table(
    df: DataFrame,
    table_fqn: str,
    spark: SparkSession,
    partition_cols: tuple[str, ...] = ("ingestion_date", "country"),
) -> None:
    """Write a DataFrame to a Gold Iceberg table with partition overwrite."""
    if not spark.catalog.tableExists(table_fqn):
        logger.info("Creating Gold Iceberg table: %s", table_fqn)
        (
            df.writeTo(table_fqn)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .partitionedBy(*partition_cols)  # type: ignore[arg-type]
            .create()
        )
    else:
        logger.info("Overwriting partitions in Gold table: %s", table_fqn)
        df.writeTo(table_fqn).overwritePartitions()


# ── Main orchestrator ─────────────────────────────────────────────────────


def run_gold_analytics(
    spark: SparkSession,
    *,
    ingestion_date: str,
    country: str,
    esco_version: str,
    esco_lang: str,
    run_id: str | None = None,
    config: PlatformSettings | None = None,
) -> GoldAnalyticsResult:
    """Execute the Gold analytics pipeline.

    Prerequisites: Gold matching tables must already be populated for the
    given partition.

    Parameters
    ----------
    spark:
        Active SparkSession.
    ingestion_date:
        Target date partition (YYYY-MM-DD).
    country:
        Target country partition.
    esco_version:
        ESCO version for dimension filtering.
    esco_lang:
        ESCO language for dimension filtering.
    run_id:
        Run identifier for lineage.
    config:
        Platform configuration.

    Returns
    -------
    GoldAnalyticsResult
        Structured result with row counts and lineage.
    """
    cfg = config or load_platform_config()
    layout = LakeLayout(cfg)

    now_utc = datetime.now(UTC)
    generated_at = now_utc.isoformat()

    try:
        ctx = get_context()
        resolved_run_id = run_id or ctx.run_id
    except RuntimeError:
        import uuid as _uuid

        resolved_run_id = run_id or _uuid.uuid4().hex[:12]

    result = GoldAnalyticsResult(
        run_id=resolved_run_id,
        ingestion_date=ingestion_date,
        country=country,
        esco_version=esco_version,
        esco_lang=esco_lang,
    )

    logger.info(
        "Starting Gold analytics: country=%s date=%s esco=%s/%s run_id=%s",
        country,
        ingestion_date,
        esco_version,
        esco_lang,
        resolved_run_id,
    )

    try:
        # Ensure Gold namespace
        gold_ns = layout.iceberg_namespace("gold")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_ns}")

        # ── Read inputs ───────────────────────────────────────────────────

        skill_matches_fqn = layout.gold_job_skill_matches_fqn()
        occ_matches_fqn = layout.gold_job_occupation_matches_fqn()
        adzuna_fqn = layout.adzuna_silver_jobs_fqn()
        skills_fqn = layout.esco_silver_skills_fqn()
        occupations_fqn = layout.esco_silver_occupations_fqn()
        relations_fqn = layout.esco_silver_relations_fqn()

        logger.info("Reading Gold skill matches from %s", skill_matches_fqn)
        skill_matches = spark.read.table(skill_matches_fqn).where(
            (F.col("country") == country) & (F.col("ingestion_date") == ingestion_date)
        )

        logger.info("Reading Adzuna Silver jobs from %s", adzuna_fqn)
        jobs_df = spark.read.table(adzuna_fqn).where(
            (F.col("country") == country) & (F.col("ingestion_date") == ingestion_date)
        )

        logger.info("Reading Gold occupation matches from %s", occ_matches_fqn)
        occ_matches = spark.read.table(occ_matches_fqn).where(
            (F.col("country") == country) & (F.col("ingestion_date") == ingestion_date)
        )

        logger.info("Reading ESCO Silver dimensions")
        skills_df = spark.read.table(skills_fqn).where(
            (F.col("version") == esco_version) & (F.col("lang") == esco_lang)
        )
        occupations_df = spark.read.table(occupations_fqn).where(
            (F.col("version") == esco_version) & (F.col("lang") == esco_lang)
        )
        relations_df = spark.read.table(relations_fqn).where(
            (F.col("version") == esco_version) & (F.col("lang") == esco_lang)
        )

        # ── 1. Skill demand daily ────────────────────────────────────────

        logger.info("Computing skill demand daily KPIs")
        skill_demand = compute_skill_demand_daily(skill_matches, jobs_df)
        skill_demand_count = skill_demand.count()
        result.skill_demand_rows = skill_demand_count
        logger.info("Skill demand daily: %d rows", skill_demand_count)

        skill_demand_fqn = layout.gold_skill_demand_daily_fqn()
        _write_gold_table(skill_demand, skill_demand_fqn, spark)

        # ── 2. Salary by skill daily ─────────────────────────────────────

        logger.info("Computing salary-by-skill daily KPIs")
        salary_by_skill = compute_salary_by_skill_daily(skill_matches, jobs_df)
        salary_count = salary_by_skill.count()
        result.salary_by_skill_rows = salary_count
        logger.info("Salary by skill daily: %d rows", salary_count)

        salary_fqn = layout.gold_salary_by_skill_daily_fqn()
        _write_gold_table(salary_by_skill, salary_fqn, spark)

        # ── 3. Occupation-skill graph ─────────────────────────────────────

        logger.info("Computing occupation-skill graph")
        occ_skill_graph = compute_occupation_skill_graph(
            skill_matches, occ_matches, relations_df, occupations_df, skills_df
        )

        # Add lineage columns
        occ_skill_graph = (
            occ_skill_graph.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
            .withColumn(
                gold_schema.gold_meta("generated_at_utc"), F.lit(generated_at).cast("timestamp")
            )
            .withColumn("esco_version", F.lit(esco_version))
            .withColumn("esco_lang", F.lit(esco_lang))
        )

        graph_count = occ_skill_graph.count()
        result.occupation_skill_graph_rows = graph_count
        logger.info("Occupation-skill graph: %d rows", graph_count)

        graph_fqn = layout.gold_occupation_skill_graph_fqn()
        _write_gold_table(occ_skill_graph, graph_fqn, spark)

        logger.info(
            "Gold analytics complete: demand=%d salary=%d graph=%d",
            skill_demand_count,
            salary_count,
            graph_count,
        )

    except Exception as exc:
        result.success = False
        result.error = str(exc)
        logger.exception("Gold analytics failed")

    return result
