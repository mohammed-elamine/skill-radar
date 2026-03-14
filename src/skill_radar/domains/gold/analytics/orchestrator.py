"""Gold analytics orchestrator — sequences KPI and graph computations.

Reads Gold matching outputs and Adzuna Silver jobs to compute:
1. Daily skill demand KPIs
2. Daily salary-by-skill KPIs
3. Occupation-skill graph with market evidence
4. Emerging-skill signals (enrichment, soft-fail)
5. Occupation market daily analytics (enrichment, soft-fail)
6. Skill demand segments via KMeans (enrichment, soft-fail)
"""

from __future__ import annotations

import contextlib
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.logging.context import get_context

from .. import schema as gold_schema
from ..matching.models import GoldAnalyticsResult
from .emerging_skills import compute_skill_emerging_daily
from .occupation_kpis import compute_occupation_skill_graph
from .occupation_market import compute_occupation_market_daily
from .occupation_profiles import compute_occupation_profile_daily
from .occupation_similarity import compute_occupation_similarity_daily
from .occupation_transitions import compute_occupation_transition_daily
from .salary_kpis import compute_salary_by_skill_daily
from .skill_kpis import compute_skill_demand_daily
from .skill_profiles import compute_skill_profile_daily
from .skill_segments import compute_skill_demand_segments

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)

_PERSIST_LEVEL = StorageLevel.MEMORY_AND_DISK


# ── Helpers ───────────────────────────────────────────────────────────────


def _phase_start(label: str) -> float:
    logger.info("▶ %s", label)
    return time.monotonic()


def _phase_done(label: str, start: float) -> None:
    elapsed = time.monotonic() - start
    logger.info("✓ %s  (%.1fs)", label, elapsed)


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

    _persisted: list[DataFrame] = []

    def _p(df: DataFrame) -> DataFrame:
        df.persist(_PERSIST_LEVEL)
        _persisted.append(df)
        return df

    try:
        # Ensure Gold namespace
        gold_ns = layout.iceberg_namespace("gold")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_ns}")

        # ══════════════════════════════════════════════════════════════════
        # Phase 1 — Read inputs
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Read analytics inputs")

        partition_filter = (F.col("country") == country) & (
            F.col("ingestion_date") == ingestion_date
        )
        esco_filter = (F.col("version") == esco_version) & (F.col("lang") == esco_lang)

        # Reused across demand + salary KPIs → persist
        skill_matches = _p(
            spark.read.table(layout.gold_job_skill_matches_fqn()).where(partition_filter)
        )
        jobs_df = _p(spark.read.table(layout.adzuna_silver_jobs_fqn()).where(partition_filter))

        occ_matches = spark.read.table(layout.gold_job_occupation_matches_fqn()).where(
            partition_filter
        )

        skills_df = spark.read.table(layout.esco_silver_skills_fqn()).where(esco_filter)
        occupations_df = spark.read.table(layout.esco_silver_occupations_fqn()).where(esco_filter)
        relations_df = spark.read.table(layout.esco_silver_relations_fqn()).where(esco_filter)

        _phase_done("Read analytics inputs", t0)

        # ══════════════════════════════════════════════════════════════════
        # Phase 2 — Skill demand daily
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Compute skill demand daily")

        skill_demand = compute_skill_demand_daily(skill_matches, jobs_df)

        skill_demand_fqn = layout.gold_skill_demand_daily_fqn()
        _write_gold_table(skill_demand, skill_demand_fqn, spark)

        # Count after write (Iceberg scan, lightweight)
        result.skill_demand_rows = (
            spark.read.table(skill_demand_fqn).where(partition_filter).count()
        )

        _phase_done("Compute skill demand daily", t0)

        # ══════════════════════════════════════════════════════════════════
        # Phase 3 — Salary by skill daily
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Compute salary by skill daily")

        salary_by_skill = compute_salary_by_skill_daily(skill_matches, jobs_df)

        salary_fqn = layout.gold_salary_by_skill_daily_fqn()
        _write_gold_table(salary_by_skill, salary_fqn, spark)

        result.salary_by_skill_rows = spark.read.table(salary_fqn).where(partition_filter).count()

        _phase_done("Compute salary by skill daily", t0)

        # ══════════════════════════════════════════════════════════════════
        # Phase 4 — Occupation-skill graph
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Compute occupation-skill graph")

        occ_skill_graph = compute_occupation_skill_graph(
            skill_matches, occ_matches, relations_df, occupations_df, skills_df
        )

        # Add lineage columns
        occ_skill_graph = (
            occ_skill_graph.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
            .withColumn(
                gold_schema.gold_meta("generated_at_utc"),
                F.lit(generated_at).cast("timestamp"),
            )
            .withColumn("esco_version", F.lit(esco_version))
            .withColumn("esco_lang", F.lit(esco_lang))
        )

        graph_fqn = layout.gold_occupation_skill_graph_fqn()
        _write_gold_table(occ_skill_graph, graph_fqn, spark)

        result.occupation_skill_graph_rows = (
            spark.read.table(graph_fqn).where(partition_filter).count()
        )

        _phase_done("Compute occupation-skill graph", t0)

        # ══════════════════════════════════════════════════════════════════
        # Phase 5 — Emerging-skill signals  (soft-fail: non-critical)
        # ══════════════════════════════════════════════════════════════════
        try:
            t0 = _phase_start("Compute emerging-skill signals")

            # Read full skill_demand_daily for same country (all dates, needed for acceleration/novelty)
            full_demand = spark.read.table(skill_demand_fqn).where(F.col("country") == country)

            emerging = compute_skill_emerging_daily(
                full_demand,
                ingestion_date=ingestion_date,
                weights=cfg.gold_analytics.emerging,
            )

            emerging_fqn = layout.gold_skill_emerging_daily_fqn()
            _write_gold_table(emerging, emerging_fqn, spark)

            result.skill_emerging_rows = (
                spark.read.table(emerging_fqn).where(partition_filter).count()
            )

            _phase_done("Compute emerging-skill signals", t0)
        except Exception:
            logger.warning(
                "Phase 5 (emerging-skill signals) failed — skipping. "
                "Core analytics are unaffected.",
                exc_info=True,
            )

        # ══════════════════════════════════════════════════════════════════
        # Phase 6 — Occupation market daily  (soft-fail: non-critical)
        # ══════════════════════════════════════════════════════════════════
        try:
            t0 = _phase_start("Compute occupation market daily")

            occ_market = compute_occupation_market_daily(occ_matches, skill_matches)

            occ_market_fqn = layout.gold_occupation_market_daily_fqn()
            _write_gold_table(occ_market, occ_market_fqn, spark)

            result.occupation_market_rows = (
                spark.read.table(occ_market_fqn).where(partition_filter).count()
            )

            _phase_done("Compute occupation market daily", t0)
        except Exception:
            logger.warning(
                "Phase 6 (occupation market daily) failed — skipping. "
                "Core analytics are unaffected.",
                exc_info=True,
            )

        # ══════════════════════════════════════════════════════════════════
        # Phase 7 — Skill demand segments (ML / KMeans, soft-fail)
        # ══════════════════════════════════════════════════════════════════
        try:
            t0 = _phase_start("Compute skill demand segments")

            # Read the just-written skill_demand for this partition
            demand_partition = spark.read.table(skill_demand_fqn).where(partition_filter)

            segments = compute_skill_demand_segments(
                demand_partition,
                ingestion_date=ingestion_date,
                config=cfg.gold_analytics.ml_segments,
            )

            segments_fqn = layout.gold_skill_demand_segments_daily_fqn()
            _write_gold_table(segments, segments_fqn, spark)

            result.skill_demand_segments_rows = (
                spark.read.table(segments_fqn).where(partition_filter).count()
            )

            _phase_done("Compute skill demand segments", t0)
        except Exception:
            logger.warning(
                "Phase 7 (skill demand segments / KMeans) failed — skipping. "
                "Core analytics are unaffected.",
                exc_info=True,
            )

        # ══════════════════════════════════════════════════════════════════
        # Phase 8 — Occupation profiles  (soft-fail: career navigation)
        # ══════════════════════════════════════════════════════════════════
        try:
            t0 = _phase_start("Compute occupation profiles")

            occ_profile = compute_occupation_profile_daily(
                occ_matches,
                skill_matches,
                spark.read.table(salary_fqn).where(partition_filter),
                occupations_df,
                relations_df,
                jobs_df,
                ingestion_date=ingestion_date,
                country=country,
                config=cfg.gold_analytics.career_nav,
            )

            # Add lineage columns
            occ_profile = (
                occ_profile.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
                .withColumn(
                    gold_schema.gold_meta("generated_at_utc"),
                    F.lit(generated_at).cast("timestamp"),
                )
                .withColumn("esco_version", F.lit(esco_version))
                .withColumn("esco_lang", F.lit(esco_lang))
            )

            occ_profile_fqn = layout.gold_occupation_profile_daily_fqn()
            _write_gold_table(occ_profile, occ_profile_fqn, spark)

            result.occupation_profile_rows = (
                spark.read.table(occ_profile_fqn).where(partition_filter).count()
            )

            _phase_done("Compute occupation profiles", t0)
        except Exception:
            logger.warning(
                "Phase 8 (occupation profiles) failed — skipping. Core analytics are unaffected.",
                exc_info=True,
            )

        # ══════════════════════════════════════════════════════════════════
        # Phase 9 — Skill profiles  (soft-fail: career navigation)
        # ══════════════════════════════════════════════════════════════════
        try:
            t0 = _phase_start("Compute skill profiles")

            skill_profile = compute_skill_profile_daily(
                spark.read.table(skill_demand_fqn).where(partition_filter),
                spark.read.table(salary_fqn).where(partition_filter),
                skill_matches,
                occ_matches,
                skills_df,
                jobs_df,
                ingestion_date=ingestion_date,
                country=country,
                config=cfg.gold_analytics.career_nav,
            )

            skill_profile = (
                skill_profile.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
                .withColumn(
                    gold_schema.gold_meta("generated_at_utc"),
                    F.lit(generated_at).cast("timestamp"),
                )
                .withColumn("esco_version", F.lit(esco_version))
                .withColumn("esco_lang", F.lit(esco_lang))
            )

            skill_profile_fqn = layout.gold_skill_profile_daily_fqn()
            _write_gold_table(skill_profile, skill_profile_fqn, spark)

            result.skill_profile_rows = (
                spark.read.table(skill_profile_fqn).where(partition_filter).count()
            )

            _phase_done("Compute skill profiles", t0)
        except Exception:
            logger.warning(
                "Phase 9 (skill profiles) failed — skipping. Core analytics are unaffected.",
                exc_info=True,
            )

        # ══════════════════════════════════════════════════════════════════
        # Phase 10 — Occupation similarity  (soft-fail: career navigation)
        # ══════════════════════════════════════════════════════════════════
        _similarity_df = None
        try:
            t0 = _phase_start("Compute occupation similarity")

            _similarity_df = compute_occupation_similarity_daily(
                relations_df,
                occupations_df,
                ingestion_date=ingestion_date,
                country=country,
                config=cfg.gold_analytics.career_nav,
            )

            _similarity_df = (
                _similarity_df.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
                .withColumn(
                    gold_schema.gold_meta("generated_at_utc"),
                    F.lit(generated_at).cast("timestamp"),
                )
                .withColumn("esco_version", F.lit(esco_version))
                .withColumn("esco_lang", F.lit(esco_lang))
            )

            sim_fqn = layout.gold_occupation_similarity_daily_fqn()
            _write_gold_table(_similarity_df, sim_fqn, spark)

            result.occupation_similarity_rows = (
                spark.read.table(sim_fqn).where(partition_filter).count()
            )

            # Re-read for transitions
            _similarity_df = spark.read.table(sim_fqn).where(partition_filter)

            _phase_done("Compute occupation similarity", t0)
        except Exception:
            logger.warning(
                "Phase 10 (occupation similarity) failed — skipping. "
                "Core analytics are unaffected.",
                exc_info=True,
            )

        # ══════════════════════════════════════════════════════════════════
        # Phase 11 — Occupation transitions  (soft-fail: career navigation)
        # ══════════════════════════════════════════════════════════════════
        try:
            if _similarity_df is None:
                logger.info("Skipping Phase 11 (occupation transitions) — similarity unavailable.")
            else:
                t0 = _phase_start("Compute occupation transitions")

                # Try to read occupation profiles for market context
                _occ_profile_ctx = None
                try:
                    _occ_profile_ctx = spark.read.table(
                        layout.gold_occupation_profile_daily_fqn()
                    ).where(partition_filter)
                except Exception:
                    logger.debug("Occupation profiles not available for transition context.")

                transitions = compute_occupation_transition_daily(
                    relations_df,
                    occupations_df,
                    skills_df,
                    _similarity_df,
                    _occ_profile_ctx,
                    ingestion_date=ingestion_date,
                    country=country,
                    config=cfg.gold_analytics.career_nav,
                )

                transitions = (
                    transitions.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
                    .withColumn(
                        gold_schema.gold_meta("generated_at_utc"),
                        F.lit(generated_at).cast("timestamp"),
                    )
                    .withColumn("esco_version", F.lit(esco_version))
                    .withColumn("esco_lang", F.lit(esco_lang))
                )

                trans_fqn = layout.gold_occupation_transition_daily_fqn()
                _write_gold_table(transitions, trans_fqn, spark)

                result.occupation_transition_rows = (
                    spark.read.table(trans_fqn).where(partition_filter).count()
                )

                _phase_done("Compute occupation transitions", t0)
        except Exception:
            logger.warning(
                "Phase 11 (occupation transitions) failed — skipping. "
                "Core analytics are unaffected.",
                exc_info=True,
            )

        logger.info(
            "Gold analytics complete: demand=%d salary=%d graph=%d "
            "emerging=%d occ_market=%d segments=%d "
            "occ_profile=%d skill_profile=%d similarity=%d transitions=%d",
            result.skill_demand_rows,
            result.salary_by_skill_rows,
            result.occupation_skill_graph_rows,
            result.skill_emerging_rows,
            result.occupation_market_rows,
            result.skill_demand_segments_rows,
            result.occupation_profile_rows,
            result.skill_profile_rows,
            result.occupation_similarity_rows,
            result.occupation_transition_rows,
        )

    except Exception as exc:
        result.success = False
        result.error = str(exc)
        logger.exception("Gold analytics failed")
    finally:
        for df in _persisted:
            with contextlib.suppress(Exception):
                df.unpersist()

    return result
