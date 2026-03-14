"""Gold matching orchestrator — sequences skill and occupation matching.

Sequences the Gold matching pipeline:
1. Read & persist ESCO Silver dimensions and Adzuna Silver jobs
2. Build ESCO label dimension and job candidate phrases
3. Match jobs to skills via candidate equi-join
4. Match jobs to occupations (title + relation inference)
5. Write Gold Iceberg tables with partition overwrite
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
from .job_candidates import build_job_candidates
from .label_dimension import build_label_dimension, max_label_tokens
from .models import CANDIDATE_MAX_NGRAM_SIZE, GoldMatchingResult
from .occupation_matching import (
    combine_occupation_matches,
    match_jobs_to_occupations_by_relations,
    match_jobs_to_occupations_by_title,
)
from .skill_matching import (
    deduplicate_skill_matches,
    match_jobs_to_skills,
)

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)

# Storage level for intermediate DataFrames that are reused.
_PERSIST_LEVEL = StorageLevel.MEMORY_AND_DISK


# ── Helpers ───────────────────────────────────────────────────────────────


def _phase_start(label: str) -> float:
    """Log a phase header and return monotonic start time."""
    logger.info("▶ %s", label)
    return time.monotonic()


def _phase_done(label: str, start: float) -> None:
    """Log phase completion with elapsed time."""
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


def run_gold_matching(
    spark: SparkSession,
    *,
    ingestion_date: str,
    country: str,
    esco_version: str,
    esco_lang: str,
    job_limit: int | None = None,
    run_id: str | None = None,
    config: PlatformSettings | None = None,
) -> GoldMatchingResult:
    """Execute the Gold matching pipeline.

    Parameters
    ----------
    spark:
        Active SparkSession with Iceberg catalog ``sr`` configured.
    ingestion_date:
        Adzuna ingestion date partition (YYYY-MM-DD).
    country:
        Country scope for Adzuna daily partition.
    esco_version:
        ESCO Silver version filter.
    esco_lang:
        ESCO Silver language filter.
    job_limit:
        Optional limit on Adzuna jobs (debugging only).
    run_id:
        Run identifier for lineage.  Defaults from logging context.
    config:
        Platform configuration.  Loaded from defaults if not provided.

    Returns
    -------
    GoldMatchingResult
        Structured result with metrics and lineage metadata.
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

    result = GoldMatchingResult(
        run_id=resolved_run_id,
        ingestion_date=ingestion_date,
        country=country,
        esco_version=esco_version,
        esco_lang=esco_lang,
    )

    logger.info(
        "Starting Gold matching: country=%s date=%s esco=%s/%s run_id=%s",
        country,
        ingestion_date,
        esco_version,
        esco_lang,
        resolved_run_id,
    )

    # Track persisted DataFrames for cleanup
    _persisted: list[DataFrame] = []

    def _p(df: DataFrame) -> DataFrame:
        """Persist and register for cleanup."""
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
        t0 = _phase_start("Read inputs")

        skills_fqn = layout.esco_silver_skills_fqn()
        occupations_fqn = layout.esco_silver_occupations_fqn()
        relations_fqn = layout.esco_silver_relations_fqn()
        adzuna_fqn = layout.adzuna_silver_jobs_fqn()

        esco_filter = (F.col("version") == esco_version) & (F.col("lang") == esco_lang)

        skills_df = _p(spark.read.table(skills_fqn).where(esco_filter))
        occupations_df = _p(spark.read.table(occupations_fqn).where(esco_filter))
        relations_df = _p(spark.read.table(relations_fqn).where(esco_filter))

        jobs_df = spark.read.table(adzuna_fqn).where(
            (F.col("country") == country) & (F.col("ingestion_date") == ingestion_date)
        )
        if job_limit and job_limit > 0:
            logger.info("Applying job_limit=%d for debugging", job_limit)
            jobs_df = jobs_df.limit(job_limit)
        jobs_df = _p(jobs_df)

        # We need job count for early-exit and final result
        jobs_count = jobs_df.count()
        result.adzuna_jobs_count = jobs_count

        _phase_done("Read inputs", t0)

        if jobs_count == 0:
            logger.warning(
                "No Adzuna jobs for country=%s date=%s — skipping matching",
                country,
                ingestion_date,
            )
            return result

        # ══════════════════════════════════════════════════════════════════
        # Phase 2 — Build ESCO label dimension
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Build label dimension")

        label_dim = _p(build_label_dimension(skills_df))
        # Compute effective n-gram cap from data
        effective_max_n = min(max_label_tokens(label_dim), CANDIDATE_MAX_NGRAM_SIZE)
        result.skill_dictionary_size = label_dim.count()

        _phase_done("Build label dimension", t0)
        logger.info(
            "Label dimension: %d rows, max_token=%d",
            result.skill_dictionary_size,
            effective_max_n,
        )

        # ══════════════════════════════════════════════════════════════════
        # Phase 3 — Build job candidate phrases
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Build job candidates")

        job_candidates = build_job_candidates(jobs_df, max_ngram_size=effective_max_n)
        # No persist here — consumed once in the join below

        _phase_done("Build job candidates", t0)

        # ══════════════════════════════════════════════════════════════════
        # Phase 4 — Skill matching (equi-join + dedup)
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Skill matching")

        raw_skill_matches = match_jobs_to_skills(jobs_df, label_dim, job_candidates)
        skill_matches = deduplicate_skill_matches(raw_skill_matches)

        # Resolve adzuna_silver_run_id
        adzuna_run_ids = jobs_df.select("silver_run_id").distinct().collect()
        adzuna_silver_run_id = adzuna_run_ids[0]["silver_run_id"] if adzuna_run_ids else ""

        # Add lineage columns
        skill_matches = (
            skill_matches.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
            .withColumn(
                gold_schema.gold_meta("generated_at_utc"),
                F.lit(generated_at).cast("timestamp"),
            )
            .withColumn("adzuna_silver_run_id", F.lit(adzuna_silver_run_id))
            .withColumn("esco_version", F.lit(esco_version))
            .withColumn("esco_lang", F.lit(esco_lang))
            .drop("silver_run_id")
        )

        # Persist deduped skill matches — reused for occupation inference + write
        skill_matches = _p(skill_matches)
        skill_match_count = skill_matches.count()
        result.job_skill_matches_count = skill_match_count

        _phase_done("Skill matching", t0)
        logger.info("Job-skill matches (deduplicated): %d", skill_match_count)

        # ══════════════════════════════════════════════════════════════════
        # Phase 5 — Occupation matching
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Occupation matching")

        # 5a. Title-based
        jobs_for_occ = jobs_df.select(
            "source_system",
            "country",
            "ingestion_date",
            "job_id",
            "adref",
            "posted_date",
            "title_normalized",
            "silver_run_id",
        )
        title_occ_matches = match_jobs_to_occupations_by_title(jobs_for_occ, occupations_df)

        # 5b. Relation-based (consume persisted skill matches)
        skill_matches_for_rel = skill_matches.select(
            "source_system",
            "country",
            "ingestion_date",
            "job_id",
            "adref",
            "posted_date",
            gold_schema.esco_skill("concept_uri"),
            F.col("adzuna_silver_run_id"),
        )
        relation_occ_matches = match_jobs_to_occupations_by_relations(
            skill_matches_for_rel, relations_df, occupations_df
        )

        # 5c. Combine + dedup
        occ_matches = combine_occupation_matches(title_occ_matches, relation_occ_matches)

        # Add lineage columns
        occ_matches = (
            occ_matches.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
            .withColumn(
                gold_schema.gold_meta("generated_at_utc"),
                F.lit(generated_at).cast("timestamp"),
            )
            .withColumn(
                "adzuna_silver_run_id",
                F.coalesce(F.col("adzuna_silver_run_id"), F.lit(adzuna_silver_run_id)),
            )
            .withColumn("esco_version", F.lit(esco_version))
            .withColumn("esco_lang", F.lit(esco_lang))
        )

        occ_match_count = occ_matches.count()
        result.job_occupation_matches_count = occ_match_count

        _phase_done("Occupation matching", t0)
        logger.info("Job-occupation matches (deduplicated): %d", occ_match_count)

        # ══════════════════════════════════════════════════════════════════
        # Phase 6 — Write Gold Iceberg tables
        # ══════════════════════════════════════════════════════════════════
        t0 = _phase_start("Write Gold tables")

        skill_table = layout.gold_job_skill_matches_fqn()
        occ_table = layout.gold_job_occupation_matches_fqn()

        _write_gold_table(skill_matches, skill_table, spark)
        _write_gold_table(occ_matches, occ_table, spark)

        _phase_done("Write Gold tables", t0)
        logger.info(
            "Gold matching complete: skill_matches=%d occ_matches=%d",
            skill_match_count,
            occ_match_count,
        )

    except Exception as exc:
        result.success = False
        result.error = str(exc)
        logger.exception("Gold matching failed")
    finally:
        # ── Cleanup persisted DataFrames ──────────────────────────────────
        for df in _persisted:
            with contextlib.suppress(Exception):
                df.unpersist()

    return result
