"""Gold matching orchestrator — sequences skill and occupation matching.

This module owns the sequencing of the Gold matching pipeline:
1. Read ESCO Silver dimensions (skills, occupations, relations).
2. Read Adzuna Silver daily job partition.
3. Build skill label dictionary.
4. Match jobs to skills.
5. Match jobs to occupations (title + relation inference).
6. Write Gold Iceberg tables with partition overwrite.

All table FQNs are resolved via :class:`LakeLayout`. No hardcoded paths or
table names.
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
from .models import GoldMatchingResult
from .occupation_matching import (
    combine_occupation_matches,
    match_jobs_to_occupations_by_relations,
    match_jobs_to_occupations_by_title,
)
from .skill_matching import (
    build_skill_label_dictionary,
    deduplicate_skill_matches,
    match_jobs_to_skills,
)

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ── Iceberg write helpers ─────────────────────────────────────────────────


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
        Run identifier for lineage. Defaults from logging context.
    config:
        Platform configuration. Loaded from defaults if not provided.

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

    try:
        # Ensure Gold namespace
        gold_ns = layout.iceberg_namespace("gold")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_ns}")
        logger.info("Ensured Gold namespace: %s", gold_ns)

        # ── 1. Read ESCO Silver dimensions ────────────────────────────────

        skills_fqn = layout.esco_silver_skills_fqn()
        occupations_fqn = layout.esco_silver_occupations_fqn()
        relations_fqn = layout.esco_silver_relations_fqn()

        logger.info("Reading ESCO Silver skills from %s", skills_fqn)
        skills_df = spark.read.table(skills_fqn).where(
            (F.col("version") == esco_version) & (F.col("lang") == esco_lang)
        )
        skills_count = skills_df.count()
        logger.info("ESCO Silver skills: %d rows", skills_count)

        logger.info("Reading ESCO Silver occupations from %s", occupations_fqn)
        occupations_df = spark.read.table(occupations_fqn).where(
            (F.col("version") == esco_version) & (F.col("lang") == esco_lang)
        )
        occupations_count = occupations_df.count()
        logger.info("ESCO Silver occupations: %d rows", occupations_count)

        logger.info("Reading ESCO Silver relations from %s", relations_fqn)
        relations_df = spark.read.table(relations_fqn).where(
            (F.col("version") == esco_version) & (F.col("lang") == esco_lang)
        )
        relations_count = relations_df.count()
        logger.info("ESCO Silver relations: %d rows", relations_count)

        # ── 2. Read Adzuna Silver daily partition ─────────────────────────

        adzuna_fqn = layout.adzuna_silver_jobs_fqn()
        logger.info("Reading Adzuna Silver jobs from %s", adzuna_fqn)
        jobs_df = spark.read.table(adzuna_fqn).where(
            (F.col("country") == country) & (F.col("ingestion_date") == ingestion_date)
        )

        if job_limit and job_limit > 0:
            logger.info("Applying job_limit=%d for debugging", job_limit)
            jobs_df = jobs_df.limit(job_limit)

        jobs_count = jobs_df.count()
        result.adzuna_jobs_count = jobs_count
        logger.info("Adzuna Silver jobs: %d rows", jobs_count)

        if jobs_count == 0:
            logger.warning(
                "No Adzuna jobs for country=%s date=%s — skipping matching",
                country,
                ingestion_date,
            )
            return result

        # ── 3. Build skill label dictionary ───────────────────────────────

        skill_labels = build_skill_label_dictionary(skills_df)
        dict_size = skill_labels.count()
        result.skill_dictionary_size = dict_size
        logger.info("Skill label dictionary: %d label rows", dict_size)

        # ── 4. Match jobs to skills ───────────────────────────────────────

        raw_skill_matches = match_jobs_to_skills(jobs_df, skill_labels, spark)
        skill_matches = deduplicate_skill_matches(raw_skill_matches)

        # Resolve adzuna_silver_run_id from the jobs partition
        adzuna_run_ids = jobs_df.select("silver_run_id").distinct().collect()
        adzuna_silver_run_id = adzuna_run_ids[0]["silver_run_id"] if adzuna_run_ids else ""

        # Add lineage columns
        skill_matches = (
            skill_matches.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
            .withColumn(
                gold_schema.gold_meta("generated_at_utc"), F.lit(generated_at).cast("timestamp")
            )
            .withColumn("adzuna_silver_run_id", F.lit(adzuna_silver_run_id))
            .withColumn("esco_version", F.lit(esco_version))
            .withColumn("esco_lang", F.lit(esco_lang))
            .drop("silver_run_id")
        )

        skill_match_count = skill_matches.count()
        result.job_skill_matches_count = skill_match_count
        logger.info("Job-skill matches (deduplicated): %d rows", skill_match_count)

        # ── 5. Match jobs to occupations ──────────────────────────────────

        # Prepare jobs for occupation matching
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

        # 5a. Title-based matching
        title_occ_matches = match_jobs_to_occupations_by_title(jobs_for_occ, occupations_df)

        # 5b. Relation-based matching (from skill matches)
        # Prepare skill matches for relation lookup
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

        # 5c. Combine and deduplicate
        occ_matches = combine_occupation_matches(title_occ_matches, relation_occ_matches)

        # Add lineage columns
        occ_matches = (
            occ_matches.withColumn(gold_schema.gold_meta("run_id"), F.lit(resolved_run_id))
            .withColumn(
                gold_schema.gold_meta("generated_at_utc"), F.lit(generated_at).cast("timestamp")
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
        logger.info("Job-occupation matches (deduplicated): %d rows", occ_match_count)

        # ── 6. Write Gold Iceberg tables ──────────────────────────────────

        skill_table = layout.gold_job_skill_matches_fqn()
        occ_table = layout.gold_job_occupation_matches_fqn()

        logger.info(
            "Writing skill matches to %s (partition: %s/%s)",
            skill_table,
            ingestion_date,
            country,
        )
        _write_gold_table(skill_matches, skill_table, spark)

        logger.info(
            "Writing occupation matches to %s (partition: %s/%s)",
            occ_table,
            ingestion_date,
            country,
        )
        _write_gold_table(occ_matches, occ_table, spark)

        logger.info(
            "Gold matching complete: skill_matches=%d occ_matches=%d",
            skill_match_count,
            occ_match_count,
        )

    except Exception as exc:
        result.success = False
        result.error = str(exc)
        logger.exception("Gold matching failed")

    return result
