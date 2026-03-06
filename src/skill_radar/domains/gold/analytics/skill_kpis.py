"""Gold skill demand KPIs — daily demand metrics by ESCO skill.

Grain: one row per (ingestion_date, country, esco_skill_concept_uri).

Aggregates from Gold job-skill matches joined with Adzuna Silver job facts
to produce daily demand indicators.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import schema as gold_schema

logger = logging.getLogger(__name__)


def compute_skill_demand_daily(
    skill_matches_df: DataFrame,
    jobs_df: DataFrame,
) -> DataFrame:
    """Compute daily skill demand KPIs.

    Parameters
    ----------
    skill_matches_df:
        Gold job-skill matches (deduplicated) with columns including
        job_id, country, ingestion_date, esco_skill_concept_uri, etc.
    jobs_df:
        Adzuna Silver jobs with salary, company, and location fields.

    Returns
    -------
    DataFrame
        One row per (ingestion_date, country, esco_skill_concept_uri)
        with demand and salary KPIs.
    """
    logger.info("Computing daily skill demand KPIs")

    # Join matches with job facts for salary/company/location
    enriched = skill_matches_df.join(
        jobs_df.select(
            F.col("job_id").alias("_job_id"),
            F.col("country").alias("_country"),
            F.col("ingestion_date").alias("_ing_date"),
            F.col("salary_min"),
            F.col("salary_max"),
            F.col("salary_mean"),
            F.col("company_name"),
            F.col("location_display_name"),
            F.col("posted_date").alias("job_posted_date"),
        ),
        on=[
            skill_matches_df["job_id"] == F.col("_job_id"),
            skill_matches_df["country"] == F.col("_country"),
            skill_matches_df["ingestion_date"] == F.col("_ing_date"),
        ],
        how="left",
    ).drop("_job_id", "_country", "_ing_date")

    # Aggregate by (ingestion_date, country, skill).
    # Column names governed by gold.schema (single source of truth).
    kpis = enriched.groupBy(
        "ingestion_date",
        "country",
        gold_schema.esco_skill("concept_uri"),
        gold_schema.esco_skill("concept_uri_uuid"),
        gold_schema.esco_skill("preferred_label"),
        gold_schema.esco_skill("type"),
        gold_schema.esco_skill("reuse_level"),
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    ).agg(
        F.countDistinct("job_id").alias("jobs_count"),
        F.countDistinct("company_name").alias("unique_companies_count"),
        F.countDistinct("location_display_name").alias("unique_locations_count"),
        F.sum(
            F.when(F.col("title_hit") == True, F.lit(1)).otherwise(F.lit(0))  # noqa: E712
        ).alias("title_match_jobs_count"),
        F.sum(
            F.when(F.col("description_hit") == True, F.lit(1)).otherwise(F.lit(0))  # noqa: E712
        ).alias("description_match_jobs_count"),
        F.avg("salary_min").alias("avg_salary_min"),
        F.avg("salary_max").alias("avg_salary_max"),
        F.avg("salary_mean").alias("avg_salary_mean"),
        F.min("job_posted_date").alias("first_seen_posted_date"),
        F.max("job_posted_date").alias("last_seen_posted_date"),
    )

    # Rename reuse_level for output schema consistency
    kpis = kpis.withColumnRenamed(gold_schema.esco_skill("reuse_level"), "reuse_level")

    return kpis
