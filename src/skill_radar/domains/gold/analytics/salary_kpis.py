"""Gold salary-by-skill KPIs — daily salary metrics for skill-matched jobs.

Grain: one row per (ingestion_date, country, esco_skill_concept_uri).

Only includes jobs that have non-null salary data.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import schema as gold_schema

logger = logging.getLogger(__name__)


def compute_salary_by_skill_daily(
    skill_matches_df: DataFrame,
    jobs_df: DataFrame,
) -> DataFrame:
    """Compute daily salary metrics by skill (salary-populated jobs only).

    Parameters
    ----------
    skill_matches_df:
        Gold job-skill matches (deduplicated).
    jobs_df:
        Adzuna Silver jobs with salary fields.

    Returns
    -------
    DataFrame
        One row per (ingestion_date, country, esco_skill_concept_uri)
        with salary statistics.
    """
    logger.info("Computing daily salary-by-skill KPIs")

    # Join matches with job salary data
    enriched = skill_matches_df.join(
        jobs_df.select(
            F.col("job_id").alias("_job_id"),
            F.col("country").alias("_country"),
            F.col("ingestion_date").alias("_ing_date"),
            F.col("salary_min"),
            F.col("salary_max"),
            F.col("salary_mean"),
        ),
        on=[
            skill_matches_df["job_id"] == F.col("_job_id"),
            skill_matches_df["country"] == F.col("_country"),
            skill_matches_df["ingestion_date"] == F.col("_ing_date"),
        ],
        how="inner",
    ).drop("_job_id", "_country", "_ing_date")

    # Filter to salary-populated jobs only
    salary_jobs = enriched.where(
        F.col("salary_min").isNotNull()
        | F.col("salary_max").isNotNull()
        | F.col("salary_mean").isNotNull()
    )

    # Aggregate
    # Aggregate by (ingestion_date, country, skill).
    # Column names governed by gold.schema (single source of truth).
    kpis = salary_jobs.groupBy(
        "ingestion_date",
        "country",
        gold_schema.esco_skill("concept_uri"),
        gold_schema.esco_skill("concept_uri_uuid"),
        gold_schema.esco_skill("preferred_label"),
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    ).agg(
        F.countDistinct("job_id").alias("salary_jobs_count"),
        F.avg("salary_min").alias("avg_salary_min"),
        F.avg("salary_max").alias("avg_salary_max"),
        F.avg("salary_mean").alias("avg_salary_mean"),
        F.min("salary_min").alias("min_salary_min"),
        F.max("salary_max").alias("max_salary_max"),
    )

    return kpis
