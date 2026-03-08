"""Gold occupation-level market analytics — daily occupation demand metrics.

Grain: one row per (ingestion_date, country, esco_occupation_concept_uri).

Aggregates from Gold job-occupation matches to produce:
- total_jobs_count — distinct jobs matched to this occupation
- unique_skills_count — number of distinct ESCO skills co-occurring
- avg_match_score — mean match score across all job-occupation pairs
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import schema as gold_schema

logger = logging.getLogger(__name__)


def compute_occupation_market_daily(
    occ_matches_df: DataFrame,
    skill_matches_df: DataFrame,
) -> DataFrame:
    """Compute daily occupation-level market analytics.

    Parameters
    ----------
    occ_matches_df:
        Gold job-occupation matches with columns including job_id, country,
        ingestion_date, esco_occupation_concept_uri, match_score, plus
        Gold lineage columns.
    skill_matches_df:
        Gold job-skill matches used to count co-occurring skills per
        occupation via shared job_id.

    Returns
    -------
    DataFrame
        One row per (ingestion_date, country, esco_occupation_concept_uri)
        with occupation-level market metrics.
    """
    logger.info("Computing daily occupation market analytics")

    occ_uri_col = gold_schema.esco_occupation("concept_uri")
    occ_label_col = gold_schema.esco_occupation("preferred_label")
    skill_uri_col = gold_schema.esco_skill("concept_uri")

    # Step 1: basic occupation aggs from occ_matches
    occ_aggs = occ_matches_df.groupBy(
        "ingestion_date",
        "country",
        occ_uri_col,
        occ_label_col,
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    ).agg(
        F.countDistinct("job_id").alias("total_jobs_count"),
        F.avg("match_score").alias("avg_match_score"),
    )

    # Step 2: count unique skills co-occurring with each occupation's jobs
    occ_jobs = occ_matches_df.select(
        "job_id",
        "country",
        "ingestion_date",
        occ_uri_col,
    )

    job_skills = skill_matches_df.select(
        F.col("job_id").alias("_sj_job_id"),
        F.col("country").alias("_sj_country"),
        F.col("ingestion_date").alias("_sj_ing_date"),
        F.col(skill_uri_col).alias("_skill_uri"),
    )

    occ_skill_pairs = occ_jobs.join(
        job_skills,
        on=[
            occ_jobs["job_id"] == job_skills["_sj_job_id"],
            occ_jobs["country"] == job_skills["_sj_country"],
            occ_jobs["ingestion_date"] == job_skills["_sj_ing_date"],
        ],
        how="inner",
    ).drop("_sj_job_id", "_sj_country", "_sj_ing_date")

    unique_skills_per_occ = occ_skill_pairs.groupBy(
        "ingestion_date",
        "country",
        occ_uri_col,
    ).agg(
        F.countDistinct("_skill_uri").alias("unique_skills_count"),
    )

    # Step 3: join skill counts into occupation aggs
    result = occ_aggs.join(
        unique_skills_per_occ,
        on=["ingestion_date", "country", occ_uri_col],
        how="left",
    ).fillna(0, subset=["unique_skills_count"])

    return result.select(
        "ingestion_date",
        "country",
        occ_uri_col,
        occ_label_col,
        "total_jobs_count",
        "unique_skills_count",
        "avg_match_score",
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    )
