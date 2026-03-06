"""Gold occupation matching — deterministic job-to-occupation inference.

Two matching signals:
1. Title-to-occupation label exact phrase match.
2. Relation-based inference: infer occupations from matched skills
   via ESCO Silver occupation-skill relations.

Grain of output: one row per (job_id, country, ingestion_date,
esco_occupation_concept_uri, match_method).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .. import schema as gold_schema
from .models import (
    MATCH_METHOD_OCCUPATION_RELATION,
    MATCH_METHOD_OCCUPATION_TITLE,
    OCCUPATION_RELATION_BASE_SCORE,
    OCCUPATION_RELATION_MAX_SCORE,
    OCCUPATION_RELATION_SKILL_WEIGHT,
    OCCUPATION_TITLE_MATCH_SCORE,
)

logger = logging.getLogger(__name__)


# ── Title-based occupation matching ───────────────────────────────────────


def match_jobs_to_occupations_by_title(
    jobs_df: DataFrame,
    occupations_df: DataFrame,
) -> DataFrame:
    """Match jobs to ESCO occupations by exact title phrase match.

    Compares job ``title_normalized`` against occupation
    ``preferred_label`` (normalized). Uses an equi-join on a normalized
    label column for efficiency.

    Parameters
    ----------
    jobs_df:
        Adzuna Silver jobs with title_normalized, job_id, etc.
    occupations_df:
        ESCO Silver occupations with concept_uri, preferred_label, etc.

    Returns
    -------
    DataFrame
        Match rows with match_method = title_exact_v1.
    """
    logger.info("Matching jobs to occupations by title")

    # Prepare occupation labels (normalized preferred_label).
    # Column names governed by gold.schema.
    occ_labels = occupations_df.select(
        F.col("concept_uri").alias(gold_schema.esco_occupation("concept_uri")),
        F.col("concept_uri_uuid").alias(gold_schema.esco_occupation("concept_uri_uuid")),
        F.col("preferred_label").alias(gold_schema.esco_occupation("preferred_label")),
        F.lower(F.regexp_replace(F.trim(F.col("preferred_label")), r"\s+", " ")).alias(
            "_occ_label_normalized"
        ),
    )

    # Join on normalized labels
    matches = jobs_df.join(
        F.broadcast(occ_labels),
        jobs_df["title_normalized"] == occ_labels["_occ_label_normalized"],
        how="inner",
    )

    result = matches.select(
        F.col("source_system"),
        F.col("country"),
        F.col("ingestion_date"),
        F.col("job_id"),
        F.col("adref"),
        F.col("posted_date"),
        F.col(gold_schema.esco_occupation("concept_uri")),
        F.col(gold_schema.esco_occupation("concept_uri_uuid")),
        F.col(gold_schema.esco_occupation("preferred_label")),
        F.lit(MATCH_METHOD_OCCUPATION_TITLE).alias("match_method"),
        F.lit(OCCUPATION_TITLE_MATCH_SCORE).alias("match_score"),
        F.lit("title").alias(gold_schema.matched("text_source")),
        F.lit(0).cast("integer").alias("supporting_skill_match_count"),
        F.col("silver_run_id").alias("adzuna_silver_run_id"),
    )

    return result


# ── Relation-based occupation matching ────────────────────────────────────


def match_jobs_to_occupations_by_relations(
    skill_matches_df: DataFrame,
    relations_df: DataFrame,
    occupations_df: DataFrame,
) -> DataFrame:
    """Infer job-occupation matches from ESCO skill-occupation relations.

    For each job that has matched skills, look up which occupations those
    skills belong to via ESCO relations. Score based on the number and
    quality of supporting matched skills.

    Parameters
    ----------
    skill_matches_df:
        Deduplicated Gold job-skill matches (output of skill matching).
    relations_df:
        ESCO Silver relations with occupation_uri, skill_uri, relation_type.
    occupations_df:
        ESCO Silver occupations for label lookup.

    Returns
    -------
    DataFrame
        Inferred occupation matches with match_method = relation_score_v1.
    """
    logger.info("Inferring occupations from matched skills via ESCO relations")

    # Join skill matches to relations
    skill_to_occ = skill_matches_df.join(
        F.broadcast(
            relations_df.select(
                F.col("occupation_uri"),
                F.col("skill_uri"),
                F.col("relation_type"),
            )
        ),
        skill_matches_df["esco_skill_concept_uri"] == relations_df["skill_uri"],
        how="inner",
    )

    # Aggregate: count supporting skills per (job, occupation)
    job_occ_agg = skill_to_occ.groupBy(
        "source_system",
        "country",
        "ingestion_date",
        "job_id",
        "adref",
        "posted_date",
        "occupation_uri",
        "adzuna_silver_run_id",
    ).agg(
        F.countDistinct(gold_schema.esco_skill("concept_uri")).alias(
            "supporting_skill_match_count"
        ),
    )

    # Compute score
    score_expr = F.least(
        F.lit(OCCUPATION_RELATION_BASE_SCORE)
        + (
            F.col("supporting_skill_match_count").cast("double")
            * F.lit(OCCUPATION_RELATION_SKILL_WEIGHT)
        ),
        F.lit(OCCUPATION_RELATION_MAX_SCORE),
    )
    job_occ_agg = job_occ_agg.withColumn("match_score", score_expr)

    # Enrich with occupation labels
    occ_labels = occupations_df.select(
        F.col("concept_uri").alias("_occ_uri"),
        F.col("concept_uri_uuid").alias(gold_schema.esco_occupation("concept_uri_uuid")),
        F.col("preferred_label").alias(gold_schema.esco_occupation("preferred_label")),
    )

    result = job_occ_agg.join(
        F.broadcast(occ_labels),
        job_occ_agg["occupation_uri"] == occ_labels["_occ_uri"],
        how="inner",
    ).select(
        F.col("source_system"),
        F.col("country"),
        F.col("ingestion_date"),
        F.col("job_id"),
        F.col("adref"),
        F.col("posted_date"),
        F.col("occupation_uri").alias(gold_schema.esco_occupation("concept_uri")),
        F.col(gold_schema.esco_occupation("concept_uri_uuid")),
        F.col(gold_schema.esco_occupation("preferred_label")),
        F.lit(MATCH_METHOD_OCCUPATION_RELATION).alias("match_method"),
        F.col("match_score"),
        F.lit("relation").alias(gold_schema.matched("text_source")),
        F.col("supporting_skill_match_count"),
        F.col("adzuna_silver_run_id"),
    )

    return result


# ── Combine and deduplicate ───────────────────────────────────────────────


def combine_occupation_matches(
    title_matches: DataFrame,
    relation_matches: DataFrame,
) -> DataFrame:
    """Combine title-based and relation-based occupation matches.

    Deduplicates by (job_id, country, ingestion_date,
    esco_occupation_concept_uri, match_method). If the same occupation is
    matched by both methods, both rows are kept (different match_method).
    Within the same method, keep the highest score.
    """
    combined = title_matches.unionByName(relation_matches)

    key_cols = [
        "job_id",
        "country",
        "ingestion_date",
        gold_schema.esco_occupation("concept_uri"),
        "match_method",
    ]

    window = Window.partitionBy(*key_cols).orderBy(
        F.col("match_score").desc(),
        F.col("supporting_skill_match_count").desc(),
    )

    deduped = (
        combined.withColumn("__rn__", F.row_number().over(window))
        .where(F.col("__rn__") == 1)
        .drop("__rn__")
    )

    return deduped
