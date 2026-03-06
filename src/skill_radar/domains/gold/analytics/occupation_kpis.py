"""Gold occupation KPIs — occupation-skill graph enriched with market data.

Grain: one row per (ingestion_date, country, esco_occupation_concept_uri,
esco_skill_concept_uri, relation_type).

Combines ESCO Silver occupation-skill relations with Gold matching
evidence (how many jobs matched each skill/occupation pair).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import schema as gold_schema

logger = logging.getLogger(__name__)


def compute_occupation_skill_graph(
    skill_matches_df: DataFrame,
    occ_matches_df: DataFrame,
    relations_df: DataFrame,
    occupations_df: DataFrame,
    skills_df: DataFrame,
) -> DataFrame:
    """Build the occupation-skill graph enriched with daily market evidence.

    For each ESCO (occupation, skill, relation_type) triple, counts how many
    jobs from the current partition matched both the occupation and the skill.

    Parameters
    ----------
    skill_matches_df:
        Gold job-skill matches.
    occ_matches_df:
        Gold job-occupation matches.
    relations_df:
        ESCO Silver relations (occupation_uri, skill_uri, relation_type).
    occupations_df:
        ESCO Silver occupations for label lookup.
    skills_df:
        ESCO Silver skills for label lookup.

    Returns
    -------
    DataFrame
        Occupation-skill graph with matched_jobs_count.
    """
    logger.info("Computing occupation-skill graph with market evidence")

    # Start from ESCO relations as the backbone.
    # Column names governed by gold.schema (single source of truth).
    base_relations = relations_df.select(
        F.col("occupation_uri").alias(gold_schema.esco_occupation("concept_uri")),
        F.col("skill_uri").alias(gold_schema.esco_skill("concept_uri")),
        F.col("relation_type"),
    )

    # Find jobs that matched both a given occupation AND a given skill
    # Join occupation matches with skill matches on job_id
    job_occ_skill = (
        occ_matches_df.select(
            "job_id",
            "country",
            "ingestion_date",
            F.col(gold_schema.esco_occupation("concept_uri")).alias("_occ_uri"),
        )
        .join(
            skill_matches_df.select(
                F.col("job_id").alias("_sj_job_id"),
                F.col("country").alias("_sj_country"),
                F.col("ingestion_date").alias("_sj_ing_date"),
                F.col(gold_schema.esco_skill("concept_uri")).alias("_skill_uri"),
            ),
            on=[
                F.col("job_id") == F.col("_sj_job_id"),
                F.col("country") == F.col("_sj_country"),
                F.col("ingestion_date") == F.col("_sj_ing_date"),
            ],
            how="inner",
        )
        .drop("_sj_job_id", "_sj_country", "_sj_ing_date")
    )

    # Count jobs per (occupation, skill, country, ingestion_date)
    evidence = job_occ_skill.groupBy(
        "country",
        "ingestion_date",
        F.col("_occ_uri").alias(gold_schema.esco_occupation("concept_uri")),
        F.col("_skill_uri").alias(gold_schema.esco_skill("concept_uri")),
    ).agg(
        F.countDistinct("job_id").alias(gold_schema.matched("jobs_count")),
    )

    # Join evidence back to base relations
    graph = base_relations.join(
        evidence,
        on=[gold_schema.esco_occupation("concept_uri"), gold_schema.esco_skill("concept_uri")],
        how="left",
    )

    # Fill nulls for unmatched relation pairs
    graph = graph.withColumn(
        gold_schema.matched("jobs_count"),
        F.coalesce(F.col(gold_schema.matched("jobs_count")), F.lit(0)),
    )

    # Enrich with occupation labels
    occ_labels = occupations_df.select(
        F.col("concept_uri").alias("_occ_uri_label"),
        F.col("concept_uri_uuid").alias(gold_schema.esco_occupation("concept_uri_uuid")),
        F.col("preferred_label").alias(gold_schema.esco_occupation("preferred_label")),
    )
    graph = graph.join(
        F.broadcast(occ_labels),
        graph[gold_schema.esco_occupation("concept_uri")] == occ_labels["_occ_uri_label"],
        how="left",
    ).drop("_occ_uri_label")

    # Enrich with skill labels
    skill_labels = skills_df.select(
        F.col("concept_uri").alias("_skill_uri_label"),
        F.col("concept_uri_uuid").alias(gold_schema.esco_skill("concept_uri_uuid")),
        F.col("preferred_label").alias(gold_schema.esco_skill("preferred_label")),
    )
    graph = graph.join(
        F.broadcast(skill_labels),
        graph[gold_schema.esco_skill("concept_uri")] == skill_labels["_skill_uri_label"],
        how="left",
    ).drop("_skill_uri_label")

    # Filter to rows that have at least country and ingestion_date
    # (from the evidence join), or keep all relation pairs including unmatched
    # For unmatched pairs, country/ingestion_date will be null — we filter them
    # out since the graph is per-partition
    graph = graph.where(F.col("country").isNotNull() & F.col("ingestion_date").isNotNull())

    return graph
