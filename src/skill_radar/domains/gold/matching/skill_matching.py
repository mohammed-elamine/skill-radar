"""Gold skill matching — deterministic dictionary-driven matching.

Matches ESCO Silver skill labels against Adzuna Silver job text
(title_normalized, description_normalized) using exact phrase matching
with word-boundary safety.

Grain of output: one row per (job_id, country, ingestion_date,
esco_skill_concept_uri, match_method).
"""

from __future__ import annotations

import logging
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import schema as gold_schema
from .models import MATCH_METHOD_SKILL, SKILL_MATCH_SCORES

logger = logging.getLogger(__name__)


# ── Label normalization ───────────────────────────────────────────────────


def normalize_label(text: str | None) -> str:
    """Normalize a label for matching: lowercase, collapse whitespace, strip.

    Consistent with Silver text normalization conventions.
    """
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())


# ── Skill label dictionary builder ────────────────────────────────────────


def build_skill_label_dictionary(
    skills_df: DataFrame,
) -> DataFrame:
    """Build a unified skill-label dictionary from ESCO Silver skills.

    Explodes preferred_label, alt_labels, and hidden_labels into one row
    per (concept_uri, label_value, label_type) with normalized labels.

    Parameters
    ----------
    skills_df:
        ESCO Silver skills DataFrame with columns:
        concept_uri, concept_uri_uuid, preferred_label, skill_type,
        reuse_level, alt_labels (array), hidden_labels (array).

    Returns
    -------
    DataFrame
        Columns: concept_uri, concept_uri_uuid, preferred_label,
        skill_type, reuse_level, label_value, label_normalized,
        label_type.
    """
    base_cols = [
        F.col("concept_uri"),
        F.col("concept_uri_uuid"),
        F.col("preferred_label"),
        F.col("skill_type"),
        F.col("reuse_level"),
    ]

    # Preferred labels
    preferred = skills_df.select(
        *base_cols,
        F.col("preferred_label").alias("label_value"),
        F.lit("preferred").alias("label_type"),
    )

    # Alt labels (explode array)
    alt = skills_df.select(
        *base_cols,
        F.explode_outer(F.col("alt_labels")).alias("label_value"),
        F.lit("alt").alias("label_type"),
    ).where(F.col("label_value").isNotNull() & (F.trim(F.col("label_value")) != F.lit("")))

    # Hidden labels (explode array)
    hidden = skills_df.select(
        *base_cols,
        F.explode_outer(F.col("hidden_labels")).alias("label_value"),
        F.lit("hidden").alias("label_type"),
    ).where(F.col("label_value").isNotNull() & (F.trim(F.col("label_value")) != F.lit("")))

    # Union all label rows
    all_labels = preferred.unionByName(alt).unionByName(hidden)

    # Add normalized label column
    all_labels = all_labels.withColumn(
        "label_normalized",
        F.lower(F.regexp_replace(F.trim(F.col("label_value")), r"\s+", " ")),
    )

    # Filter out empty normalized labels
    all_labels = all_labels.where(
        F.col("label_normalized").isNotNull() & (F.col("label_normalized") != F.lit(""))
    )

    # Deduplicate by (concept_uri, label_normalized, label_type)
    all_labels = all_labels.dropDuplicates(["concept_uri", "label_normalized", "label_type"])

    return all_labels


# ── Phrase-safe matching ──────────────────────────────────────────────────


def _escape_regex(label: str) -> str:
    """Escape a label for safe use in a regex pattern."""
    return re.escape(label)


def build_match_regex_pattern(labels: list[str]) -> str | None:
    """Build a single regex pattern matching any of the given labels.

    Uses word-boundary anchors (``\\b``) to ensure phrase-aware matching
    and avoid naive substring false positives.

    Returns None if no valid labels provided.
    """
    escaped = [_escape_regex(lbl) for lbl in labels if lbl.strip()]
    if not escaped:
        return None
    # Sort by length descending so longer phrases match first
    escaped.sort(key=len, reverse=True)
    return r"\b(?:" + "|".join(escaped) + r")\b"


# ── Spark matching logic ─────────────────────────────────────────────────


def match_jobs_to_skills(
    jobs_df: DataFrame,
    skill_labels_df: DataFrame,
    spark: SparkSession,  # noqa: ARG001
) -> DataFrame:
    """Match Adzuna Silver jobs to ESCO skills via exact phrase matching.

    Strategy:
    1. Collect skill labels (broadcast-safe for typical ESCO sizes).
    2. For each label, check if it appears in title_normalized or
       description_normalized using word-boundary-safe regex.
    3. Produce one row per (job_id, concept_uri, match_method) with
       strongest match score per text source.

    Parameters
    ----------
    jobs_df:
        Adzuna Silver jobs with at minimum: job_id, adref, country,
        ingestion_date, posted_date, title_normalized,
        description_normalized, silver_run_id.
    skill_labels_df:
        Output of :func:`build_skill_label_dictionary`.
    spark:
        Active SparkSession.

    Returns
    -------
    DataFrame
        Raw match rows before deduplication. Columns include:
        job_id, country, ingestion_date, esco_skill_concept_uri,
        matched_label, matched_label_type, matched_text_source,
        match_method, match_score, title_hit, description_hit.
    """
    logger.info("Starting skill matching via broadcast join + regex")

    # Use a cross-join approach with broadcast of the label dictionary,
    # then filter matches. This is appropriate for ESCO-sized dictionaries
    # (thousands of labels) against daily job partitions (thousands of jobs).

    # Prepare job columns
    jobs = jobs_df.select(
        F.col("job_id"),
        F.col("adref"),
        F.col("country"),
        F.col("ingestion_date"),
        F.col("posted_date"),
        F.col("title_normalized"),
        F.col("description_normalized"),
        F.col("silver_run_id"),
        F.col("source_system"),
    )

    # Prepare label columns with alias prefix to avoid ambiguity.
    # Column names are governed by gold.schema (single source of truth).
    labels = skill_labels_df.select(
        F.col("concept_uri").alias(gold_schema.esco_skill("concept_uri")),
        F.col("concept_uri_uuid").alias(gold_schema.esco_skill("concept_uri_uuid")),
        F.col("preferred_label").alias(gold_schema.esco_skill("preferred_label")),
        F.col("skill_type").alias(gold_schema.esco_skill("type")),
        F.col("reuse_level").alias(gold_schema.esco_skill("reuse_level")),
        F.col("label_value").alias(gold_schema.matched("label")),
        F.col("label_normalized"),
        F.col("label_type").alias(gold_schema.matched("label_type")),
    )

    # Build regex pattern column for word-boundary matching
    # Pattern: \b<label_normalized>\b
    labels = labels.withColumn(
        "_label_pattern",
        F.concat(
            F.lit(r"\b"),
            F.regexp_replace("label_normalized", r"([\.\+\*\?\^\$\{\}\(\)\|\[\]\\])", r"\\$1"),
            F.lit(r"\b"),
        ),
    )

    # Broadcast the label dictionary (ESCO labels are small)
    labels_broadcast = F.broadcast(labels)

    # Cross join and filter matches.
    # NOTE: Column.rlike() only accepts a string literal in PySpark 3.x.
    # For column-to-column regex matching we use F.expr() which compiles
    # to Spark SQL's native RLIKE operator (two-Column form).
    # Title match
    title_matches = (
        jobs.crossJoin(labels_broadcast)
        .where(F.expr("title_normalized RLIKE _label_pattern"))
        .withColumn("matched_text_source", F.lit("title"))
        .withColumn("title_hit", F.lit(True))
        .withColumn("description_hit", F.lit(False))
    )

    # Description match
    desc_matches = (
        jobs.crossJoin(labels_broadcast)
        .where(F.expr("description_normalized RLIKE _label_pattern"))
        .withColumn("matched_text_source", F.lit("description"))
        .withColumn("title_hit", F.lit(False))
        .withColumn("description_hit", F.lit(True))
    )

    # Union title and description matches
    all_matches = title_matches.unionByName(desc_matches)

    # Add match method
    all_matches = all_matches.withColumn("match_method", F.lit(MATCH_METHOD_SKILL))

    # Compute match score from label_type and text_source
    score_expr = _build_score_expression()
    all_matches = all_matches.withColumn("match_score", score_expr)

    # Select final columns (drop internal helper columns).
    # Column names governed by gold.schema.
    result = all_matches.select(
        "source_system",
        "country",
        "ingestion_date",
        "job_id",
        "adref",
        "posted_date",
        gold_schema.esco_skill("concept_uri"),
        gold_schema.esco_skill("concept_uri_uuid"),
        gold_schema.esco_skill("preferred_label"),
        gold_schema.esco_skill("type"),
        gold_schema.esco_skill("reuse_level"),
        gold_schema.matched("label"),
        gold_schema.matched("label_type"),
        gold_schema.matched("text_source"),
        "match_method",
        "match_score",
        "title_hit",
        "description_hit",
        "silver_run_id",
    )

    return result


def _build_score_expression() -> F.Column:
    """Build a Spark Column expression for deterministic match scoring."""
    _type = F.col(gold_schema.matched("label_type"))
    _src = F.col(gold_schema.matched("text_source"))
    return (
        F.when(
            (_type == "preferred") & (_src == "title"),
            F.lit(SKILL_MATCH_SCORES[("preferred", "title")]),
        )
        .when(
            (_type == "alt") & (_src == "title"),
            F.lit(SKILL_MATCH_SCORES[("alt", "title")]),
        )
        .when(
            (_type == "hidden") & (_src == "title"),
            F.lit(SKILL_MATCH_SCORES[("hidden", "title")]),
        )
        .when(
            (_type == "preferred") & (_src == "description"),
            F.lit(SKILL_MATCH_SCORES[("preferred", "description")]),
        )
        .when(
            (_type == "alt") & (_src == "description"),
            F.lit(SKILL_MATCH_SCORES[("alt", "description")]),
        )
        .when(
            (_type == "hidden") & (_src == "description"),
            F.lit(SKILL_MATCH_SCORES[("hidden", "description")]),
        )
        .otherwise(F.lit(0.0))
    )


def deduplicate_skill_matches(matches_df: DataFrame) -> DataFrame:
    """Deduplicate skill matches per (job_id, country, ingestion_date, concept_uri, method).

    For each unique business key, keep the row with the highest match_score.
    Ties are broken by label_type priority (preferred > alt > hidden) and
    text_source priority (title > description).
    """
    from pyspark.sql import Window

    key_cols = [
        "job_id",
        "country",
        "ingestion_date",
        gold_schema.esco_skill("concept_uri"),
        "match_method",
    ]

    window = Window.partitionBy(*key_cols).orderBy(
        F.col("match_score").desc(),
        # Tie-breaker: prefer title over description
        F.when(F.col(gold_schema.matched("text_source")) == "title", F.lit(0))
        .otherwise(F.lit(1))
        .asc(),
        # Tie-breaker: prefer preferred > alt > hidden
        F.when(F.col(gold_schema.matched("label_type")) == "preferred", F.lit(0))
        .when(F.col(gold_schema.matched("label_type")) == "alt", F.lit(1))
        .otherwise(F.lit(2))
        .asc(),
    )

    deduped = (
        matches_df.withColumn("__rn__", F.row_number().over(window))
        .where(F.col("__rn__") == 1)
        .drop("__rn__")
    )

    # Merge title_hit and description_hit across all rows before dedup
    # We need to know if a skill was hit in BOTH title and description
    hit_agg = matches_df.groupBy(*key_cols).agg(
        F.max(F.col("title_hit")).alias("_any_title_hit"),
        F.max(F.col("description_hit")).alias("_any_desc_hit"),
    )

    result = deduped.join(hit_agg, on=key_cols, how="left")
    result = (
        result.withColumn("title_hit", F.col("_any_title_hit"))
        .withColumn("description_hit", F.col("_any_desc_hit"))
        .drop("_any_title_hit", "_any_desc_hit")
    )

    return result
