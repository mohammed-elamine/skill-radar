"""ESCO skill label dimension — flat lookup table for candidate matching.

Converts ESCO Silver skills into a flat label dimension with one row per
label variant. This is the reference side of the candidate-based matching
architecture:

    job candidates (n-grams) ──equi-join──▶ label dimension

The dimension is broadcast-safe for typical ESCO sizes (~14 000 skills,
~60 000 label variants) and eliminates the need for cross-join + regex
evaluation.

Columns
-------
- concept_uri:         ESCO skill concept URI
- concept_uri_uuid:    UUID derived from the concept URI
- preferred_label:     Canonical skill name
- skill_type:          ESCO skill type (skill / knowledge / …)
- reuse_level:         ESCO reuse level
- label_value:         Original label text
- label_normalized:    Lowercased, whitespace-collapsed label
- label_type:          One of ``preferred``, ``alt``, ``hidden``
- label_token_count:   Number of whitespace-delimited tokens in the
                       normalized label (used by the candidate generator
                       to cap n-gram size)
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_label_dimension(skills_df: DataFrame) -> DataFrame:
    """Build a flat skill-label dimension from ESCO Silver skills.

    Explodes ``preferred_label``, ``alt_labels``, and ``hidden_labels``
    into one row per unique (concept_uri, label_normalized, label_type),
    then adds ``label_token_count`` for downstream n-gram capping.

    Parameters
    ----------
    skills_df:
        ESCO Silver skills with at minimum: ``concept_uri``,
        ``concept_uri_uuid``, ``preferred_label``, ``skill_type``,
        ``reuse_level``, ``alt_labels`` (array), ``hidden_labels``
        (array).

    Returns
    -------
    DataFrame
        One row per unique (concept_uri, label_normalized, label_type).
    """
    base_cols = [
        F.col("concept_uri"),
        F.col("concept_uri_uuid"),
        F.col("preferred_label"),
        F.col("skill_type"),
        F.col("reuse_level"),
    ]

    # ── Preferred labels ──────────────────────────────────────────────────
    preferred = skills_df.select(
        *base_cols,
        F.col("preferred_label").alias("label_value"),
        F.lit("preferred").alias("label_type"),
    )

    # ── Alt labels (explode array) ────────────────────────────────────────
    alt = (
        skills_df.select(
            *base_cols,
            F.explode_outer(F.col("alt_labels")).alias("label_value"),
            F.lit("alt").alias("label_type"),
        )
        .where(F.col("label_value").isNotNull())
        .where(F.trim(F.col("label_value")) != F.lit(""))
    )

    # ── Hidden labels (explode array) ─────────────────────────────────────
    hidden = (
        skills_df.select(
            *base_cols,
            F.explode_outer(F.col("hidden_labels")).alias("label_value"),
            F.lit("hidden").alias("label_type"),
        )
        .where(F.col("label_value").isNotNull())
        .where(F.trim(F.col("label_value")) != F.lit(""))
    )

    # ── Union + normalize ─────────────────────────────────────────────────
    all_labels = preferred.unionByName(alt).unionByName(hidden)

    all_labels = all_labels.withColumn(
        "label_normalized",
        F.lower(F.regexp_replace(F.trim(F.col("label_value")), r"\s+", " ")),
    )

    # Drop empty normalized labels
    all_labels = all_labels.where(
        F.col("label_normalized").isNotNull() & (F.col("label_normalized") != F.lit(""))
    )

    # ── Deduplicate ───────────────────────────────────────────────────────
    all_labels = all_labels.dropDuplicates(["concept_uri", "label_normalized", "label_type"])

    # ── Token count (for n-gram capping) ──────────────────────────────────
    all_labels = all_labels.withColumn(
        "label_token_count",
        F.size(F.split(F.col("label_normalized"), r"\s+")),
    )

    logger.info("Label dimension built — schema ready for broadcast join")

    return all_labels


def max_label_tokens(label_dim_df: DataFrame) -> int:
    """Return the maximum token count across all labels in the dimension.

    Used by the candidate generator to decide the n-gram ceiling.
    Falls back to ``1`` if the dimension is empty.
    """
    row = label_dim_df.agg(F.max("label_token_count").alias("max_tokens")).collect()
    if row and row[0]["max_tokens"] is not None:
        return int(row[0]["max_tokens"])
    return 1
