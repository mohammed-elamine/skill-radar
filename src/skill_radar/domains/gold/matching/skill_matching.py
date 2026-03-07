"""Gold skill matching — candidate-based deterministic matching.

Primary matching engine
-----------------------
Matches ESCO skill labels against Adzuna job text by equi-joining
**bounded n-gram candidate phrases** (from job text) to the **ESCO label
dimension** (from Silver skills) on ``candidate_phrase == label_normalized``.

This replaces the previous broadcast cross-join + regex strategy as the
default execution path.  The equi-join approach:

- Leverages Spark's hash-join optimizer instead of O(jobs x labels) regex
- Inherits natural word-boundary semantics from whitespace-tokenized n-grams
- Keeps scoring, deduplication, and output schema identical to the prior version

Regex fallback
--------------
The legacy regex helpers (:func:`build_match_regex_pattern`,
:func:`normalize_label`) are retained for narrow fallback use.  They are
**not** invoked by the default matching path and can be enabled via
:func:`match_jobs_to_skills_regex` for diagnostics or edge-case coverage.

Grain of output
- one row per (job_id, country, ingestion_date,
  esco_skill_concept_uri, match_method).
"""

from __future__ import annotations

import logging
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .. import schema as gold_schema
from .models import MATCH_METHOD_SKILL
from .scoring import build_skill_score_expr

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Legacy helpers (kept for backward-compat tests + optional regex fallback)
# ═══════════════════════════════════════════════════════════════════════════


def normalize_label(text: str | None) -> str:
    """Normalize a label for matching: lowercase, collapse whitespace, strip.

    Consistent with Silver text normalization conventions.
    """
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())


def build_skill_label_dictionary(skills_df: DataFrame) -> DataFrame:
    """Build a unified skill-label dictionary from ESCO Silver skills.

    .. deprecated::
        Prefer :func:`label_dimension.build_label_dimension` which also
        emits ``label_token_count`` for n-gram capping.  This thin wrapper
        delegates to it for backward compatibility.
    """
    from .label_dimension import build_label_dimension

    return build_label_dimension(skills_df)


# ═══════════════════════════════════════════════════════════════════════════
# Regex fallback (isolated, NOT default path)
# ═══════════════════════════════════════════════════════════════════════════


def _escape_regex(label: str) -> str:
    """Escape a label for safe use in a regex pattern."""
    return re.escape(label)


def build_match_regex_pattern(labels: list[str]) -> str | None:
    """Build a single regex pattern matching any of the given labels.

    Uses word-boundary anchors (``\\b``) to ensure phrase-aware matching
    and avoid naive substring false positives.

    Returns None if no valid labels provided.

    .. note::
        This function is **not** used by the default candidate-based
        matching path.  It is retained for diagnostics and narrow
        regex-fallback scenarios.
    """
    escaped = [_escape_regex(lbl) for lbl in labels if lbl.strip()]
    if not escaped:
        return None
    escaped.sort(key=len, reverse=True)
    return r"\b(?:" + "|".join(escaped) + r")\b"


# ═══════════════════════════════════════════════════════════════════════════
# Primary matching engine — candidate equi-join
# ═══════════════════════════════════════════════════════════════════════════


def match_jobs_to_skills(
    jobs_df: DataFrame,
    label_dim_df: DataFrame,
    job_candidates_df: DataFrame,
) -> DataFrame:
    """Match jobs to ESCO skills via candidate-phrase equi-join.

    Strategy
    --------
    1. Join ``job_candidates_df`` to ``label_dim_df`` on
       ``candidate_phrase == label_normalized`` (broadcast hash-join).
    2. Carry ``text_source`` from candidates and ``label_type`` from
       the dimension to assign deterministic scores.
    3. Enrich with job identity columns from ``jobs_df``.

    Parameters
    ----------
    jobs_df:
        Adzuna Silver jobs with identity + text columns.
    label_dim_df:
        ESCO label dimension (output of
        :func:`label_dimension.build_label_dimension`).
    job_candidates_df:
        Bounded n-gram candidates (output of
        :func:`job_candidates.build_job_candidates`).

    Returns
    -------
    DataFrame
        Raw match rows (before deduplication).
    """
    logger.info("Starting skill matching via candidate equi-join")

    # ── Prepare label side (broadcast) ────────────────────────────────────
    labels = F.broadcast(
        label_dim_df.select(
            F.col("concept_uri").alias(gold_schema.esco_skill("concept_uri")),
            F.col("concept_uri_uuid").alias(gold_schema.esco_skill("concept_uri_uuid")),
            F.col("preferred_label").alias(gold_schema.esco_skill("preferred_label")),
            F.col("skill_type").alias(gold_schema.esco_skill("type")),
            F.col("reuse_level").alias(gold_schema.esco_skill("reuse_level")),
            F.col("label_value").alias(gold_schema.matched("label")),
            F.col("label_normalized"),
            F.col("label_type").alias(gold_schema.matched("label_type")),
        )
    )

    # ── Equi-join: candidates → labels ────────────────────────────────────
    matched = job_candidates_df.join(
        labels,
        job_candidates_df["candidate_phrase"] == labels["label_normalized"],
        how="inner",
    )

    # Rename text_source to gold schema convention
    matched = matched.withColumn(gold_schema.matched("text_source"), F.col("text_source"))

    # ── Add match method + score ──────────────────────────────────────────
    matched = matched.withColumn("match_method", F.lit(MATCH_METHOD_SKILL))
    matched = matched.withColumn(
        "match_score",
        build_skill_score_expr(
            gold_schema.matched("label_type"),
            gold_schema.matched("text_source"),
        ),
    )

    # ── Compute title_hit / description_hit flags ─────────────────────────
    matched = matched.withColumn(
        "title_hit",
        F.col(gold_schema.matched("text_source")) == F.lit("title"),
    ).withColumn(
        "description_hit",
        F.col(gold_schema.matched("text_source")) == F.lit("description"),
    )

    # ── Enrich with job identity columns ──────────────────────────────────
    job_identity = jobs_df.select(
        F.col("job_id").alias("_ji_job_id"),
        F.col("country").alias("_ji_country"),
        F.col("ingestion_date").alias("_ji_ing_date"),
        "adref",
        "posted_date",
        "silver_run_id",
        "source_system",
    )

    result = matched.join(
        job_identity,
        on=[
            matched["job_id"] == job_identity["_ji_job_id"],
            matched["country"] == job_identity["_ji_country"],
            matched["ingestion_date"] == job_identity["_ji_ing_date"],
        ],
        how="inner",
    ).select(
        F.col("source_system"),
        matched["country"],
        matched["ingestion_date"],
        matched["job_id"],
        F.col("adref"),
        F.col("posted_date"),
        F.col(gold_schema.esco_skill("concept_uri")),
        F.col(gold_schema.esco_skill("concept_uri_uuid")),
        F.col(gold_schema.esco_skill("preferred_label")),
        F.col(gold_schema.esco_skill("type")),
        F.col(gold_schema.esco_skill("reuse_level")),
        F.col(gold_schema.matched("label")),
        F.col(gold_schema.matched("label_type")),
        F.col(gold_schema.matched("text_source")),
        F.col("match_method"),
        F.col("match_score"),
        F.col("title_hit"),
        F.col("description_hit"),
        F.col("silver_run_id"),
    )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Deduplication
# ═══════════════════════════════════════════════════════════════════════════


def deduplicate_skill_matches(matches_df: DataFrame) -> DataFrame:
    """Deduplicate skill matches per (job, skill, method) business key.

    For each unique key, keep the row with the **highest** match_score.
    Ties are broken by label_type priority (preferred > alt > hidden) and
    text_source priority (title > description).

    ``title_hit`` and ``description_hit`` are merged across all source
    rows before dedup, so the winning row carries *both* flags if the
    skill was matched in both text sources.
    """
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

    # Merge title_hit and description_hit across all source rows
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
