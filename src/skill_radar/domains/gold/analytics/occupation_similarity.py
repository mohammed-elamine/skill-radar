"""Gold analytics — occupation similarity daily.

Computes pairwise occupation similarity based on ESCO skill-set overlap,
weighted by relation type (essential skills count more than optional).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from skill_radar.config.models import CareerNavigationConfig

logger = logging.getLogger(__name__)


def _count_by_type(
    relations_df: DataFrame,
    relation_type: str,
    alias_prefix: str,
) -> DataFrame:
    """Count distinct skills per occupation for a given relation type."""
    return (
        relations_df.where(F.col("relation_type") == relation_type)
        .groupBy("occupation_uri")
        .agg(
            F.collect_set("skill_uri").alias(f"{alias_prefix}_skills"),
            F.count("skill_uri").alias(f"{alias_prefix}_count"),
        )
    )


def compute_occupation_similarity_daily(
    relations_df: DataFrame,
    occupations_df: DataFrame,
    *,
    ingestion_date: str,
    country: str,
    config: CareerNavigationConfig,
) -> DataFrame:
    """Compute pairwise occupation similarity from ESCO skill overlap.

    Parameters
    ----------
    relations_df:
        ESCO Silver relations dimension.
    occupations_df:
        ESCO Silver occupations dimension (for labels).
    ingestion_date:
        Target partition date.
    country:
        Target partition country.
    config:
        Career navigation configuration (top_n, weights).

    Returns
    -------
    DataFrame
        Top-N similar occupations per source.
    """
    sim_cfg = config.similarity
    top_n = sim_cfg.top_n
    w_e = sim_cfg.w_essential
    w_o = sim_cfg.w_optional

    # ── Step 1: Build occupation skill sets ───────────────────────────
    ess = _count_by_type(relations_df, "essential", "ess")
    opt = _count_by_type(relations_df, "optional", "opt")

    # All occupations from relations with at least one skill
    all_occ = (
        relations_df.select("occupation_uri")
        .distinct()
        .join(ess, "occupation_uri", "left")
        .join(opt, "occupation_uri", "left")
        .na.fill(0, subset=["ess_count", "opt_count"])
    )

    # Replace null arrays with empty arrays
    empty_arr = F.array().cast("array<string>")
    all_occ = (
        all_occ.withColumn("ess_skills", F.coalesce(F.col("ess_skills"), empty_arr))
        .withColumn("opt_skills", F.coalesce(F.col("opt_skills"), empty_arr))
        .withColumn("total_skill_count", F.col("ess_count") + F.col("opt_count"))
    )

    # ── Step 2: Self-cross join ──────────────────────────────────────
    src = all_occ.select(
        F.col("occupation_uri").alias("source_occupation_uri"),
        F.col("ess_skills").alias("src_ess"),
        F.col("opt_skills").alias("src_opt"),
        F.col("ess_count").alias("src_ess_count"),
        F.col("opt_count").alias("src_opt_count"),
        F.col("total_skill_count").alias("source_skill_count"),
    )

    tgt = all_occ.select(
        F.col("occupation_uri").alias("target_occupation_uri"),
        F.col("ess_skills").alias("tgt_ess"),
        F.col("opt_skills").alias("tgt_opt"),
        F.col("ess_count").alias("tgt_ess_count"),
        F.col("opt_count").alias("tgt_opt_count"),
        F.col("total_skill_count").alias("target_skill_count"),
    )

    pairs = src.crossJoin(tgt).where(
        F.col("source_occupation_uri") != F.col("target_occupation_uri")
    )

    # ── Step 3: Compute overlap counts ───────────────────────────────
    pairs = (
        pairs.withColumn("shared_ess", F.size(F.array_intersect("src_ess", "tgt_ess")))
        .withColumn("shared_opt", F.size(F.array_intersect("src_opt", "tgt_opt")))
        .withColumn("union_ess", F.size(F.array_union("src_ess", "tgt_ess")))
        .withColumn("union_opt", F.size(F.array_union("src_opt", "tgt_opt")))
    )

    # ── Step 4: Weighted Jaccard ─────────────────────────────────────
    # Denominator: avoid division by zero
    weighted_num = (F.col("shared_ess") * F.lit(w_e)) + (F.col("shared_opt") * F.lit(w_o))
    weighted_den = (F.col("union_ess") * F.lit(w_e)) + (F.col("union_opt") * F.lit(w_o))

    pairs = pairs.withColumn(
        "similarity_score",
        F.when(weighted_den > 0, weighted_num / weighted_den).otherwise(0.0),
    )

    # ── Step 5: Top-N per source ─────────────────────────────────────
    w = Window.partitionBy("source_occupation_uri").orderBy(F.col("similarity_score").desc())
    top_pairs = (
        pairs.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") <= top_n).drop("_rn")
    )

    # ── Step 6: Add labels from Silver ───────────────────────────────
    src_labels = occupations_df.select(
        F.col("concept_uri").alias("_src_uri"),
        F.col("preferred_label").alias("source_occupation_label"),
    )
    tgt_labels = occupations_df.select(
        F.col("concept_uri").alias("_tgt_uri"),
        F.col("preferred_label").alias("target_occupation_label"),
    )

    result = (
        top_pairs.join(src_labels, F.col("source_occupation_uri") == F.col("_src_uri"), "left")
        .join(tgt_labels, F.col("target_occupation_uri") == F.col("_tgt_uri"), "left")
        .withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("country", F.lit(country))
        .withColumn("shared_skill_count", F.col("shared_ess") + F.col("shared_opt"))
        .withColumn("shared_essential_skill_count", F.col("shared_ess"))
        .withColumn("shared_optional_skill_count", F.col("shared_opt"))
    )

    return result.select(
        "ingestion_date",
        "country",
        "source_occupation_uri",
        "source_occupation_label",
        "target_occupation_uri",
        "target_occupation_label",
        "similarity_score",
        "shared_skill_count",
        "shared_essential_skill_count",
        "shared_optional_skill_count",
        "source_skill_count",
        "target_skill_count",
    )
