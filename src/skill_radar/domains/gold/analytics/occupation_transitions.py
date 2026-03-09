"""Gold analytics — occupation transition daily.

Computes transition guidance between occupation pairs: shared skills,
missing skills, difficulty score, and market context (salary/jobs delta).

Algorithm
---------
1. Build per-occupation skill sets from ESCO Silver relations (essential
   and optional separately).
2. For each (from, to) pair, compute:
   - shared skills = intersection
   - missing skills = target - source
   - missing essential = essential(target) - all(source)
   - missing optional = optional(target) - all(source)
   - difficulty = w_essential * |missing_essential| + w_optional * |missing_optional|
3. Enrich with salary/jobs context from occupation profiles.

Inputs
------
- ESCO Silver relations (dimension)
- ESCO Silver occupations (dimension, for labels)
- ESCO Silver skills (dimension, for labels in JSON)
- Gold occupation_profile_daily (for salary/jobs context)

Output
------
One row per (from_occupation, to_occupation) pair with
transition analysis and market context.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import schema as gold_schema

if TYPE_CHECKING:
    from skill_radar.config.models import CareerNavigationConfig

logger = logging.getLogger(__name__)


def compute_occupation_transition_daily(
    relations_df: DataFrame,
    occupations_df: DataFrame,
    skills_df: DataFrame,
    similarity_df: DataFrame,
    occ_profile_df: DataFrame | None,
    *,
    ingestion_date: str,
    country: str,
    config: CareerNavigationConfig,
) -> DataFrame:
    """Compute transition analysis for occupation pairs.

    Only computes transitions for pairs that already appear in the
    similarity dataset (top-N nearest occupations), keeping the output
    size manageable.

    Parameters
    ----------
    relations_df:
        ESCO Silver relations dimension.
    occupations_df:
        ESCO Silver occupations dimension (for labels).
    skills_df:
        ESCO Silver skills dimension (for skill labels in JSON fields).
    similarity_df:
        Gold occupation-similarity daily (provides the pair set).
    occ_profile_df:
        Gold occupation-profile daily (for salary/jobs context).
        Can be None if profiles are unavailable.
    ingestion_date:
        Target partition date.
    country:
        Target partition country.
    config:
        Career navigation configuration.

    Returns
    -------
    DataFrame
        One row per transition pair with gap analysis and market context.
    """
    trans_cfg = config.transition
    w_e = trans_cfg.w_essential
    w_o = trans_cfg.w_optional

    # ── Step 1: Build occupation skill sets ───────────────────────────
    # All skills per occupation (essential + optional together, and split)
    ess_rel = relations_df.where(F.col("relation_type") == "essential")
    opt_rel = relations_df.where(F.col("relation_type") == "optional")

    ess_sets = ess_rel.groupBy("occupation_uri").agg(
        F.collect_set("skill_uri").alias("ess_skills"),
    )
    opt_sets = opt_rel.groupBy("occupation_uri").agg(
        F.collect_set("skill_uri").alias("opt_skills"),
    )
    all_sets = relations_df.groupBy("occupation_uri").agg(
        F.collect_set("skill_uri").alias("all_skills"),
    )

    empty_arr = F.array().cast("array<string>")

    occ_skills = (
        all_sets.join(ess_sets, "occupation_uri", "left")
        .join(opt_sets, "occupation_uri", "left")
        .withColumn("ess_skills", F.coalesce(F.col("ess_skills"), empty_arr))
        .withColumn("opt_skills", F.coalesce(F.col("opt_skills"), empty_arr))
    )

    # ── Step 2: Restrict to pairs from similarity dataset ────────────
    pairs = similarity_df.select(
        F.col("source_occupation_uri").alias("from_occupation_uri"),
        F.col("target_occupation_uri").alias("to_occupation_uri"),
        F.col("similarity_score"),
    )

    # ── Step 3: Attach skill sets for both sides ─────────────────────
    from_skills = occ_skills.select(
        F.col("occupation_uri").alias("_from_uri"),
        F.col("all_skills").alias("from_all"),
        F.col("ess_skills").alias("from_ess"),
        F.col("opt_skills").alias("from_opt"),
    )
    to_skills = occ_skills.select(
        F.col("occupation_uri").alias("_to_uri"),
        F.col("all_skills").alias("to_all"),
        F.col("ess_skills").alias("to_ess"),
        F.col("opt_skills").alias("to_opt"),
    )

    enriched = pairs.join(
        from_skills, F.col("from_occupation_uri") == F.col("_from_uri"), "left"
    ).join(to_skills, F.col("to_occupation_uri") == F.col("_to_uri"), "left")

    # Fill nulls in case of missing join
    for col_name in ["from_all", "from_ess", "from_opt", "to_all", "to_ess", "to_opt"]:
        enriched = enriched.withColumn(col_name, F.coalesce(F.col(col_name), empty_arr))

    # ── Step 4: Compute shared and missing skill sets ────────────────
    enriched = (
        enriched.withColumn("shared_arr", F.array_intersect("from_all", "to_all"))
        .withColumn("missing_arr", F.array_except("to_all", "from_all"))
        .withColumn("missing_ess_arr", F.array_except("to_ess", "from_all"))
        .withColumn("missing_opt_arr", F.array_except("to_opt", "from_all"))
        .withColumn("missing_skill_count", F.size("missing_arr"))
        .withColumn("missing_essential_skill_count", F.size("missing_ess_arr"))
    )

    # ── Step 5: Transition difficulty ────────────────────────────────
    enriched = enriched.withColumn(
        "transition_difficulty_score",
        (F.size("missing_ess_arr").cast("double") * F.lit(w_e))
        + (F.size("missing_opt_arr").cast("double") * F.lit(w_o)),
    )

    # ── Step 6: Convert skill URIs to labelled JSON arrays ───────────
    # Build a skill_uri → label lookup
    skill_lookup = skills_df.select(
        F.col("concept_uri").alias("_sk_uri"),
        F.col("preferred_label").alias("_sk_label"),
    )

    # For JSON fields we use to_json(collect_list(struct(...)))
    # We explode each array, join with labels, and re-aggregate
    for arr_col, json_col in [
        ("shared_arr", "shared_skills_json"),
        ("missing_arr", "missing_skills_json"),
        ("missing_ess_arr", "missing_essential_skills_json"),
        ("missing_opt_arr", "missing_optional_skills_json"),
    ]:
        exploded = (
            enriched.select(
                "from_occupation_uri",
                "to_occupation_uri",
                F.explode_outer(arr_col).alias("_exp_uri"),
            )
            .join(skill_lookup, F.col("_exp_uri") == F.col("_sk_uri"), "left")
            .groupBy("from_occupation_uri", "to_occupation_uri")
            .agg(
                F.to_json(
                    F.collect_list(
                        F.struct(
                            F.col("_exp_uri").alias("skill_uri"),
                            F.coalesce(F.col("_sk_label"), F.col("_exp_uri")).alias("skill_label"),
                        )
                    )
                ).alias(json_col)
            )
        )
        enriched = enriched.join(
            exploded,
            ["from_occupation_uri", "to_occupation_uri"],
            "left",
        )

    # Fill empty JSON
    for jcol in [
        "shared_skills_json",
        "missing_skills_json",
        "missing_essential_skills_json",
        "missing_optional_skills_json",
    ]:
        enriched = enriched.withColumn(jcol, F.coalesce(F.col(jcol), F.lit("[]")))

    # ── Step 7: Labels from Silver ───────────────────────────────────
    from_labels = occupations_df.select(
        F.col("concept_uri").alias("_fl_uri"),
        F.col("preferred_label").alias("from_occupation_label"),
    )
    to_labels = occupations_df.select(
        F.col("concept_uri").alias("_tl_uri"),
        F.col("preferred_label").alias("to_occupation_label"),
    )

    enriched = enriched.join(
        from_labels, F.col("from_occupation_uri") == F.col("_fl_uri"), "left"
    ).join(to_labels, F.col("to_occupation_uri") == F.col("_tl_uri"), "left")

    # ── Step 8: Market context from occupation profiles ──────────────
    if occ_profile_df is not None:
        occ_uri_col = gold_schema.esco_occupation("concept_uri")
        from_ctx = occ_profile_df.select(
            F.col(occ_uri_col).alias("_fc_uri"),
            F.col("avg_salary_mean").alias("from_avg_salary_mean"),
            F.col("matched_jobs_count").alias("from_jobs_count"),
        )
        to_ctx = occ_profile_df.select(
            F.col(occ_uri_col).alias("_tc_uri"),
            F.col("avg_salary_mean").alias("to_avg_salary_mean"),
            F.col("matched_jobs_count").alias("to_jobs_count"),
        )
        enriched = enriched.join(
            from_ctx, F.col("from_occupation_uri") == F.col("_fc_uri"), "left"
        ).join(to_ctx, F.col("to_occupation_uri") == F.col("_tc_uri"), "left")
    else:
        enriched = (
            enriched.withColumn("from_avg_salary_mean", F.lit(None).cast("double"))
            .withColumn("to_avg_salary_mean", F.lit(None).cast("double"))
            .withColumn("from_jobs_count", F.lit(None).cast("long"))
            .withColumn("to_jobs_count", F.lit(None).cast("long"))
        )

    enriched = (
        enriched.withColumn(
            "salary_delta_mean", F.col("to_avg_salary_mean") - F.col("from_avg_salary_mean")
        )
        .withColumn("jobs_delta", F.col("to_jobs_count") - F.col("from_jobs_count"))
        .withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("country", F.lit(country))
    )

    return enriched.select(
        "ingestion_date",
        "country",
        "from_occupation_uri",
        "from_occupation_label",
        "to_occupation_uri",
        "to_occupation_label",
        "similarity_score",
        "shared_skills_json",
        "missing_skills_json",
        "missing_essential_skills_json",
        "missing_optional_skills_json",
        "missing_skill_count",
        "missing_essential_skill_count",
        "transition_difficulty_score",
        "from_avg_salary_mean",
        "to_avg_salary_mean",
        "salary_delta_mean",
        "from_jobs_count",
        "to_jobs_count",
        "jobs_delta",
    )
