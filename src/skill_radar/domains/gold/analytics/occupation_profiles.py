"""Gold analytics — occupation profile daily.

Builds a canonical occupation card per (ingestion_date, country, occupation)
from existing Gold tables and ESCO Silver dimensions.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import schema as gold_schema

if TYPE_CHECKING:
    from skill_radar.config.models import CareerNavigationConfig

logger = logging.getLogger(__name__)


def _array_to_search_text(
    preferred_label: str, alt_labels_col: str, hidden_labels_col: str
) -> F.Column:
    """Build a normalized search-text column from label arrays."""
    return F.lower(
        F.concat_ws(
            " ",
            F.col(preferred_label),
            F.coalesce(F.concat_ws(" ", F.col(alt_labels_col)), F.lit("")),
            F.coalesce(F.concat_ws(" ", F.col(hidden_labels_col)), F.lit("")),
        )
    )


def compute_occupation_profile_daily(
    occ_matches_df: DataFrame,
    skill_matches_df: DataFrame,
    salary_df: DataFrame,
    occupations_df: DataFrame,
    relations_df: DataFrame,
    jobs_df: DataFrame,
    *,
    ingestion_date: str,
    country: str,
    config: CareerNavigationConfig,
) -> DataFrame:
    """Compute the daily occupation profile dataset.

    Parameters
    ----------
    occ_matches_df:
        Gold job-occupation matches for the target partition.
    skill_matches_df:
        Gold job-skill matches for the target partition.
    salary_df:
        Gold salary-by-skill daily for the target partition.
    occupations_df:
        ESCO Silver occupations dimension.
    relations_df:
        ESCO Silver occupation-skill relations dimension.
    jobs_df:
        Adzuna Silver jobs for the target partition.
    ingestion_date:
        Target partition date.
    country:
        Target partition country.
    config:
        Career navigation configuration.

    Returns
    -------
    DataFrame
        One row per occupation.
    """
    top_skills = config.top_skills
    top_companies = config.top_companies

    occ_uri_col = gold_schema.esco_occupation("concept_uri")
    occ_label_col = gold_schema.esco_occupation("preferred_label")
    skill_uri_col = gold_schema.esco_skill("concept_uri")
    skill_label_col = gold_schema.esco_skill("preferred_label")

    # ── Step 1: Occupation demand metrics ─────────────────────────────
    # Join occ_matches → jobs to count distinct companies/locations
    occ_jobs = occ_matches_df.select(
        F.col(occ_uri_col),
        F.col("job_id"),
    ).join(
        jobs_df.select(
            F.col("job_id").alias("_job_id"),
            F.col("company_name").alias("_company"),
            F.col("location_display_name").alias("_location"),
        ),
        F.col("job_id") == F.col("_job_id"),
        "left",
    )

    occ_demand = occ_jobs.groupBy(occ_uri_col).agg(
        F.countDistinct("job_id").alias("matched_jobs_count"),
        F.countDistinct("_company").alias("distinct_companies_count"),
        F.countDistinct("_location").alias("distinct_locations_count"),
    )

    # ── Step 2: Top companies per occupation ──────────────────────────
    occ_company_counts = (
        occ_jobs.where(F.col("_company").isNotNull())
        .groupBy(occ_uri_col, "_company")
        .agg(F.count("*").alias("_co_count"))
    )

    co_window = Window.partitionBy(occ_uri_col).orderBy(F.col("_co_count").desc())
    top_co = (
        occ_company_counts.withColumn("_rn", F.row_number().over(co_window))
        .where(F.col("_rn") <= top_companies)
        .groupBy(occ_uri_col)
        .agg(
            F.to_json(F.collect_list(F.struct("_company", "_co_count"))).alias("top_companies_json")
        )
    )

    # ── Step 3: Salary context via skill matches ─────────────────────
    # Average salary across skills matched to jobs of this occupation
    occ_skill_jobs = occ_matches_df.select(
        F.col(occ_uri_col),
        F.col("job_id").alias("_osj_job"),
    ).join(
        skill_matches_df.select(
            F.col("job_id").alias("_sm_job"),
            F.col(skill_uri_col).alias("_sm_skill"),
        ),
        F.col("_osj_job") == F.col("_sm_job"),
        "inner",
    )

    occ_salary = (
        occ_skill_jobs.select(F.col(occ_uri_col), F.col("_sm_skill"))
        .distinct()
        .join(
            salary_df.select(
                F.col(skill_uri_col).alias("_sal_skill"),
                F.col("avg_salary_mean").alias("_sal_mean"),
            ),
            F.col("_sm_skill") == F.col("_sal_skill"),
            "inner",
        )
        .groupBy(occ_uri_col)
        .agg(F.avg("_sal_mean").alias("avg_salary_mean"))
    )

    # ── Step 4: Top essential/optional skills from ESCO relations ────
    # Filter relations to essential/optional, rank by matched_jobs_count
    occ_skill_evidence = (
        occ_matches_df.select(
            F.col(occ_uri_col).alias("_ev_occ"),
            F.col("job_id").alias("_ev_job"),
        )
        .join(
            skill_matches_df.select(
                F.col("job_id").alias("_sm2_job"),
                F.col(skill_uri_col).alias("_sm2_skill"),
                F.col(skill_label_col).alias("_sm2_label"),
            ),
            F.col("_ev_job") == F.col("_sm2_job"),
            "inner",
        )
        .groupBy("_ev_occ", "_sm2_skill", "_sm2_label")
        .agg(
            F.countDistinct("_ev_job").alias("_skill_jobs"),
        )
    )

    rel_typed = relations_df.select(
        F.col("occupation_uri").alias("_rel_occ"),
        F.col("skill_uri").alias("_rel_skill"),
        F.col("relation_type").alias("_rel_type"),
    )

    skill_ranked = occ_skill_evidence.join(
        rel_typed,
        (F.col("_ev_occ") == F.col("_rel_occ")) & (F.col("_sm2_skill") == F.col("_rel_skill")),
        "left",
    ).withColumn(
        "_rel_type_clean",
        F.coalesce(F.col("_rel_type"), F.lit("unknown")),
    )

    for rel_type, col_name in [
        ("essential", "top_essential_skills_json"),
        ("optional", "top_optional_skills_json"),
    ]:
        w = Window.partitionBy("_ev_occ").orderBy(F.col("_skill_jobs").desc())
        filtered = skill_ranked.where(F.col("_rel_type_clean") == rel_type)
        ranked = (
            filtered.withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") <= top_skills)
            .groupBy("_ev_occ")
            .agg(
                F.to_json(
                    F.collect_list(
                        F.struct(
                            F.col("_sm2_skill").alias("skill_uri"),
                            F.col("_sm2_label").alias("skill_label"),
                            F.col("_skill_jobs").alias("jobs_count"),
                        )
                    )
                ).alias(col_name)
            )
        )
        if rel_type == "essential":
            essential_skills = ranked
        else:
            optional_skills = ranked

    # ── Step 5: Related skills count from ESCO relations ────────────
    related_count = (
        relations_df.select(
            F.col("occupation_uri").alias("_rc_occ"),
            F.col("skill_uri").alias("_rc_skill"),
        )
        .groupBy("_rc_occ")
        .agg(F.countDistinct("_rc_skill").alias("related_skills_count"))
    )

    # ── Step 6: Occupation labels from Silver ──────────────────────
    occ_labels = occupations_df.select(
        F.col("concept_uri").alias("_ol_uri"),
        F.col("concept_uri_uuid").alias("_ol_uuid"),
        F.col("preferred_label").alias("_ol_label"),
        F.col("alt_labels").alias("_ol_alt"),
        F.col("hidden_labels").alias("_ol_hidden"),
    )

    # ── Step 7: Assemble ──────────────────────────────────────────────
    profile = (
        occ_demand.join(occ_salary, occ_uri_col, "left")
        .join(top_co, occ_uri_col, "left")
        .join(essential_skills, F.col(occ_uri_col) == F.col("_ev_occ"), "left")
        .drop("_ev_occ")
        .join(optional_skills, F.col(occ_uri_col) == optional_skills["_ev_occ"], "left")
        .drop("_ev_occ")
        .join(related_count, F.col(occ_uri_col) == F.col("_rc_occ"), "left")
        .drop("_rc_occ")
        .join(occ_labels, F.col(occ_uri_col) == F.col("_ol_uri"), "left")
    )

    # Build searchable text
    profile = profile.withColumn(
        "occupation_search_text",
        F.lower(
            F.concat_ws(
                " ",
                F.col("_ol_label"),
                F.coalesce(F.concat_ws(" ", F.col("_ol_alt")), F.lit("")),
                F.coalesce(F.concat_ws(" ", F.col("_ol_hidden")), F.lit("")),
            )
        ),
    )

    profile = (
        profile.withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("country", F.lit(country))
        .withColumn(gold_schema.esco_occupation("concept_uri_uuid"), F.col("_ol_uuid"))
        .withColumn(occ_label_col, F.col("_ol_label"))
        .na.fill(
            0,
            subset=[
                "matched_jobs_count",
                "distinct_companies_count",
                "distinct_locations_count",
                "related_skills_count",
            ],
        )
        .na.fill(
            "[]",
            subset=["top_essential_skills_json", "top_optional_skills_json", "top_companies_json"],
        )
    )

    return profile.select(
        "ingestion_date",
        "country",
        occ_uri_col,
        gold_schema.esco_occupation("concept_uri_uuid"),
        occ_label_col,
        "occupation_search_text",
        "matched_jobs_count",
        "distinct_companies_count",
        "distinct_locations_count",
        "avg_salary_mean",
        "top_essential_skills_json",
        "top_optional_skills_json",
        "top_companies_json",
        "related_skills_count",
    )
