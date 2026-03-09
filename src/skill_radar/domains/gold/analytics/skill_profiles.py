"""Gold analytics — skill profile daily.

Builds a canonical skill card per (ingestion_date, country, skill)
from existing Gold tables and ESCO Silver dimensions.

Inputs
------
- Gold skill-demand daily (partition)
- Gold salary-by-skill daily (partition)
- Gold job-skill matches (partition)
- Gold job-occupation matches (partition)
- ESCO Silver skills (dimension)
- Adzuna Silver jobs (partition)

Output
------
One row per skill with:
- searchable text (preferred + alt + hidden labels)
- demand metrics (jobs, companies, locations)
- salary context
- top occupations (JSON)
- top companies (JSON)
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


def compute_skill_profile_daily(
    skill_demand_df: DataFrame,
    salary_df: DataFrame,
    skill_matches_df: DataFrame,
    occ_matches_df: DataFrame,
    skills_df: DataFrame,
    jobs_df: DataFrame,
    *,
    ingestion_date: str,
    country: str,
    config: CareerNavigationConfig,
) -> DataFrame:
    """Compute the daily skill profile dataset.

    Parameters
    ----------
    skill_demand_df:
        Gold skill-demand daily for the target partition.
    salary_df:
        Gold salary-by-skill daily for the target partition.
    skill_matches_df:
        Gold job-skill matches for the target partition.
    occ_matches_df:
        Gold job-occupation matches for the target partition.
    skills_df:
        ESCO Silver skills dimension.
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
        One row per skill.
    """
    top_occupations = config.top_occupations
    top_companies = config.top_companies

    skill_uri_col = gold_schema.esco_skill("concept_uri")
    occ_uri_col = gold_schema.esco_occupation("concept_uri")
    occ_label_col = gold_schema.esco_occupation("preferred_label")

    # ── Step 1: Base demand metrics (already aggregated) ──────────────
    demand_base = skill_demand_df.select(
        F.col(skill_uri_col),
        F.col("jobs_count"),
        F.col("unique_companies_count").alias("companies_count"),
        F.col("unique_locations_count").alias("locations_count"),
    )

    # ── Step 2: Salary context ───────────────────────────────────────
    salary_base = salary_df.select(
        F.col(skill_uri_col).alias("_sal_skill"),
        F.col("avg_salary_mean"),
    )

    # ── Step 3: Top occupations associated with the skill ────────────
    # Join skill_matches → occ_matches via job_id to find co-occurring occupations
    skill_occ = (
        skill_matches_df.select(
            F.col(skill_uri_col).alias("_so_skill"),
            F.col("job_id").alias("_so_job"),
        )
        .join(
            occ_matches_df.select(
                F.col("job_id").alias("_om_job"),
                F.col(occ_uri_col).alias("_om_occ"),
                F.col(occ_label_col).alias("_om_label"),
            ),
            F.col("_so_job") == F.col("_om_job"),
            "inner",
        )
        .groupBy("_so_skill", "_om_occ", "_om_label")
        .agg(F.countDistinct("_so_job").alias("_occ_jobs"))
    )

    occ_window = Window.partitionBy("_so_skill").orderBy(F.col("_occ_jobs").desc())
    top_occ = (
        skill_occ.withColumn("_rn", F.row_number().over(occ_window))
        .where(F.col("_rn") <= top_occupations)
        .groupBy("_so_skill")
        .agg(
            F.to_json(
                F.collect_list(
                    F.struct(
                        F.col("_om_occ").alias("occupation_uri"),
                        F.col("_om_label").alias("occupation_label"),
                        F.col("_occ_jobs").alias("jobs_count"),
                    )
                )
            ).alias("top_occupations_json")
        )
    )

    # ── Step 4: Top companies ────────────────────────────────────────
    skill_companies = (
        skill_matches_df.select(
            F.col(skill_uri_col).alias("_sc_skill"),
            F.col("job_id").alias("_sc_job"),
        )
        .join(
            jobs_df.select(
                F.col("job_id").alias("_j_id"),
                F.col("company_name").alias("_company"),
            ),
            F.col("_sc_job") == F.col("_j_id"),
            "left",
        )
        .where(F.col("_company").isNotNull())
        .groupBy("_sc_skill", "_company")
        .agg(F.count("*").alias("_co_count"))
    )

    co_window = Window.partitionBy("_sc_skill").orderBy(F.col("_co_count").desc())
    top_co = (
        skill_companies.withColumn("_rn", F.row_number().over(co_window))
        .where(F.col("_rn") <= top_companies)
        .groupBy("_sc_skill")
        .agg(
            F.to_json(F.collect_list(F.struct("_company", "_co_count"))).alias("top_companies_json")
        )
    )

    # ── Step 5: Skill labels from Silver ─────────────────────────────
    skill_labels = skills_df.select(
        F.col("concept_uri").alias("_sl_uri"),
        F.col("concept_uri_uuid").alias("_sl_uuid"),
        F.col("preferred_label").alias("_sl_label"),
        F.col("skill_type").alias("_sl_type"),
        F.col("alt_labels").alias("_sl_alt"),
        F.col("hidden_labels").alias("_sl_hidden"),
    )

    # ── Step 6: Assemble ─────────────────────────────────────────────
    profile = (
        demand_base.join(salary_base, F.col(skill_uri_col) == F.col("_sal_skill"), "left")
        .join(top_occ, F.col(skill_uri_col) == F.col("_so_skill"), "left")
        .join(top_co, F.col(skill_uri_col) == F.col("_sc_skill"), "left")
        .join(skill_labels, F.col(skill_uri_col) == F.col("_sl_uri"), "left")
    )

    # Build searchable text
    profile = profile.withColumn(
        "skill_search_text",
        F.lower(
            F.concat_ws(
                " ",
                F.col("_sl_label"),
                F.coalesce(F.concat_ws(" ", F.col("_sl_alt")), F.lit("")),
                F.coalesce(F.concat_ws(" ", F.col("_sl_hidden")), F.lit("")),
            )
        ),
    )

    profile = (
        profile.withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("country", F.lit(country))
        .withColumn(gold_schema.esco_skill("concept_uri_uuid"), F.col("_sl_uuid"))
        .withColumn(gold_schema.esco_skill("preferred_label"), F.col("_sl_label"))
        .withColumn("skill_type", F.coalesce(F.col("_sl_type"), F.lit("")))
        .na.fill(0, subset=["jobs_count", "companies_count", "locations_count"])
        .na.fill("[]", subset=["top_occupations_json", "top_companies_json"])
    )

    return profile.select(
        "ingestion_date",
        "country",
        skill_uri_col,
        gold_schema.esco_skill("concept_uri_uuid"),
        gold_schema.esco_skill("preferred_label"),
        "skill_type",
        "skill_search_text",
        "jobs_count",
        "companies_count",
        "locations_count",
        "avg_salary_mean",
        "top_occupations_json",
        "top_companies_json",
    )
