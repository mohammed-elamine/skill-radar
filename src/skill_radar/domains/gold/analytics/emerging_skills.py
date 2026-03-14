"""Gold emerging-skill signals — momentum, acceleration, and novelty scores.

Grain: one row per (ingestion_date, country, esco_skill_concept_uri).
Composite score is a configurable weighted sum of momentum, acceleration,
and novelty, clamped to [0, 1].
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .. import schema as gold_schema

if TYPE_CHECKING:
    from skill_radar.config.models import EmergingScoreConfig

logger = logging.getLogger(__name__)


def compute_skill_emerging_daily(
    skill_demand_df: DataFrame,
    *,
    ingestion_date: str,
    weights: EmergingScoreConfig | None = None,
) -> DataFrame:
    """Compute emerging-skill signals for one ingestion date.

    Parameters
    ----------
    skill_demand_df:
        The **full** ``skill_demand_daily`` table (all dates for the same
        country).  Must include columns: ``ingestion_date``, ``country``,
        ``esco_skill_concept_uri``, ``esco_skill_preferred_label``,
        ``jobs_count``, plus Gold lineage columns.
    ingestion_date:
        The target date for which to compute signals.
    weights:
        Composite-score weights.  Defaults to 0.4 / 0.3 / 0.3.

    Returns
    -------
    DataFrame
        One row per (ingestion_date, country, esco_skill_concept_uri) with
        momentum_score, acceleration_score, novelty_score, and
        emerging_composite_score.
    """
    from skill_radar.config.models import EmergingScoreConfig

    w = weights or EmergingScoreConfig()
    logger.info("Computing emerging-skill signals for %s", ingestion_date)

    skill_uri_col = gold_schema.esco_skill("concept_uri")
    skill_label_col = gold_schema.esco_skill("preferred_label")

    # ── Partition window for per-country ranking ──────────────────────
    country_date_window = Window.partitionBy("country", "ingestion_date")

    # ── Momentum: percent-rank of jobs_count within same (country, date)
    demand = skill_demand_df.select(
        "ingestion_date",
        "country",
        skill_uri_col,
        skill_label_col,
        "jobs_count",
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    )

    ranked = demand.withColumn(
        "momentum_score",
        F.percent_rank().over(country_date_window.orderBy(F.col("jobs_count").asc())),
    )

    # ── Acceleration: day-over-day Δ ratio ────────────────────────────
    # Self-join current date with previous date
    current = ranked.where(F.col("ingestion_date") == ingestion_date)
    prev = demand.select(
        F.col("ingestion_date").alias("_prev_date"),
        F.col("country").alias("_prev_country"),
        F.col(skill_uri_col).alias("_prev_uri"),
        F.col("jobs_count").alias("prev_jobs_count"),
    ).where(F.col("_prev_date") == F.date_sub(F.lit(ingestion_date), 1))

    with_prev = current.join(
        prev,
        on=[
            current["country"] == prev["_prev_country"],
            current[skill_uri_col] == prev["_prev_uri"],
        ],
        how="left",
    ).drop("_prev_date", "_prev_country", "_prev_uri")

    # acceleration = (current - prev) / max(prev, 1);  clamped to [0, 1]
    with_accel = with_prev.withColumn(
        "acceleration_score",
        F.when(
            F.col("prev_jobs_count").isNull(),
            F.lit(0.0),
        ).otherwise(
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    (F.col("jobs_count") - F.col("prev_jobs_count"))
                    / F.greatest(F.col("prev_jobs_count"), F.lit(1)),
                ),
            )
        ),
    ).drop("prev_jobs_count")

    # ── Novelty: inverse of observation count ─────────────────────────
    # Count how many distinct dates this skill has been observed
    obs_counts = demand.groupBy("country", skill_uri_col).agg(
        F.countDistinct("ingestion_date").alias("_obs_days")
    )

    with_novelty = (
        with_accel.join(
            obs_counts,
            on=["country", skill_uri_col],
            how="left",
        )
        .withColumn(
            "novelty_score",
            F.lit(1.0) / F.greatest(F.col("_obs_days"), F.lit(1)),
        )
        .drop("_obs_days")
    )

    # ── Composite score ───────────────────────────────────────────────
    result = with_novelty.withColumn(
        "emerging_composite_score",
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                F.lit(w.w_momentum) * F.col("momentum_score")
                + F.lit(w.w_acceleration) * F.col("acceleration_score")
                + F.lit(w.w_novelty) * F.col("novelty_score"),
            ),
        ),
    )

    # Keep only the target date rows
    result = result.where(F.col("ingestion_date") == ingestion_date)

    return result.select(
        "ingestion_date",
        "country",
        skill_uri_col,
        skill_label_col,
        "jobs_count",
        "momentum_score",
        "acceleration_score",
        "novelty_score",
        "emerging_composite_score",
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    )
