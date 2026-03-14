"""Gold skill demand segmentation — KMeans clustering via Spark MLlib.

Grain: one row per (ingestion_date, country, esco_skill_concept_uri).
Assigns each skill to a market segment (niche/growing/established/dominant)
based on demand features.  Falls back to ``"unclustered"`` when
``min_rows`` is not met.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import schema as gold_schema

if TYPE_CHECKING:
    from skill_radar.config.models import MLSegmentsConfig

logger = logging.getLogger(__name__)


def compute_skill_demand_segments(
    skill_demand_df: DataFrame,
    *,
    ingestion_date: str,
    config: MLSegmentsConfig | None = None,
) -> DataFrame:
    """Assign demand segments to skills using KMeans.

    Parameters
    ----------
    skill_demand_df:
        The ``skill_demand_daily`` table filtered to the target partition
        (one row per skill).  Must include: ``ingestion_date``, ``country``,
        ``esco_skill_concept_uri``, ``esco_skill_preferred_label``,
        ``jobs_count``, ``unique_companies_count``, ``unique_locations_count``,
        plus Gold lineage columns.
    ingestion_date:
        Target date (used for logging / fallback).
    config:
        ML segment parameters.  Defaults from ``MLSegmentsConfig()``.

    Returns
    -------
    DataFrame
        Input enriched with ``segment_id`` (int) and ``segment_label`` (str).
    """
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import VectorAssembler

    from skill_radar.config.models import MLSegmentsConfig

    cfg = config or MLSegmentsConfig()
    logger.info(
        "Computing skill demand segments (k=%d) for %s",
        cfg.k,
        ingestion_date,
    )

    skill_uri_col = gold_schema.esco_skill("concept_uri")
    skill_label_col = gold_schema.esco_skill("preferred_label")

    # Select and filter to target date
    base = skill_demand_df.where(F.col("ingestion_date") == ingestion_date).select(
        "ingestion_date",
        "country",
        skill_uri_col,
        skill_label_col,
        "jobs_count",
        "unique_companies_count",
        "unique_locations_count",
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    )

    row_count = base.count()

    if row_count < cfg.min_rows:
        logger.warning(
            "Only %d skills (< min_rows=%d) — skipping KMeans, labelling as 'unclustered'",
            row_count,
            cfg.min_rows,
        )
        return base.withColumn("segment_id", F.lit(0)).withColumn(
            "segment_label", F.lit("unclustered")
        )

    # ── Feature assembly ──────────────────────────────────────────────
    feature_cols = [c for c in cfg.features if c in base.columns]
    if not feature_cols:
        feature_cols = ["jobs_count"]

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="_features",
        handleInvalid="skip",
    )
    assembled = assembler.transform(base.fillna(0, subset=feature_cols))

    # ── KMeans ────────────────────────────────────────────────────────
    kmeans = KMeans(
        k=min(cfg.k, row_count),
        seed=cfg.seed,
        maxIter=cfg.max_iter,
        featuresCol="_features",
        predictionCol="segment_id",
    )
    model = kmeans.fit(assembled)
    clustered = model.transform(assembled).drop("_features")

    # ── Label mapping — order clusters by centroid magnitude ──────────
    # Cluster with highest centroid norm → highest segment index name
    import numpy as np

    centers = model.clusterCenters()
    norms = [(i, float(np.linalg.norm(c))) for i, c in enumerate(centers)]
    norms.sort(key=lambda x: x[1])
    # Build mapping: original_cluster_id → rank
    rank_map = {original_id: rank for rank, (original_id, _) in enumerate(norms)}

    segment_names = cfg.segment_names
    # Build Spark mapping expression
    mapping_expr = F.create_map(
        *[
            item
            for cluster_id, rank in rank_map.items()
            for item in (
                F.lit(cluster_id),
                F.lit(segment_names.get(str(rank), f"segment_{rank}")),
            )
        ]
    )

    result = clustered.withColumn("segment_label", mapping_expr[F.col("segment_id")])

    return result.select(
        "ingestion_date",
        "country",
        skill_uri_col,
        skill_label_col,
        "segment_id",
        "segment_label",
        "jobs_count",
        "unique_companies_count",
        "unique_locations_count",
        gold_schema.gold_meta("run_id"),
        gold_schema.gold_meta("generated_at_utc"),
        "esco_version",
        "esco_lang",
    )
