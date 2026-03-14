"""ESCO Silver formatting — Bronze → Silver Iceberg pipeline.

Reads from Bronze Iceberg tables and writes typed + normalized Silver
tables with deduplication and partition-level idempotency.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.logging.context import get_context
from skill_radar.platform.runtime import get_runtime_context, resolve_s3_endpoint
from skill_radar.platform.spark.transforms import (
    dedupe_by_key,
    extract_uri_uuid,
    normalize_text_col,
    parse_date_col,
    split_newline_labels,
)

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ── Result dataclasses ────────────────────────────────────────────────────


@dataclass
class SilverEntityResult:
    """Per-entity formatting outcome."""

    entity: str
    table: str = ""
    input_row_count: int = 0
    output_row_count: int = 0
    duplicates_removed: int = 0
    status: str = "pending"
    error: str = ""


@dataclass
class SilverFormatResult:
    """Full run outcome."""

    run_id: str = ""
    version: str = ""
    lang: str = ""
    entities: list[SilverEntityResult] = field(default_factory=list)
    success: bool = True

    def summary_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "version": self.version,
            "lang": self.lang,
            "success": self.success,
            "entities": [asdict(e) for e in self.entities],
        }


# ── Namespace helpers ─────────────────────────────────────────────────────


SILVER_NAMESPACE = "sr_silver"


def ensure_silver_namespace(spark: SparkSession, *, catalog: str = "sr") -> None:
    """Create the Silver Iceberg namespace if it does not exist."""
    fqn = f"{catalog}.{SILVER_NAMESPACE}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fqn}")
    logger.info("Ensured Iceberg namespace: %s", fqn)


def write_silver_table(
    df: DataFrame,
    table_fqn: str,
    spark: SparkSession,
) -> None:
    """Write a DataFrame to an Iceberg Silver table with partition overwrite.

    If the table does not exist, it is created with Iceberg format-version=2
    partitioned by ``(version, lang)``. If it exists, overwrite matching
    ``(version, lang)`` partitions for idempotency.
    """
    if not spark.catalog.tableExists(table_fqn):
        logger.info("Creating Iceberg table: %s", table_fqn)
        (
            df.writeTo(table_fqn)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .partitionedBy("version", "lang")  # type: ignore[arg-type]
            .create()
        )
    else:
        logger.info("Overwriting partitions in: %s", table_fqn)
        df.writeTo(table_fqn).overwritePartitions()


# ── Entity transformations ────────────────────────────────────────────────


def _transform_skills(df: DataFrame) -> DataFrame:
    """Transform skills entity from Bronze to Silver schema.

    Applies:
    - Text normalization on label/description fields
    - Label array splitting for alt_labels and hidden_labels
    - UUID extraction from concept_uri
    - Date parsing for modified_date
    """
    # Start with select to control output columns
    result = df.select(
        # Primary key
        F.col("concept_uri"),
        extract_uri_uuid("concept_uri").alias("concept_uri_uuid"),
        # Typing / classification
        F.col("concept_type")
        if "concept_type" in df.columns
        else F.lit(None).alias("concept_type"),
        F.col("skill_type"),
        F.col("reuse_level"),
        # Text fields - normalized
        normalize_text_col("preferred_label").alias("preferred_label"),
        normalize_text_col("description").alias("description"),
        # Keep scope_note and definition if present
        *(
            [normalize_text_col("scope_note").alias("scope_note")]
            if "scope_note" in df.columns
            else []
        ),
        *(
            [normalize_text_col("definition").alias("definition")]
            if "definition" in df.columns
            else []
        ),
        # Label arrays from raw fields
        split_newline_labels("alt_labels_raw").alias("alt_labels"),
        split_newline_labels("hidden_labels_raw").alias("hidden_labels"),
        # Date parsing - keep raw for traceability
        *(
            [F.col("modified_date").alias("modified_date_raw")]
            if "modified_date" in df.columns
            else []
        ),
        *(
            [parse_date_col("modified_date").alias("modified_date")]
            if "modified_date" in df.columns
            else []
        ),
        # Lineage columns
        F.col("dataset"),
        F.col("entity"),
        F.col("version"),
        F.col("lang"),
        F.col("source_zip_key"),
        F.col("manifest_key"),
        F.col("artifact_sha256"),
        F.col("ingested_at_utc"),
        F.col("run_id"),
    )

    # Add computed counts
    result = result.withColumn("alt_labels_count", F.size(F.col("alt_labels")))
    result = result.withColumn("hidden_labels_count", F.size(F.col("hidden_labels")))

    return result


def _transform_occupations(df: DataFrame) -> DataFrame:
    """Transform occupations entity from Bronze to Silver schema.

    Applies:
    - Text normalization on label/description fields
    - Label array splitting for alt_labels and hidden_labels
    - UUID extraction from concept_uri
    - Date parsing for modified_date
    """
    # Build column list dynamically based on what exists in Bronze
    result = df.select(
        # Primary key
        F.col("concept_uri"),
        extract_uri_uuid("concept_uri").alias("concept_uri_uuid"),
        # Typing / classification
        F.col("concept_type")
        if "concept_type" in df.columns
        else F.lit(None).alias("concept_type"),
        *([F.col("isco_group")] if "isco_group" in df.columns else []),
        *([F.col("code")] if "code" in df.columns else []),
        *([F.col("nace_code")] if "nace_code" in df.columns else []),
        # Text fields - normalized
        normalize_text_col("preferred_label").alias("preferred_label"),
        normalize_text_col("description").alias("description"),
        # Keep scope_note and definition if present
        *(
            [normalize_text_col("scope_note").alias("scope_note")]
            if "scope_note" in df.columns
            else []
        ),
        *(
            [normalize_text_col("definition").alias("definition")]
            if "definition" in df.columns
            else []
        ),
        # Label arrays from raw fields
        split_newline_labels("alt_labels_raw").alias("alt_labels"),
        split_newline_labels("hidden_labels_raw").alias("hidden_labels"),
        # Date parsing - keep raw for traceability
        *(
            [F.col("modified_date").alias("modified_date_raw")]
            if "modified_date" in df.columns
            else []
        ),
        *(
            [parse_date_col("modified_date").alias("modified_date")]
            if "modified_date" in df.columns
            else []
        ),
        # Lineage columns
        F.col("dataset"),
        F.col("entity"),
        F.col("version"),
        F.col("lang"),
        F.col("source_zip_key"),
        F.col("manifest_key"),
        F.col("artifact_sha256"),
        F.col("ingested_at_utc"),
        F.col("run_id"),
    )

    # Add computed counts
    result = result.withColumn("alt_labels_count", F.size(F.col("alt_labels")))
    result = result.withColumn("hidden_labels_count", F.size(F.col("hidden_labels")))

    return result


def _transform_relations(df: DataFrame) -> DataFrame:
    """Transform relations entity from Bronze to Silver schema.

    Applies:
    - Text normalization on label fields
    - relation_type normalization (trim, lower, collapse whitespace)
    - UUID extraction from URIs
    """
    result = df.select(
        # Primary keys
        F.col("occupation_uri"),
        extract_uri_uuid("occupation_uri").alias("occupation_uri_uuid"),
        F.col("skill_uri"),
        extract_uri_uuid("skill_uri").alias("skill_uri_uuid"),
        # Labels - normalized
        normalize_text_col("occupation_label").alias("occupation_label"),
        normalize_text_col("skill_label").alias("skill_label"),
        # relation_type - normalized (trim + lowercase + collapse whitespace)
        F.lower(normalize_text_col("relation_type")).alias("relation_type"),
        # skill_type - normalized
        normalize_text_col("skill_type").alias("skill_type"),
        # Lineage columns
        F.col("dataset"),
        F.col("entity"),
        F.col("version"),
        F.col("lang"),
        F.col("source_zip_key"),
        F.col("manifest_key"),
        F.col("artifact_sha256"),
        F.col("ingested_at_utc"),
        F.col("run_id"),
    )

    return result


# ── Deduplication helpers ─────────────────────────────────────────────────


def _dedupe_skills(df: DataFrame) -> DataFrame:
    """Deduplicate skills by (concept_uri, version, lang)."""
    # Determine order columns - prefer modified_date if it exists
    order_cols: list[tuple[str, bool]] = []
    if "modified_date" in df.columns:
        order_cols.append(("modified_date", True))  # desc
    order_cols.append(("ingested_at_utc", True))  # desc

    return dedupe_by_key(
        df,
        key_cols=["concept_uri", "version", "lang"],
        order_cols=order_cols,
    )


def _dedupe_occupations(df: DataFrame) -> DataFrame:
    """Deduplicate occupations by (concept_uri, version, lang)."""
    order_cols: list[tuple[str, bool]] = []
    if "modified_date" in df.columns:
        order_cols.append(("modified_date", True))  # desc
    order_cols.append(("ingested_at_utc", True))  # desc

    return dedupe_by_key(
        df,
        key_cols=["concept_uri", "version", "lang"],
        order_cols=order_cols,
    )


def _dedupe_relations(df: DataFrame) -> DataFrame:
    """Deduplicate relations by (occupation_uri, skill_uri, relation_type, version, lang)."""
    return dedupe_by_key(
        df,
        key_cols=["occupation_uri", "skill_uri", "relation_type", "version", "lang"],
        order_cols=[("ingested_at_utc", True)],  # desc
    )


# ── Entity transformation registry ───────────────────────────────────────


_ENTITY_TRANSFORMS = {
    "skills": (_transform_skills, _dedupe_skills),
    "occupations": (_transform_occupations, _dedupe_occupations),
    "relations": (_transform_relations, _dedupe_relations),
}


# ── Main orchestrator ─────────────────────────────────────────────────────


def run_silver_format(
    spark: SparkSession,
    config: PlatformSettings | None = None,
    *,
    version: str,
    lang: str,
    entities: list[str] | None = None,
    dry_run: bool = False,
    run_id: str | None = None,
) -> SilverFormatResult:
    """Execute the ESCO Silver formatting pipeline.

    Parameters
    ----------
    spark:
        Active SparkSession (with Iceberg catalog ``sr`` configured).
    config:
        Optional pre-loaded platform config.
    version:
        ESCO artifact version (e.g. ``v1.2.1``).
    lang:
        Language code (e.g. ``fr``).
    entities:
        Subset of entities to process. ``None`` means all entities.
    dry_run:
        Validate and display the formatting plan without writing to Iceberg.
    run_id:
        Optional run ID override (otherwise read from logging context).

    Returns
    -------
    SilverFormatResult
        Result containing per-entity outcomes.
    """
    cfg = config or load_platform_config()
    layout = LakeLayout(cfg)

    # Resolve run_id from logging context (requires init_logging)
    ctx = get_context()
    resolved_run_id = run_id or ctx.run_id

    result = SilverFormatResult(
        run_id=resolved_run_id,
        version=version,
        lang=lang,
    )

    # Determine which entities to process
    all_entities = list(_ENTITY_TRANSFORMS.keys())
    target_entities = entities if entities else all_entities

    # Validate entity names
    for name in target_entities:
        if name not in _ENTITY_TRANSFORMS:
            raise ValueError(f"Unknown entity '{name}'. Available: {all_entities}")

    if dry_run:
        logger.info("[DRY-RUN] Silver formatting plan:")
        for entity in target_entities:
            bronze_fqn = layout.iceberg_table_fqn("bronze", "esco", entity, raw=True)
            silver_fqn = layout.iceberg_table_fqn("silver", "esco", entity, raw=False)
            logger.info("  entity=%s  bronze=%s  silver=%s", entity, bronze_fqn, silver_fqn)
        result.success = True
        return result

    # Ensure Silver namespace exists
    ensure_silver_namespace(spark)

    # Process each entity
    for entity_name in target_entities:
        er = SilverEntityResult(entity=entity_name)
        result.entities.append(er)

        transform_fn, dedupe_fn = _ENTITY_TRANSFORMS[entity_name]
        bronze_fqn = layout.iceberg_table_fqn("bronze", "esco", entity_name, raw=True)
        silver_fqn = layout.iceberg_table_fqn("silver", "esco", entity_name, raw=False)
        er.table = silver_fqn

        try:
            logger.info(
                "Processing entity=%s  bronze=%s  silver=%s",
                entity_name,
                bronze_fqn,
                silver_fqn,
            )

            # Read from Bronze, filtered by partition
            df_bronze = spark.read.table(bronze_fqn).where(
                (F.col("version") == version) & (F.col("lang") == lang)
            )

            input_count = df_bronze.count()
            er.input_row_count = input_count
            logger.info("Read %d rows from Bronze for %s", input_count, entity_name)

            if input_count == 0:
                er.status = "skipped"
                er.error = "No rows found in Bronze for this partition"
                logger.warning(
                    "No rows found in Bronze for %s (version=%s, lang=%s)",
                    entity_name,
                    version,
                    lang,
                )
                continue

            # Transform
            df_transformed = transform_fn(df_bronze)

            # Deduplicate
            df_deduped = dedupe_fn(df_transformed)

            output_count = df_deduped.count()
            er.output_row_count = output_count
            er.duplicates_removed = input_count - output_count
            logger.info(
                "Transformed %d → %d rows (%d duplicates removed) for %s",
                input_count,
                output_count,
                er.duplicates_removed,
                entity_name,
            )

            # Write to Silver
            write_silver_table(df_deduped, silver_fqn, spark)
            er.status = "success"
            logger.info(
                "Wrote %d rows to %s (version=%s, lang=%s)",
                output_count,
                silver_fqn,
                version,
                lang,
            )

        except Exception as exc:
            er.status = "failed"
            er.error = str(exc)
            result.success = False
            logger.exception("Error processing %s", entity_name)

    return result


# ── Run summary upload ────────────────────────────────────────────────────


def upload_run_summary(
    run_result: SilverFormatResult,
    *,
    config: PlatformSettings | None = None,
) -> str | None:
    """Upload a small JSON run summary to the logs bucket.

    Key layout::

        runs/esco_silver_format/dt=YYYY-MM-DD/run_id=<run_id>/summary.json

    Returns the S3 key on success, ``None`` on failure.
    """
    import boto3

    cfg = config or load_platform_config()
    logs_bucket = cfg.logging.logs_bucket
    dt = datetime.now(UTC).strftime("%Y-%m-%d")
    key = f"runs/esco_silver_format/dt={dt}/run_id={run_result.run_id}/summary.json"

    try:
        body = json.dumps(run_result.summary_dict(), indent=2, default=str)
        ctx = get_runtime_context()
        s3_endpoint = resolve_s3_endpoint(cfg.storage.s3, ctx)
        client = boto3.client("s3", endpoint_url=s3_endpoint)
        client.put_object(
            Bucket=logs_bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Uploaded run summary → s3://%s/%s", logs_bucket, key)
        return key
    except Exception:
        logger.warning("Failed to upload run summary", exc_info=True)
        return None
