"""Adzuna Silver formatting — Bronze → Silver Iceberg pipeline.

Reads from the Bronze ``adzuna_jobs_raw`` table, applies normalization
and deduplication, and writes a typed Silver ``adzuna_jobs`` table.

Silver design principles:
- One normalized job fact table for downstream analytics/matching.
- Strongly typed fields; raw semantics preserved (no NLP).
- Deduplication by ``(country, job_id)`` keeping latest Bronze record.
- Partition-overwrite for idempotent reruns (by ``country``, ``ingestion_date``).
- Adzuna Silver is kept separate from ESCO Silver — Gold joins come later.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.logging.context import get_context

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------


@dataclass
class SilverFormatResult:
    """Outcome of a Silver formatting run."""

    run_id: str = ""
    country: str = ""
    ingestion_date: str = ""
    input_row_count: int = 0
    output_row_count: int = 0
    duplicates_removed: int = 0
    target_table: str = ""
    bronze_table: str = ""
    formatted_at_utc: str = ""
    success: bool = True
    error: str = ""

    def summary_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Pure normalization helpers (no Spark dependency)
# ---------------------------------------------------------------------------


def _safe_parse_json_array(value: str | None) -> list[str]:
    """Parse a JSON string into a list of strings, returning [] on failure."""
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def _location_hierarchy(area: list[str]) -> tuple[str, str, str]:
    """Derive (country, region, subregion) from Adzuna location area array.

    Adzuna area arrays are typically ordered broad → narrow:
    e.g. ["France", "Île-de-France", "Paris"]
    """
    country = area[0] if len(area) > 0 else ""
    region = area[1] if len(area) > 1 else ""
    subregion = area[2] if len(area) > 2 else ""
    return country, region, subregion


# ---------------------------------------------------------------------------
# Spark UDFs and column transforms
# ---------------------------------------------------------------------------


@F.udf(returnType=T.ArrayType(T.StringType()))
def _parse_area_udf(area_json: str) -> list[str]:
    """UDF: parse location_area_json into array<string>."""
    return _safe_parse_json_array(area_json)


def _normalize_text(col_name: str) -> F.Column:
    """Trim and collapse whitespace in a text column."""
    return F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " ")


def _lower_normalize(col_name: str, alias: str) -> F.Column:
    """Lowercase normalized text helper column."""
    return F.lower(_normalize_text(col_name)).alias(alias)


def _parse_created_at(col_name: str = "created_at_raw") -> F.Column:
    """Parse Adzuna ``created`` timestamp string to UTC timestamp.

    Adzuna format is typically ISO-8601: ``2026-03-05T12:34:56Z``
    """
    return F.to_timestamp(F.col(col_name))


def _safe_boolean(col_name: str) -> F.Column:
    """Parse a raw string to boolean (handles '0'/'1', 'true'/'false', etc.)."""
    c = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(c.isin("1", "true", "t", "yes"), F.lit(True))
        .when(c.isin("0", "false", "f", "no"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


# ---------------------------------------------------------------------------
# Main Silver transform
# ---------------------------------------------------------------------------


def _transform_bronze_to_silver(
    df: DataFrame,
    *,
    silver_run_id: str,
    formatted_at_utc: str,
) -> DataFrame:
    """Apply all Silver transformations to a Bronze DataFrame.

    This function:
    1. Parses timestamps and JSON fields.
    2. Derives location hierarchy.
    3. Computes salary_mean and boolean flags.
    4. Adds normalized helper columns.
    5. Adds Silver lineage columns.
    """
    # Parse location area JSON into array.
    silver = df.withColumn("location_area", _parse_area_udf(F.col("location_area_json")))

    # Derive location hierarchy.
    silver = (
        silver.withColumn(
            "location_country",
            F.when(F.size("location_area") > 0, F.element_at("location_area", 1)).otherwise(
                F.lit("")
            ),
        )
        .withColumn(
            "location_region",
            F.when(F.size("location_area") > 1, F.element_at("location_area", 2)).otherwise(
                F.lit("")
            ),
        )
        .withColumn(
            "location_subregion",
            F.when(F.size("location_area") > 2, F.element_at("location_area", 3)).otherwise(
                F.lit("")
            ),
        )
    )

    # Parse created_at timestamp.
    silver = silver.withColumn("posted_at_utc", _parse_created_at("created_at_raw"))
    silver = silver.withColumn(
        "posted_date",
        F.date_format(F.col("posted_at_utc"), "yyyy-MM-dd"),
    )

    # Salary mean.
    silver = silver.withColumn(
        "salary_mean",
        F.when(
            F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull(),
            (F.col("salary_min") + F.col("salary_max")) / 2.0,
        ).otherwise(F.lit(None).cast("double")),
    )

    # Boolean parsing.
    silver = silver.withColumn("salary_is_predicted", _safe_boolean("salary_is_predicted_raw"))

    # Contract boolean flags.
    ct = F.lower(F.trim(F.col("contract_time_raw")))
    silver = silver.withColumn("is_full_time", ct == F.lit("full_time"))
    silver = silver.withColumn("is_part_time", ct == F.lit("part_time"))

    ctype = F.lower(F.trim(F.col("contract_type_raw")))
    silver = silver.withColumn("is_permanent", ctype == F.lit("permanent"))
    silver = silver.withColumn("is_contract", ctype == F.lit("contract"))

    # Normalized helpers.
    silver = silver.withColumn("title_normalized", _lower_normalize("title", "title_normalized"))
    silver = silver.withColumn(
        "description_normalized",
        F.lower(_normalize_text("description")),
    )
    silver = silver.withColumn(
        "company_normalized",
        F.lower(_normalize_text("company_display_name")),
    )
    silver = silver.withColumn(
        "location_normalized",
        F.lower(_normalize_text("location_display_name")),
    )

    # Silver lineage columns.
    silver = silver.withColumn("silver_run_id", F.lit(silver_run_id))
    silver = silver.withColumn("formatted_at_utc", F.lit(formatted_at_utc).cast("timestamp"))

    # Rename Bronze lineage columns for Silver.
    silver = silver.withColumn(
        "bronze_extracted_at_utc", F.col("extracted_at_utc").cast("timestamp")
    )
    silver = silver.withColumn("bronze_run_id", F.col("run_id"))

    # Select final Silver schema.
    return silver.select(
        # Identity
        F.col("job_id"),
        F.col("adref"),
        F.lit("adzuna").alias("source_system"),
        F.col("country"),
        # Job content
        _normalize_text("title").alias("job_title"),
        _normalize_text("description").alias("job_description"),
        F.col("posted_at_utc"),
        F.col("posted_date"),
        F.col("redirect_url").alias("job_url"),
        # Company
        _normalize_text("company_display_name").alias("company_name"),
        F.col("company_canonical_name"),
        # Category
        F.col("category_tag"),
        F.col("category_label"),
        # Location
        F.col("location_display_name"),
        F.col("location_area"),
        F.col("location_country"),
        F.col("location_region"),
        F.col("location_subregion"),
        F.col("latitude"),
        F.col("longitude"),
        # Compensation
        F.col("salary_min"),
        F.col("salary_max"),
        F.col("salary_mean"),
        F.col("salary_is_predicted"),
        # Contract
        F.col("contract_time_raw").alias("contract_time"),
        F.col("contract_type_raw").alias("contract_type"),
        F.col("is_full_time"),
        F.col("is_part_time"),
        F.col("is_permanent"),
        F.col("is_contract"),
        # Normalization helpers
        F.col("title_normalized"),
        F.col("description_normalized"),
        F.col("company_normalized"),
        F.col("location_normalized"),
        # Lineage
        F.col("bronze_extracted_at_utc"),
        F.col("bronze_run_id"),
        F.col("search_key"),
        F.col("search_params_json"),
        F.col("silver_run_id"),
        F.col("formatted_at_utc"),
        F.col("ingestion_date"),
    )


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def _deduplicate_jobs(df: DataFrame) -> DataFrame:
    """Deduplicate Silver jobs by ``(country, job_id)``.

    Keeps the most recent Bronze record by ``bronze_extracted_at_utc``.
    Rows with null ``job_id`` are preserved without deduplication.
    """
    # Separate null job_id rows (preserve all).
    null_ids = df.where(F.col("job_id").isNull() | (F.trim(F.col("job_id")) == F.lit("")))
    valid_ids = df.where(F.col("job_id").isNotNull() & (F.trim(F.col("job_id")) != F.lit("")))

    if valid_ids.isEmpty():
        return df

    window = Window.partitionBy("country", "job_id").orderBy(
        F.col("bronze_extracted_at_utc").desc_nulls_last(),
        F.col("adref").desc_nulls_last(),
    )
    deduped = (
        valid_ids.withColumn("__row_num__", F.row_number().over(window))
        .where(F.col("__row_num__") == 1)
        .drop("__row_num__")
    )

    return deduped.unionByName(null_ids) if not null_ids.isEmpty() else deduped


# ---------------------------------------------------------------------------
# Iceberg write
# ---------------------------------------------------------------------------


def _write_silver_table(
    df: DataFrame,
    table_fqn: str,
    spark: SparkSession,
) -> None:
    """Write DataFrame to Silver Iceberg with partition overwrite."""
    if not spark.catalog.tableExists(table_fqn):
        logger.info("Creating Silver Iceberg table: %s", table_fqn)
        (
            df.writeTo(table_fqn)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .partitionedBy("country", "ingestion_date")  # type: ignore[arg-type]
            .create()
        )
    else:
        logger.info("Overwriting partitions in Silver table: %s", table_fqn)
        df.writeTo(table_fqn).overwritePartitions()


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_silver_format(
    spark: SparkSession,
    *,
    country: str | None = None,
    ingestion_date: str | None = None,
    run_id: str | None = None,
    config: PlatformSettings | None = None,
) -> SilverFormatResult:
    """Execute a Silver formatting run for Adzuna jobs.

    Reads Bronze raw table, transforms, deduplicates, and writes to
    the Silver Iceberg table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    country:
        Country scope. Defaults to config default.
    ingestion_date:
        Ingestion date scope (YYYY-MM-DD). Defaults to current UTC date.
    run_id:
        Run identifier for lineage. Defaults from logging context.
    config:
        Platform configuration. Loaded from defaults if not provided.

    Returns
    -------
    SilverFormatResult
        Structured result with metrics and lineage metadata.
    """
    cfg = config or load_platform_config()
    layout = LakeLayout(cfg)

    now_utc = datetime.now(UTC)
    formatted_at_utc = now_utc.isoformat()
    resolved_date = ingestion_date or now_utc.strftime("%Y-%m-%d")
    resolved_country = country or cfg.adzuna.default_country

    try:
        ctx = get_context()
        resolved_run_id = run_id or ctx.run_id
    except RuntimeError:
        import uuid

        resolved_run_id = run_id or uuid.uuid4().hex[:12]

    bronze_table = layout.adzuna_bronze_jobs_raw_fqn()
    silver_table = layout.adzuna_silver_jobs_fqn()

    result = SilverFormatResult(
        run_id=resolved_run_id,
        country=resolved_country,
        ingestion_date=resolved_date,
        bronze_table=bronze_table,
        target_table=silver_table,
        formatted_at_utc=formatted_at_utc,
    )

    logger.info(
        "Starting Silver formatting: country=%s date=%s run_id=%s",
        resolved_country,
        resolved_date,
        resolved_run_id,
    )

    try:
        # Ensure Silver namespace.
        ns_fqn = layout.iceberg_namespace("silver")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns_fqn}")

        # Read Bronze scope.
        if not spark.catalog.tableExists(bronze_table):
            result.success = False
            result.error = f"Bronze table does not exist: {bronze_table}"
            return result

        bronze_df = spark.table(bronze_table).where(
            (F.col("country") == resolved_country) & (F.col("ingestion_date") == resolved_date)
        )

        input_count = bronze_df.count()
        result.input_row_count = input_count

        if input_count == 0:
            logger.warning(
                "No Bronze rows for country=%s date=%s — writing empty Silver partition.",
                resolved_country,
                resolved_date,
            )
            result.output_row_count = 0
            result.duplicates_removed = 0
            return result

        # Transform.
        silver_df = _transform_bronze_to_silver(
            bronze_df,
            silver_run_id=resolved_run_id,
            formatted_at_utc=formatted_at_utc,
        )

        # Deduplicate.
        deduped_df = _deduplicate_jobs(silver_df)
        output_count = deduped_df.count()

        result.output_row_count = output_count
        result.duplicates_removed = input_count - output_count

        # Write.
        _write_silver_table(deduped_df, silver_table, spark)

        logger.info(
            "Silver formatting complete: country=%s date=%s input=%d output=%d "
            "dupes_removed=%d table=%s",
            resolved_country,
            resolved_date,
            input_count,
            output_count,
            result.duplicates_removed,
            silver_table,
        )

    except Exception as exc:
        result.success = False
        result.error = str(exc)
        logger.exception("Silver formatting failed")

    return result


# ---------------------------------------------------------------------------
# Run summary upload
# ---------------------------------------------------------------------------


def upload_run_summary(result: SilverFormatResult) -> None:
    """Write a JSON run summary to the logs directory."""
    import os
    from pathlib import Path

    log_dir = Path(os.environ.get("LOG_DIR", "logs")) / "validation" / "adzuna_silver"
    log_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{result.ingestion_date}_{result.run_id}.json"
    path = log_dir / filename

    with Path.open(path, "w") as fh:
        json.dump(result.summary_dict(), fh, indent=2, default=str)

    logger.info("Silver run summary written: %s", path)
