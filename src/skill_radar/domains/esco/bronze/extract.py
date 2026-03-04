"""ESCO Bronze extraction — contract-driven CSV → Iceberg pipeline.

This module contains the main extraction logic.  It is imported by the Spark
entrypoint (``jobs/esco/bronze_esco_to_iceberg.py``) and by the CLI.

Responsibilities
----------------
1. Resolve landing artifact keys via :class:`LakeLayout`.
2. Download & extract CSVs from the landed ESCO ZIP.
3. **Upload extracted CSVs to S3 staging** so Spark reads from ``s3a://``
   (distributed-safe).
4. Validate CSV columns and allowed values against the ESCO contract.
5. Rename columns to snake_case with Bronze conventions (contract-driven).
6. Add lineage columns (dataset, entity, version, lang, run_id, …).
7. Write to Iceberg Bronze tables with partition-level idempotency.

The module has **no** hardcoded bucket names, paths, or table names — it
delegates everything to platform agents (:class:`LakeLayout`).
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from skill_radar.config.loader import load_platform_config
from skill_radar.domains.esco.contract import load_esco_contract
from skill_radar.platform.lake.enums import Domain, Source
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.logging.context import get_context

from .errors import BronzeValidationError
from .iceberg import ensure_bronze_namespace, write_bronze_table
from .schema_mapping import (
    count_column_name,
    newline_raw_fields,
    norm_column_name,
    rename_mapping,
)
from .validation import extract_csv_from_zip, validate_required_columns

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings
    from skill_radar.domains.esco.contract.models import ContractEntity, EscoContract

logger = logging.getLogger(__name__)


# ── Result dataclass ──────────────────────────────────────────────────────


@dataclass
class BronzeEntityResult:
    """Per-entity extraction outcome."""

    entity: str
    table: str = ""
    row_count: int = 0
    status: str = "pending"
    error: str = ""


@dataclass
class BronzeRunResult:
    """Full run outcome."""

    run_id: str = ""
    version: str = ""
    lang: str = ""
    entities: list[BronzeEntityResult] = field(default_factory=list)
    success: bool = True

    def summary_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "version": self.version,
            "lang": self.lang,
            "success": self.success,
            "entities": [asdict(e) for e in self.entities],
        }


# ── Validation helpers ────────────────────────────────────────────────────


def _validate_allowed_values(
    df: DataFrame,
    contract_entity: ContractEntity,
) -> None:
    """Raise if any column value violates the contract's allowed_values."""
    for spec in contract_entity.required_columns:
        if spec.allowed_values is None:
            continue

        allowed = set(spec.allowed_values)
        invalid_df = (
            df.select(spec.name)
            .where(~F.col(spec.name).isin(list(allowed)))
            .where(F.col(spec.name).isNotNull())
            .groupBy(spec.name)
            .count()
        )
        invalid_rows = invalid_df.collect()
        if invalid_rows:
            counts = {row[spec.name]: row["count"] for row in invalid_rows}
            raise BronzeValidationError(
                f"Entity '{contract_entity.name}', column '{spec.name}': "
                f"found {sum(counts.values())} rows with invalid values. "
                f"Allowed: {sorted(allowed)}. Invalid value counts: {counts}"
            )


# ── CSV reading ───────────────────────────────────────────────────────────


def _read_csv_spark(
    spark: SparkSession,
    csv_s3a_path: str,
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame from an s3a:// path."""
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(csv_s3a_path)
    )


# ── Column transforms ────────────────────────────────────────────────────


def _apply_renames(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Rename columns according to vendor → bronze mapping."""
    for vendor_col, bronze_col in mapping.items():
        if vendor_col in df.columns:
            df = df.withColumnRenamed(vendor_col, bronze_col)
    return df


def _add_newline_helpers(
    df: DataFrame,
    raw_fields: tuple[str, ...],
) -> DataFrame:
    """Add ``_norm`` and ``_count`` columns for newline-separated fields."""
    for raw_col in raw_fields:
        if raw_col not in df.columns:
            continue
        ncol = norm_column_name(raw_col)
        ccol = count_column_name(raw_col)

        # Normalise \r\n and \r to \n
        df = df.withColumn(
            ncol,
            F.regexp_replace(
                F.regexp_replace(F.col(raw_col), r"\r\n", "\n"),
                r"\r",
                "\n",
            ),
        )
        # Count labels (handle null/empty)
        df = df.withColumn(
            ccol,
            F.when(
                F.col(ncol).isNull() | (F.trim(F.col(ncol)) == F.lit("")),
                F.lit(0),
            ).otherwise(
                F.size(F.split(F.col(ncol), "\n")),
            ),
        )
    return df


def _add_lineage_columns(
    df: DataFrame,
    *,
    dataset: str,
    entity: str,
    version: str,
    lang: str,
    source_zip_key: str,
    manifest_key: str,
    artifact_sha256: str,
    run_id: str,
) -> DataFrame:
    """Append lineage columns to the DataFrame."""
    return (
        df.withColumn("dataset", F.lit(dataset))
        .withColumn("entity", F.lit(entity))
        .withColumn("version", F.lit(version))
        .withColumn("lang", F.lit(lang))
        .withColumn("source_zip_key", F.lit(source_zip_key))
        .withColumn("manifest_key", F.lit(manifest_key))
        .withColumn("artifact_sha256", F.lit(artifact_sha256))
        .withColumn("ingested_at_utc", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )


# ── Manifest loading helper ───────────────────────────────────────────────


def _load_manifest_sha256(
    manifest_key: str,
    s3_endpoint: str,
    s3_bucket: str,
) -> str | None:
    """Load landed manifest JSON and return ``artifact.sha256`` if present.

    Uses boto3 directly (inside the Spark driver, not Spark I/O).
    Returns ``None`` if the manifest is missing or unreadable.
    """
    import boto3

    try:
        client = boto3.client("s3", endpoint_url=s3_endpoint)
        resp = client.get_object(Bucket=s3_bucket, Key=manifest_key)
        manifest = json.loads(resp["Body"].read().decode("utf-8"))
        return str(manifest.get("artifact", {}).get("sha256"))
    except Exception:
        logger.warning(
            "Could not load manifest at s3://%s/%s — will compute SHA-256 from ZIP",
            s3_bucket,
            manifest_key,
        )
        return None


# ── S3 staging helpers ────────────────────────────────────────────────────


def _upload_csv_to_staging(
    local_csv: Path,
    staging_key: str,
    endpoint: str,
    bucket: str,
) -> None:
    """Upload a local CSV file to S3 staging area."""
    import boto3

    client = boto3.client("s3", endpoint_url=endpoint)
    client.upload_file(str(local_csv), bucket, staging_key)
    logger.info("Staged CSV → s3://%s/%s", bucket, staging_key)


def _download_zip_from_s3(
    local_path: Path,
    key: str,
    endpoint: str,
    bucket: str,
) -> None:
    """Download an S3 object to a local file (Spark driver only)."""
    import boto3

    client = boto3.client("s3", endpoint_url=endpoint)
    client.download_file(bucket, key, str(local_path))
    logger.info("Downloaded s3://%s/%s → %s", bucket, key, local_path)


# ── Main extraction function ──────────────────────────────────────────────


def run_bronze_extraction(
    spark: SparkSession,
    version: str,
    lang: str,
    *,
    entities: list[str] | None = None,
    fail_fast: bool = True,
    dry_run: bool = False,
    config: PlatformSettings | None = None,
    contract: EscoContract | None = None,
    run_id: str | None = None,
    cleanup_staging: bool = False,
) -> BronzeRunResult:
    """Execute the ESCO Bronze extraction pipeline.

    Parameters
    ----------
    spark:
        Active SparkSession (with Iceberg catalog ``sr`` configured).
    version:
        ESCO artifact version (e.g. ``v1.2.0``).
    lang:
        Language code (e.g. ``fr``).
    entities:
        Subset of entities to process.  ``None`` means all contract entities.
    fail_fast:
        Abort on the first entity error instead of continuing.
    dry_run:
        Validate and display the extraction plan without writing to Iceberg.
    config:
        Optional pre-loaded platform config.
    contract:
        Optional pre-loaded ESCO contract.
    run_id:
        Optional run ID override (otherwise read from logging context).
    cleanup_staging:
        Delete staged CSVs from S3 after successful extraction.
    """
    cfg = config or load_platform_config()
    esco = contract or load_esco_contract()
    layout = LakeLayout(cfg)

    # Resolve run_id from logging context (requires init_logging)
    ctx = get_context()
    resolved_run_id = run_id or ctx.run_id

    result = BronzeRunResult(
        run_id=resolved_run_id,
        version=version,
        lang=lang,
    )

    # ── 1. Resolve landing keys via LakeLayout ───────────────────────────
    domain = Domain.TAXONOMY.value
    source = Source.ESCO.value
    zip_key = layout.landing_zip_key(domain, source, version, lang)
    manifest_key = layout.landing_manifest_key(domain, source, version, lang)
    s3_bucket = cfg.storage.s3.bucket
    s3_endpoint = cfg.storage.s3.endpoint

    logger.info("Landing artifact: s3://%s/%s", s3_bucket, zip_key)

    # ── 2. Load manifest SHA-256 ─────────────────────────────────────────
    artifact_sha256 = _load_manifest_sha256(manifest_key, s3_endpoint, s3_bucket)
    if artifact_sha256:
        logger.info("Artifact SHA-256 (from manifest): %s", artifact_sha256)
    else:
        logger.warning("SHA-256 not available from manifest — will compute after download")

    # ── 3. Download ZIP from S3 ──────────────────────────────────────────
    tmp_root = Path(tempfile.mkdtemp(prefix="esco_bronze_"))
    staging_keys: list[str] = []

    try:
        local_zip = tmp_root / "esco.zip"
        logger.info("Downloading ZIP to %s", local_zip)
        _download_zip_from_s3(local_zip, zip_key, s3_endpoint, s3_bucket)

        # Compute SHA-256 fallback
        if artifact_sha256 is None:
            from skill_radar.utils.hashing import sha256_file

            artifact_sha256 = sha256_file(local_zip)
            logger.info("Computed artifact SHA-256: %s", artifact_sha256)

        # ── 4. Determine which entities to process ───────────────────────
        contract_entities = {e.name: e for e in esco.entities}
        target_names = entities if entities else list(contract_entities.keys())

        for name in target_names:
            if name not in contract_entities:
                raise BronzeValidationError(
                    f"Entity '{name}' not found in contract. "
                    f"Available: {list(contract_entities.keys())}"
                )

        # ── 4b. Compute staging prefix ───────────────────────────────────
        staging_prefix = layout.bronze_staging_prefix(
            domain,
            source,
            version,
            lang,
            resolved_run_id,
        )

        if dry_run:
            logger.info("[DRY-RUN] Extraction plan:")
            for name in target_names:
                ce = contract_entities[name]
                csv_fn = ce.filename_pattern.replace("{lang}", lang)
                tbl = layout.iceberg_table_fqn("bronze", "esco", name)
                logger.info("  entity=%s  csv=%s  table=%s", name, csv_fn, tbl)
            result.success = True
            return result

        # ── 5. Ensure Iceberg namespace ──────────────────────────────────
        ensure_bronze_namespace(spark)

        # ── 6. Process each entity ───────────────────────────────────────
        for entity_name in target_names:
            er = BronzeEntityResult(entity=entity_name)
            result.entities.append(er)
            ce = contract_entities[entity_name]
            csv_filename = ce.filename_pattern.replace("{lang}", lang)
            table_fqn = layout.iceberg_table_fqn("bronze", "esco", entity_name)
            er.table = table_fqn

            try:
                logger.info(
                    "Processing entity=%s  csv=%s  table=%s",
                    entity_name,
                    csv_filename,
                    table_fqn,
                )

                # a) Extract CSV from ZIP to local temp
                csv_path = extract_csv_from_zip(local_zip, csv_filename, tmp_root)

                # b) Upload CSV to S3 staging (distributed-safe)
                staging_key = f"{staging_prefix}/entity={entity_name}/{csv_filename}"
                _upload_csv_to_staging(csv_path, staging_key, s3_endpoint, s3_bucket)
                staging_keys.append(staging_key)

                # c) Read CSV from S3A staging path with Spark
                s3a_path = f"s3a://{s3_bucket}/{staging_key}"
                df = _read_csv_spark(spark, s3a_path)
                logger.info(
                    "Read %d rows, %d columns from %s",
                    df.count(),
                    len(df.columns),
                    s3a_path,
                )

                # d) Validate required columns
                validate_required_columns(df.columns, ce)

                # e) Validate allowed values
                _validate_allowed_values(df, ce)

                # f) Rename columns (contract-driven)
                col_map = rename_mapping(ce, df.columns)
                logger.info("Column mapping: %s", col_map)
                df = _apply_renames(df, col_map)

                # g) Add newline helpers (contract-driven)
                raw_fields = newline_raw_fields(ce)
                if ce.derived_newline_helpers and raw_fields:
                    df = _add_newline_helpers(df, raw_fields)

                # h) Add lineage columns
                df = _add_lineage_columns(
                    df,
                    dataset="esco",
                    entity=entity_name,
                    version=version,
                    lang=lang,
                    source_zip_key=zip_key,
                    manifest_key=manifest_key,
                    artifact_sha256=artifact_sha256 or "",
                    run_id=resolved_run_id,
                )

                # i) Write to Iceberg
                row_count = df.count()
                write_bronze_table(df, table_fqn, spark)

                er.row_count = row_count
                er.status = "success"
                logger.info(
                    "Wrote %d rows to %s (version=%s, lang=%s)",
                    row_count,
                    table_fqn,
                    version,
                    lang,
                )

            except BronzeValidationError as exc:
                er.status = "failed"
                er.error = str(exc)
                result.success = False
                logger.error("Validation error for %s: %s", entity_name, exc)
                if fail_fast:
                    break

            except Exception as exc:
                er.status = "failed"
                er.error = str(exc)
                result.success = False
                logger.exception("Unexpected error processing %s", entity_name)
                if fail_fast:
                    break

    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

        # Optional: clean up S3 staging
        if cleanup_staging and staging_keys:
            _cleanup_staging(staging_keys, s3_endpoint, s3_bucket)

    return result


def _cleanup_staging(keys: list[str], endpoint: str, bucket: str) -> None:
    """Delete staged CSV files from S3 after successful extraction."""
    import boto3

    try:
        client = boto3.client("s3", endpoint_url=endpoint)
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
        logger.info("Cleaned up %d staged files", len(keys))
    except Exception:
        logger.warning("Failed to clean up staging files", exc_info=True)


# ── Run summary upload ────────────────────────────────────────────────────


def upload_run_summary(
    run_result: BronzeRunResult,
    *,
    config: PlatformSettings | None = None,
) -> str | None:
    """Upload a small JSON run summary to the logs bucket.

    Key layout::

        runs/esco_bronze_extract/dt=YYYY-MM-DD/run_id=<run_id>/summary.json

    Returns the S3 key on success, ``None`` on failure.
    """
    import boto3

    cfg = config or load_platform_config()
    logs_bucket = cfg.logging.logs_bucket
    dt = datetime.now(UTC).strftime("%Y-%m-%d")
    key = f"runs/esco_bronze_extract/dt={dt}/run_id={run_result.run_id}/summary.json"

    try:
        body = json.dumps(run_result.summary_dict(), indent=2, default=str)
        s3_endpoint = cfg.storage.s3.endpoint
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
