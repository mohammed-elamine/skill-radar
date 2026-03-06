"""Adzuna Bronze extraction — API → Iceberg raw capture.

This module fetches Adzuna API results for a configured extraction preset
and persists them to a Bronze Iceberg table with full lineage metadata.

Bronze design principles:
- Preserve raw fidelity — every API field is captured in ``raw_payload_json``.
- Append-only — each run adds rows; Bronze never deletes history.
- Duplicate runs on the same day produce duplicate rows in Bronze;
  Silver handles deduplication.
- Partitioned by ``ingestion_date`` and ``country``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from skill_radar.config.adzuna import Settings as AdzunaCredentials
from skill_radar.config.loader import load_platform_config
from skill_radar.domains.adzuna.api.client import AdzunaClient, PageResult
from skill_radar.domains.adzuna.contract import load_adzuna_contract
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.logging.context import get_context

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------


@dataclass
class BronzeRunResult:
    """Outcome of a Bronze extraction run."""

    run_id: str = ""
    country: str = ""
    preset: str = ""
    pages_fetched: int = 0
    rows_ingested: int = 0
    rows_written: int = 0
    target_table: str = ""
    request_log_table: str = ""
    request_log_rows: int = 0
    extracted_at_utc: str = ""
    ingestion_date: str = ""
    success: bool = True
    error: str = ""

    def summary_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Row mapping — API JSON → Bronze flat row
# ---------------------------------------------------------------------------


def _map_job_to_bronze_row(
    job: dict[str, Any],
    *,
    source_system: str,
    country: str,
    preset: str,
    search_key: str,
    search_params_json: str,
    page: int,
    results_per_page: int,
    position: int,
    extracted_at_utc: str,
    ingestion_date: str,
    run_id: str,
) -> dict[str, Any]:
    """Map a single Adzuna API job object to a Bronze row dict."""
    location = job.get("location", {}) or {}
    category = job.get("category", {}) or {}
    company = job.get("company", {}) or {}

    return {
        "job_id": str(job.get("id", "")),
        "adref": str(job.get("adref", "")),
        "title": job.get("title", ""),
        "description": job.get("description", ""),
        "created_at_raw": job.get("created", ""),
        "redirect_url": job.get("redirect_url", ""),
        "latitude": _safe_float(job.get("latitude")),
        "longitude": _safe_float(job.get("longitude")),
        "salary_min": _safe_float(job.get("salary_min")),
        "salary_max": _safe_float(job.get("salary_max")),
        "salary_is_predicted_raw": str(job.get("salary_is_predicted", "")),
        "contract_time_raw": job.get("contract_time", ""),
        "contract_type_raw": job.get("contract_type", ""),
        "location_display_name": location.get("display_name", ""),
        "location_area_json": json.dumps(location.get("area", []), ensure_ascii=False),
        "category_tag": category.get("tag", ""),
        "category_label": category.get("label", ""),
        "company_display_name": company.get("display_name", ""),
        "company_canonical_name": company.get("canonical_name", ""),
        "raw_payload_json": json.dumps(job, ensure_ascii=False),
        # Extraction metadata
        "source_system": source_system,
        "country": country,
        "preset": preset,
        "search_key": search_key,
        "search_params_json": search_params_json,
        "page": page,
        "results_per_page": results_per_page,
        "api_result_position": position,
        "extracted_at_utc": extracted_at_utc,
        "ingestion_date": ingestion_date,
        "run_id": run_id,
    }


def _map_page_to_request_log_row(
    page_result: PageResult,
    *,
    source_system: str,
    country: str,
    preset: str,
    run_id: str,
    ingestion_date: str,
) -> dict[str, Any]:
    """Map a PageResult to a request-log Bronze row."""
    return {
        "source_system": source_system,
        "country": country,
        "preset": preset,
        "page": page_result.page,
        "request_params_json": json.dumps(page_result.request_params, ensure_ascii=False),
        "response_count": len(page_result.results),
        "http_status": page_result.http_status,
        "request_started_at_utc": page_result.request_started_at_utc,
        "request_finished_at_utc": page_result.request_finished_at_utc,
        "duration_ms": page_result.duration_ms,
        "success": page_result.success,
        "error_message": page_result.error_message,
        "run_id": run_id,
        "ingestion_date": ingestion_date,
    }


def _safe_float(value: Any) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Iceberg write helpers
# ---------------------------------------------------------------------------


def _ensure_namespace(spark: SparkSession, layout: LakeLayout, layer: str) -> None:
    """Create the Iceberg namespace if it does not exist."""
    fqn = layout.iceberg_namespace(layer)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fqn}")
    logger.info("Ensured Iceberg namespace: %s", fqn)


def _write_bronze_append(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    table_fqn: str,
    partition_cols: list[str],
) -> int:
    """Write rows to a Bronze Iceberg table in append mode.

    Creates the table if it does not exist; appends otherwise.
    Returns the number of rows written.
    """
    if not rows:
        return 0

    df = spark.createDataFrame(rows)

    if not spark.catalog.tableExists(table_fqn):
        logger.info("Creating Iceberg table: %s", table_fqn)
        builder = (
            df.writeTo(table_fqn)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
        )
        for col in partition_cols:
            builder = builder.partitionedBy(col)  # type: ignore[arg-type]
        builder.create()
    else:
        logger.info("Appending to Iceberg table: %s (%d rows)", table_fqn, len(rows))
        df.writeTo(table_fqn).append()

    return len(rows)


# ---------------------------------------------------------------------------
# Main extraction orchestrator
# ---------------------------------------------------------------------------


def run_bronze_extraction(
    spark: SparkSession,
    *,
    preset: str | None = None,
    country: str | None = None,
    max_pages: int | None = None,
    results_per_page: int | None = None,
    run_id: str | None = None,
    config: PlatformSettings | None = None,
) -> BronzeRunResult:
    """Execute a Bronze extraction run for Adzuna jobs.

    Fetches Adzuna API results for the given preset/country scope and
    writes them to the Bronze Iceberg table. Also writes a request-log
    table for observability.

    Parameters
    ----------
    spark:
        Active SparkSession.
    preset:
        Extraction preset name (from contract). Defaults to config value.
    country:
        Country code override. Defaults to preset or config value.
    max_pages:
        Maximum pages to fetch. Defaults to config value.
    results_per_page:
        Results per page. Defaults to config value.
    run_id:
        Run identifier for lineage. Defaults from logging context.
    config:
        Platform configuration. Loaded from defaults if not provided.

    Returns
    -------
    BronzeRunResult
        Structured result with metrics and lineage metadata.
    """
    cfg = config or load_platform_config()
    contract = load_adzuna_contract()
    layout = LakeLayout(cfg)
    creds = AdzunaCredentials.from_env()

    # Resolve parameters.
    resolved_preset = preset or cfg.adzuna.default_preset
    preset_config = contract.get_preset(resolved_preset)
    resolved_country = country or preset_config.country or cfg.adzuna.default_country
    resolved_max_pages = max_pages or cfg.adzuna.max_pages_per_run
    resolved_rpp = results_per_page or cfg.adzuna.results_per_page

    # Resolve run metadata.
    now_utc = datetime.now(UTC)
    extracted_at_utc = now_utc.isoformat()
    ingestion_date = now_utc.strftime("%Y-%m-%d")

    try:
        ctx = get_context()
        resolved_run_id = run_id or ctx.run_id
    except RuntimeError:
        import uuid

        resolved_run_id = run_id or uuid.uuid4().hex[:12]

    result = BronzeRunResult(
        run_id=resolved_run_id,
        country=resolved_country,
        preset=resolved_preset,
        extracted_at_utc=extracted_at_utc,
        ingestion_date=ingestion_date,
    )

    # Build API client.
    client = AdzunaClient(
        app_id=creds.adzuna_app_id,
        app_key=creds.adzuna_app_key,
        base_url=cfg.adzuna.base_url,
        results_per_page=resolved_rpp,
        timeout_seconds=cfg.adzuna.request_timeout_seconds,
        max_retries=cfg.adzuna.max_retries,
        backoff_seconds=cfg.adzuna.backoff_seconds,
    )

    logger.info(
        "Starting Bronze extraction: country=%s preset=%s max_pages=%d rpp=%d run_id=%s",
        resolved_country,
        resolved_preset,
        resolved_max_pages,
        resolved_rpp,
        resolved_run_id,
    )

    try:
        # Ensure namespace exists.
        _ensure_namespace(spark, layout, "bronze")

        # Fetch all pages.
        search_result = client.search_all(
            resolved_country,
            resolved_preset,
            max_pages=resolved_max_pages,
            what=preset_config.what,
            where=preset_config.where,
            category=preset_config.category,
            max_days_old=preset_config.max_days_old,
            sort_by=preset_config.sort_by,
            full_time=preset_config.full_time,
            part_time=preset_config.part_time,
            salary_min=preset_config.salary_min,
            salary_max=preset_config.salary_max,
        )

        if not search_result.success:
            result.success = False
            result.error = search_result.error_message
            return result

        result.pages_fetched = search_result.pages_fetched

        # Map API results to Bronze rows.
        bronze_rows: list[dict[str, Any]] = []
        for page_result in search_result.page_results:
            search_params_json = json.dumps(page_result.request_params, ensure_ascii=False)
            for idx, job in enumerate(page_result.results):
                bronze_rows.append(
                    _map_job_to_bronze_row(
                        job,
                        source_system="adzuna",
                        country=resolved_country,
                        preset=resolved_preset,
                        search_key=search_result.search_key,
                        search_params_json=search_params_json,
                        page=page_result.page,
                        results_per_page=resolved_rpp,
                        position=idx + 1,
                        extracted_at_utc=extracted_at_utc,
                        ingestion_date=ingestion_date,
                        run_id=resolved_run_id,
                    )
                )

        result.rows_ingested = len(bronze_rows)

        # Write Bronze jobs raw table.
        jobs_table = layout.adzuna_bronze_jobs_raw_fqn()
        result.target_table = jobs_table
        result.rows_written = _write_bronze_append(
            spark,
            bronze_rows,
            jobs_table,
            partition_cols=["ingestion_date", "country"],
        )

        # Write request-log table.
        log_rows = [
            _map_page_to_request_log_row(
                pr,
                source_system="adzuna",
                country=resolved_country,
                preset=resolved_preset,
                run_id=resolved_run_id,
                ingestion_date=ingestion_date,
            )
            for pr in search_result.page_results
        ]
        log_table = layout.adzuna_bronze_request_log_fqn()
        result.request_log_table = log_table
        result.request_log_rows = _write_bronze_append(
            spark,
            log_rows,
            log_table,
            partition_cols=["ingestion_date", "country"],
        )

        logger.info(
            "Bronze extraction complete: country=%s preset=%s pages=%d "
            "rows_written=%d request_log_rows=%d table=%s",
            resolved_country,
            resolved_preset,
            result.pages_fetched,
            result.rows_written,
            result.request_log_rows,
            jobs_table,
        )

    except Exception as exc:
        result.success = False
        result.error = str(exc)
        logger.exception("Bronze extraction failed")

    return result


# ---------------------------------------------------------------------------
# Run summary upload (mirrors ESCO pattern)
# ---------------------------------------------------------------------------


def upload_run_summary(result: BronzeRunResult) -> None:
    """Write a JSON run summary to the logs directory.

    Follows the same pattern as ESCO Bronze run summaries for
    consistency and future S3 upload.
    """
    import os
    from pathlib import Path

    log_dir = Path(os.environ.get("LOG_DIR", "logs")) / "validation" / "adzuna_bronze"
    log_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{result.ingestion_date}_{result.run_id}.json"
    path = log_dir / filename

    with Path.open(path, "w") as fh:
        json.dump(result.summary_dict(), fh, indent=2, default=str)

    logger.info("Bronze run summary written: %s", path)
