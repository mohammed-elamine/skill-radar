"""Validation report sinks: write reports to local filesystem and S3.

This module provides:
- :func:`write_report_local`: Write JSON report to local logs directory
- :func:`write_report_s3`: Upload report to MinIO logs bucket
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from skill_radar.platform.runtime import get_runtime_context, resolve_s3_endpoint

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

    from .models import ValidationReport

logger = logging.getLogger(__name__)


def _report_filename(report: ValidationReport) -> str:
    """Generate a filename for the report.

    Format: <timestamp>.<run_id>.json
    """
    # Use started_at_utc timestamp in compact format
    try:
        dt = datetime.fromisoformat(report.started_at_utc.replace("Z", "+00:00"))
        ts = dt.strftime("%Y%m%d_%H%M%S")
    except (ValueError, AttributeError):
        ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    return f"{ts}.{report.run_id}.json"


def write_report_local(
    report: ValidationReport,
    *,
    log_dir: str | Path | None = None,
) -> Path:
    """Write validation report to local filesystem.

    Reports are written to:
        <log_dir>/validation/<validator_name>/<timestamp>.<run_id>.json

    Parameters
    ----------
    report:
        The validation report to write.
    log_dir:
        Base log directory (defaults to $LOG_DIR or "logs").

    Returns
    -------
    Path:
        The full path to the written report file.
    """
    resolved_dir = Path(log_dir or os.environ.get("LOG_DIR", "logs"))
    report_dir = resolved_dir / "validation" / report.validator_name
    report_dir.mkdir(parents=True, exist_ok=True)

    filename = _report_filename(report)
    report_path = report_dir / filename

    # Store the local path in artifacts
    report.artifacts["local_report_path"] = str(report_path)

    # Write JSON
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report.as_dict(), f, indent=2)

    logger.info("Wrote validation report to %s", report_path)
    return report_path


def write_report_s3(
    report: ValidationReport,
    *,
    config: PlatformSettings | None = None,
    bucket: str | None = None,
) -> str | None:
    """Upload validation report to S3 logs bucket.

    Reports are uploaded to:
        s3://<logs_bucket>/validation/<validator_name>/<timestamp>.<run_id>.json

    Parameters
    ----------
    report:
        The validation report to upload.
    config:
        Platform configuration (loads defaults if not provided).
    bucket:
        Override bucket name (defaults to config.logging.logs_bucket).

    Returns
    -------
    str | None:
        The S3 key if upload succeeded, None if skipped/failed.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        logger.warning("boto3 not available, skipping S3 report upload")
        return None

    # Load config if not provided
    if config is None:
        try:
            from skill_radar.config.loader import load_platform_config

            config = load_platform_config()
        except Exception:
            logger.warning("Could not load platform config, skipping S3 upload")
            return None

    logs_bucket = bucket or config.logging.logs_bucket
    ctx = get_runtime_context()
    endpoint = resolve_s3_endpoint(config.storage.s3, ctx)

    # Build S3 key
    filename = _report_filename(report)
    s3_key = f"validation/{report.validator_name}/{filename}"

    try:
        client = boto3.client("s3", endpoint_url=endpoint)
        report_json = json.dumps(report.as_dict(), indent=2)

        client.put_object(
            Bucket=logs_bucket,
            Key=s3_key,
            Body=report_json.encode("utf-8"),
            ContentType="application/json",
        )

        report.artifacts["s3_report_key"] = s3_key
        report.artifacts["s3_report_bucket"] = logs_bucket

        logger.info("Uploaded validation report to s3://%s/%s", logs_bucket, s3_key)
        return s3_key

    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "NoSuchBucket":
            logger.warning("Logs bucket %s does not exist, skipping S3 upload", logs_bucket)
        else:
            logger.warning("Failed to upload report to S3: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Unexpected error uploading report to S3: %s", exc)
        return None


def finalize_report(
    report: ValidationReport,
    *,
    config: PlatformSettings | None = None,
    log_dir: str | Path | None = None,
    upload_s3: bool | None = None,
) -> tuple[Path, str | None]:
    """Write report locally and optionally upload to S3.

    Parameters
    ----------
    report:
        The validation report to finalize.
    config:
        Platform configuration (loads defaults if not provided).
    log_dir:
        Base log directory.
    upload_s3:
        - True: upload to S3
        - False: skip S3
        - None: decide based on LOG_UPLOAD env var

    Returns
    -------
    tuple[Path, str | None]:
        Tuple of (local_path, s3_key or None).
    """
    local_path = write_report_local(report, log_dir=log_dir)

    # Decide S3 upload
    if upload_s3 is None:
        upload_s3 = os.environ.get("LOG_UPLOAD", "").lower() in {"1", "true", "yes"}

    s3_key = None
    if upload_s3:
        s3_key = write_report_s3(report, config=config)

    return local_path, s3_key
