"""Upload log files to the MinIO/S3 logs bucket.

Best-effort: failures are logged as warnings and never mask the original
job exit code.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


def upload_logfile_to_s3(
    logfile: Path,
    job_name: str,
    run_id: str,
    *,
    bucket: str | None = None,
    endpoint: str | None = None,
    dt: str | None = None,
) -> str | None:
    """Upload *logfile* to the S3 logs bucket.

    Returns the S3 key on success, ``None`` on failure.

    Key layout::

        logs/<job_name>/dt=YYYY-MM-DD/run_id=<run_id>/app.log

    Parameters
    ----------
    logfile:
        Path to the local log file.
    job_name:
        Name of the job that produced the log.
    run_id:
        Unique identifier for this run.
    bucket:
        Override for the logs bucket (defaults to env var
        ``SKILLRADAR_S3_LOGS_BUCKET`` or ``skillradar-logs``).
    endpoint:
        Override for the S3 endpoint (defaults to env var
        ``SKILLRADAR_S3_ENDPOINT`` or ``http://localhost:9000``).
    dt:
        Date partition override (defaults to today UTC).
    """
    if not logfile.exists() or logfile.stat().st_size == 0:
        logger.debug("Skipping log upload: file empty or missing (%s)", logfile)
        return None

    resolved_bucket = bucket or os.environ.get("SKILLRADAR_S3_LOGS_BUCKET", "skillradar-logs")
    resolved_endpoint = endpoint or os.environ.get(
        "SKILLRADAR_S3_ENDPOINT", "http://localhost:9000"
    )
    date_partition = dt or datetime.now(UTC).strftime("%Y-%m-%d")
    key = f"logs/{job_name}/dt={date_partition}/run_id={run_id}/app.log"

    try:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=resolved_endpoint,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-3"),
            use_ssl=resolved_endpoint.startswith("https"),
        )
        client.upload_file(str(logfile), resolved_bucket, key)
        logger.info(
            "Log uploaded → s3://%s/%s",
            resolved_bucket,
            key,
        )
        return key
    except Exception:
        logger.warning(
            "Failed to upload log to s3://%s/%s (best-effort)",
            resolved_bucket,
            key,
            exc_info=True,
        )
        return None
