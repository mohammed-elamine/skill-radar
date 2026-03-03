"""Logging formatters — human-readable text and structured JSON."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime


class TextFormatter(logging.Formatter):
    """Concise single-line format including run context for the console.

    Sample output::

        2026-03-03T09:12:33Z INFO  esco.intake [run=abc123 job=esco_intake_upload] Validation passed
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        level = f"{record.levelname:<5}"
        name = record.name
        # Context tags (use getattr for safety before filter is attached)
        run_id = getattr(record, "run_id", "")
        job_name = getattr(record, "job_name", "")
        ctx_parts = []
        if run_id:
            ctx_parts.append(f"run={run_id}")
        if job_name:
            ctx_parts.append(f"job={job_name}")
        ctx_str = f" [{' '.join(ctx_parts)}]" if ctx_parts else ""
        msg = record.getMessage()
        line = f"{ts} {level} {name}{ctx_str} {msg}"
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            line = f"{line}\n{record.exc_text}"
        return line


class JsonFormatter(logging.Formatter):
    """Outputs one JSON object per line with all context fields.

    Designed for the file handler so logs are machine-parseable and easily
    queryable by downstream tools.
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=UTC).isoformat()
        payload: dict = {
            "timestamp": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", ""),
            "job_name": getattr(record, "job_name", ""),
            "env": getattr(record, "env", ""),
            "dataset": getattr(record, "dataset", ""),
            "version": getattr(record, "version", ""),
            "lang": getattr(record, "lang", ""),
            "git_sha": getattr(record, "git_sha", ""),
            "spark_app_id": getattr(record, "spark_app_id", ""),
            "host": getattr(record, "host", ""),
        }
        # Strip empty strings to keep JSON lean
        payload = {k: v for k, v in payload.items() if v}
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)
