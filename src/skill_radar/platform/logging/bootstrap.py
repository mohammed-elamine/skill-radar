"""Logging bootstrap — single-point initialisation and teardown.

Call :func:`init_logging` **once** at every process entry-point (CLI command,
Spark driver ``main()``, smoke test).  All other modules simply use
``logging.getLogger(__name__)``.

Call :func:`finalize_logging` at process exit to flush handlers and
(optionally) upload the log file to the MinIO logs bucket.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from .context import RunContext, create_initial_context, get_context, reset_context
from .filters import ContextFilter
from .formatters import JsonFormatter, TextFormatter
from .upload import upload_logfile_to_s3

_INITIALIZED = False
_LOGFILE_PATH: Path | None = None

# Sentinel name used as the root-handler marker so we can detect re-init.
_HANDLER_MARKER = "skill_radar_platform_logging"


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


def init_logging(
    job_name: str,
    *,
    log_dir: str | None = None,
    level: str | None = None,
    console_format: str | None = None,
    file_format: str | None = None,
    enable_file: bool = True,
    verbose: bool = False,
    env: str | None = None,
    git_sha: str | None = None,
) -> RunContext:
    """Configure Python logging for this process (idempotent, call once)."""
    global _INITIALIZED, _LOGFILE_PATH

    if _INITIALIZED:
        return get_context()

    # 1. Create run context -----------------------------------------------
    ctx = create_initial_context(job_name, env=env, git_sha=git_sha)

    # 2. Resolve parameters from env / defaults ---------------------------
    resolved_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    if resolved_level not in logging.getLevelNamesMapping():
        raise ValueError(f"Invalid log level: {resolved_level}")
    resolved_log_dir = Path(log_dir or os.environ.get("LOG_DIR", "logs"))
    resolved_console_fmt = console_format or os.environ.get("LOG_FORMAT", "text")
    resolved_file_fmt = file_format or "json"

    root = logging.getLogger()
    root.setLevel(resolved_level)

    # 3. Remove any pre-existing handlers (avoid duplicates) --------------
    root.handlers.clear()

    # 4. Create context-injecting filter ----------------------------------
    #    Attached to each *handler* (not to the root logger) so that
    #    records propagated from child loggers are also enriched.
    ctx_filter = ContextFilter()
    ctx_filter.name = _HANDLER_MARKER

    # 5. Console handler --------------------------------------------------
    console = logging.StreamHandler()
    console_level = (
        logging.DEBUG if verbose else logging.getLevelNamesMapping().get(resolved_level, None)
    )
    if console_level is None:
        raise ValueError(f"Invalid log level: {resolved_level}")
    console.setLevel(console_level)
    if resolved_console_fmt == "json":
        console.setFormatter(JsonFormatter())
    else:
        console.setFormatter(TextFormatter())
    console.addFilter(ctx_filter)
    root.addHandler(console)

    # 6. File handler (optional) ------------------------------------------
    if enable_file:
        resolved_log_dir.mkdir(parents=True, exist_ok=True)
        ts_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"{job_name}.{ts_str}.{ctx.run_id}.log"
        _LOGFILE_PATH = resolved_log_dir / filename
        file_handler = logging.FileHandler(str(_LOGFILE_PATH), encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)  # capture everything in file
        if resolved_file_fmt == "text":
            file_handler.setFormatter(TextFormatter())
        else:
            file_handler.setFormatter(JsonFormatter())
        file_handler.addFilter(ctx_filter)
        root.addHandler(file_handler)
        # Store logfile path in context for later retrieval
        from .context import set_context

        set_context(logfile=str(_LOGFILE_PATH))

    _INITIALIZED = True
    logging.getLogger(__name__).debug(
        "Logging initialised: job=%s run_id=%s level=%s file=%s",
        job_name,
        ctx.run_id,
        resolved_level,
        _LOGFILE_PATH or "(disabled)",
    )
    return ctx


def finalize_logging(*, upload: bool | None = None) -> str | None:
    """Flush handlers and optionally upload the log file to S3.

    Parameters
    ----------
    upload:
        - ``True``: upload log file to the S3 logs bucket.
        - ``False``: skip upload.
        - ``None`` (default): consult ``$LOG_UPLOAD`` env var
          (defaults to ``false``).

    Returns
    -------
    str | None
        The S3 key if upload succeeded, otherwise ``None``.
    """
    global _INITIALIZED, _LOGFILE_PATH

    # Flush all handlers
    root = logging.getLogger()
    for handler in root.handlers:
        handler.flush()

    s3_key: str | None = None

    # Decide whether to upload
    should_upload = upload if upload is not None else _is_truthy(os.environ.get("LOG_UPLOAD"))

    if should_upload and _LOGFILE_PATH is not None:
        ctx = get_context()
        s3_key = upload_logfile_to_s3(
            _LOGFILE_PATH,
            ctx.job_name,
            ctx.run_id,
        )

    # Clean up state for potential re-init (mainly tests)
    _INITIALIZED = False
    _LOGFILE_PATH = None
    reset_context()

    return s3_key
