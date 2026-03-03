"""Logging filter that injects :class:`RunContext` fields into every record.

Attached to the root logger once by :func:`init_logging` so that *all*
handlers (console, file, future sinks) see the same enriched records.
"""

from __future__ import annotations

import logging

from .context import get_context


class ContextFilter(logging.Filter):
    """Inject run-context fields into every :class:`logging.LogRecord`.

    Fields are read from the active :class:`RunContext` stored in a
    :class:`contextvars.ContextVar`.  If no context has been initialised,
    safe defaults are used.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = get_context()
        record.run_id = ctx.run_id  # type: ignore[attr-defined]
        record.job_name = ctx.job_name  # type: ignore[attr-defined]
        record.env = ctx.env  # type: ignore[attr-defined]
        record.dataset = ctx.dataset  # type: ignore[attr-defined]
        record.version = ctx.version  # type: ignore[attr-defined]
        record.lang = ctx.lang  # type: ignore[attr-defined]
        record.git_sha = ctx.git_sha  # type: ignore[attr-defined]
        record.spark_app_id = ctx.spark_app_id  # type: ignore[attr-defined]
        record.host = ctx.host  # type: ignore[attr-defined]
        return True
