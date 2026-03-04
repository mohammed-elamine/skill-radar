"""Run-context management via :mod:`contextvars`.

Every log record emitted after :func:`init_logging` is enriched with the
fields stored in the active :class:`RunContext`.  Mid-run code can call
:func:`set_context` to add domain-specific fields (e.g. dataset, version,
lang) that were not yet known at process start.
"""

from __future__ import annotations

import os
import socket
import uuid
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class RunContext:
    """Immutable snapshot of the current run's metadata.

    Fields are added progressively during a run via :func:`set_context`.
    """

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    job_name: str = "unknown"
    env: str = "local"
    started_at_utc: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    # Domain-specific (set later via set_context)
    dataset: str = ""
    version: str = ""
    lang: str = ""
    dt: str = ""
    # Infrastructure
    git_sha: str = ""
    spark_app_id: str = ""
    host: str = field(default_factory=socket.gethostname)
    # Derived at init_logging
    logfile: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return only non-empty fields as a dict for log record injection."""
        return {k: v for k, v in asdict(self).items() if v}


# ---------------------------------------------------------------------------
# Module-level context variable
# ---------------------------------------------------------------------------

_current_context: ContextVar[RunContext | None] = ContextVar(
    "skill_radar_run_context",
    default=None,
)


def create_initial_context(
    job_name: str,
    *,
    env: str | None = None,
    git_sha: str | None = None,
) -> RunContext:
    """Build and activate the initial context for a run."""
    ctx = RunContext(
        job_name=job_name,
        env=env or os.environ.get("SKILLRADAR_ENV", "local"),
        git_sha=git_sha or os.environ.get("GIT_SHA", ""),
    )
    _current_context.set(ctx)
    return ctx


def get_context() -> RunContext:
    """Return the active run context.

    Raises
    ------
    RuntimeError
        If no context has been created via :func:`create_initial_context`.
    """
    ctx = _current_context.get()
    if ctx is None:
        raise RuntimeError(
            "RunContext not initialized. Call init_logging(job_name=...) in your entrypoint."
        )
    return ctx


def set_context(**fields: Any) -> RunContext:
    """Merge *fields* into the active context and return the updated copy.

    Unknown field names are silently ignored so callers don't break when the
    schema evolves.
    """
    current = _current_context.get()
    if current is None:
        raise RuntimeError("RunContext not initialized")

    known = set(current.__dataclass_fields__)
    updates = {k: v for k, v in fields.items() if k in known and v}
    if not updates:
        return current

    new_ctx = RunContext(**{**asdict(current), **updates})
    _current_context.set(new_ctx)
    return new_ctx


def reset_context() -> None:
    """Reset context to default (mainly for test isolation)."""
    _current_context.set(None)
