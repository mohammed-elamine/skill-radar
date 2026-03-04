"""Tests for ContextFilter.

The filter always requires an initialised ``RunContext``.  A ``RuntimeError``
is raised if the context is missing — every job entrypoint must call
:func:`init_logging` before emitting any log record.
"""

from __future__ import annotations

import logging

import pytest

from skill_radar.platform.logging.context import (
    create_initial_context,
    reset_context,
)
from skill_radar.platform.logging.filters import ContextFilter


@pytest.fixture(autouse=True)
def _clean_context():
    """Reset run context after every test."""
    yield
    reset_context()


def _make_record(msg: str = "hello") -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )


class TestContextFilter:
    """ContextFilter enriches log records with RunContext fields."""

    def test_raises_without_context(self) -> None:
        """Missing RunContext must raise RuntimeError."""
        filt = ContextFilter()
        with pytest.raises(RuntimeError, match="RunContext not initialized"):
            filt.filter(_make_record())

    def test_enriches_record_when_context_exists(self) -> None:
        """Fields from RunContext are injected into the log record."""
        create_initial_context(job_name="test_job")
        filt = ContextFilter()
        record = _make_record()
        assert filt.filter(record) is True
        assert record.job_name == "test_job"  # type: ignore[attr-defined]
        assert hasattr(record, "run_id")
        assert hasattr(record, "env")
        assert hasattr(record, "host")

    def test_consistent_run_id(self) -> None:
        """All records within the same context share the same run_id."""
        create_initial_context(job_name="consistency")
        filt = ContextFilter()
        r1 = _make_record("msg1")
        r2 = _make_record("msg2")
        filt.filter(r1)
        filt.filter(r2)
        assert r1.run_id == r2.run_id  # type: ignore[attr-defined]
