"""Tests for ContextFilter.

The filter injects RunContext fields when available and gracefully degrades
when context is missing (e.g. during shutdown or 3rd-party logging).
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

    def test_graceful_without_context(self) -> None:
        """Missing RunContext does not raise - filter returns True without injecting."""
        filt = ContextFilter()
        record = _make_record()
        # Should not raise RuntimeError
        result = filt.filter(record)
        assert result is True
        # Fields should not be injected
        assert not hasattr(record, "run_id")
        assert not hasattr(record, "job_name")

    def test_graceful_after_finalize(self) -> None:
        """After finalize_logging (reset), filter still works without raising."""
        create_initial_context(job_name="test_job")
        filt = ContextFilter()
        record1 = _make_record("before")
        assert filt.filter(record1) is True
        assert record1.job_name == "test_job"  # type: ignore[attr-defined]

        # Simulate finalize_logging() which calls reset_context()
        reset_context()

        record2 = _make_record("after")
        # Should not raise
        result = filt.filter(record2)
        assert result is True
        # Fields should not be injected on record2
        assert not hasattr(record2, "run_id")

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
