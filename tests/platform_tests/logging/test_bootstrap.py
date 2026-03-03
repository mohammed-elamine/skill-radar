"""Tests for the platform logging bootstrap module."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from skill_radar.platform.logging.bootstrap import (
    finalize_logging,
    init_logging,
)
from skill_radar.platform.logging.context import get_context, reset_context, set_context
from skill_radar.platform.logging.filters import ContextFilter


@pytest.fixture(autouse=True)
def _clean_logging():
    """Reset logging state between tests to ensure isolation."""
    # Teardown: reset after each test
    yield
    # Force re-initialization capability
    import skill_radar.platform.logging.bootstrap as _mod

    _mod._INITIALIZED = False
    _mod._LOGFILE_PATH = None
    # Clear root handlers
    root = logging.getLogger()
    root.handlers.clear()
    for f in list(root.filters):
        root.removeFilter(f)
    reset_context()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestInitIdempotent:
    def test_second_call_returns_same_context(self, tmp_path: Path):
        ctx1 = init_logging("test_job", log_dir=str(tmp_path), enable_file=False)
        ctx2 = init_logging("test_job", log_dir=str(tmp_path), enable_file=False)
        assert ctx1.run_id == ctx2.run_id

    def test_handlers_not_duplicated(self, tmp_path: Path):
        init_logging("test_job", log_dir=str(tmp_path), enable_file=False)
        init_logging("test_job", log_dir=str(tmp_path), enable_file=False)
        root = logging.getLogger()
        # Only 1 console handler
        assert len(root.handlers) == 1


# ---------------------------------------------------------------------------
# Context injection
# ---------------------------------------------------------------------------


class TestContextInjection:
    def test_fields_present_on_record(self, tmp_path: Path):
        init_logging("ctx_test", log_dir=str(tmp_path), enable_file=False)
        set_context(dataset="esco", version="v1.0.0", lang="fr")

        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        root = logging.getLogger()
        cap = Capture()
        cap.addFilter(ContextFilter())
        root.addHandler(cap)

        logging.getLogger("test.ctx").info("hello")

        assert len(records) == 1
        rec = records[0]
        assert rec.run_id  # type: ignore[attr-defined]
        assert rec.job_name == "ctx_test"  # type: ignore[attr-defined]
        assert rec.dataset == "esco"  # type: ignore[attr-defined]
        assert rec.version == "v1.0.0"  # type: ignore[attr-defined]
        assert rec.lang == "fr"  # type: ignore[attr-defined]

    def test_set_context_merges(self, tmp_path: Path):
        init_logging("merge_test", log_dir=str(tmp_path), enable_file=False)
        ctx1 = set_context(dataset="esco")
        assert ctx1.dataset == "esco"
        ctx2 = set_context(version="v2.0.0")
        assert ctx2.dataset == "esco"  # preserved
        assert ctx2.version == "v2.0.0"  # added

    def test_get_context_returns_active(self, tmp_path: Path):
        init_logging("get_test", log_dir=str(tmp_path), enable_file=False)
        ctx = get_context()
        assert ctx.job_name == "get_test"
        assert ctx.env  # should have a default


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


class TestJsonFormatter:
    def test_outputs_valid_json(self, tmp_path: Path):
        init_logging(
            "json_test",
            log_dir=str(tmp_path),
            enable_file=True,
            file_format="json",
        )
        test_logger = logging.getLogger("test.json")
        test_logger.info("hello json")

        finalize_logging(upload=False)

        # Find the log file
        logfiles = list(tmp_path.glob("json_test.*.log"))
        assert len(logfiles) == 1
        content = logfiles[0].read_text().strip()
        # Every line should be valid JSON
        for line in content.splitlines():
            obj = json.loads(line)
            assert "timestamp" in obj
            assert "level" in obj
            assert "message" in obj
            assert "run_id" in obj
            assert "job_name" in obj


# ---------------------------------------------------------------------------
# Log file creation
# ---------------------------------------------------------------------------


class TestLogFile:
    def test_logfile_created_and_written(self, tmp_path: Path):
        init_logging("file_test", log_dir=str(tmp_path), enable_file=True)
        logging.getLogger("test.file").warning("check file")

        finalize_logging(upload=False)

        logfiles = list(tmp_path.glob("file_test.*.log"))
        assert len(logfiles) == 1
        text = logfiles[0].read_text()
        assert "check file" in text

    def test_no_file_when_disabled(self, tmp_path: Path):
        init_logging("nofile_test", log_dir=str(tmp_path), enable_file=False)
        logging.getLogger("test.nofile").info("no file")

        finalize_logging(upload=False)

        logfiles = list(tmp_path.glob("nofile_test.*.log"))
        assert len(logfiles) == 0


# ---------------------------------------------------------------------------
# Finalize
# ---------------------------------------------------------------------------


class TestFinalize:
    def test_finalize_resets_state(self, tmp_path: Path):
        init_logging("fin_test", log_dir=str(tmp_path), enable_file=False)
        finalize_logging(upload=False)

        import skill_radar.platform.logging.bootstrap as _mod

        assert _mod._INITIALIZED is False

    def test_finalize_without_upload(self, tmp_path: Path):
        init_logging("noup_test", log_dir=str(tmp_path), enable_file=True)
        logging.getLogger("test.noup").info("no upload")
        key = finalize_logging(upload=False)
        assert key is None
