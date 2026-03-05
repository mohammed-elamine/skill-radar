"""Unit tests for validation sinks (local filesystem)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from skill_radar.platform.validate.models import CheckResult, CheckStatus, ValidationReport
from skill_radar.platform.validate.sinks import _report_filename, write_report_local

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


class TestReportFilename:
    """Tests for _report_filename helper."""

    def test_filename_format(self) -> None:
        """Filename follows expected format."""
        report = ValidationReport(
            validator_name="test",
            env="local",
            run_id="abc123",
            started_at_utc="2025-01-15T10:30:00+00:00",
        )
        filename = _report_filename(report)
        assert filename == "20250115_103000.abc123.json"

    def test_filename_with_iso_z_suffix(self) -> None:
        """Handle ISO format with Z suffix."""
        report = ValidationReport(
            validator_name="test",
            env="local",
            run_id="def456",
            started_at_utc="2025-06-20T14:45:30Z",
        )
        filename = _report_filename(report)
        assert filename == "20250620_144530.def456.json"


class TestWriteReportLocal:
    """Tests for write_report_local function."""

    def test_writes_json_file(self, tmp_path: Path) -> None:
        """Report is written as JSON to correct path."""
        report = ValidationReport(
            validator_name="test_validator",
            env="test",
            run_id="run123",
            started_at_utc="2025-01-01T00:00:00+00:00",
            checks=[
                CheckResult(
                    name="check1",
                    description="First check",
                    status=CheckStatus.PASS,
                )
            ],
        )

        path = write_report_local(report, log_dir=tmp_path)

        assert path.exists()
        assert path.parent.name == "test_validator"
        assert path.parent.parent.name == "validation"

        # Verify JSON content
        with path.open() as f:
            data = json.load(f)
        assert data["validator_name"] == "test_validator"
        assert data["run_id"] == "run123"
        assert len(data["checks"]) == 1

    def test_creates_nested_directories(self, tmp_path: Path) -> None:
        """Creates nested directory structure if needed."""
        report = ValidationReport(
            validator_name="deep_nested_validator",
            env="local",
            run_id="xyz",
        )

        path = write_report_local(report, log_dir=tmp_path)
        assert path.parent.exists()
        assert (tmp_path / "validation" / "deep_nested_validator").is_dir()

    def test_stores_path_in_artifacts(self, tmp_path: Path) -> None:
        """Stores the local path in report artifacts."""
        report = ValidationReport(
            validator_name="test",
            env="local",
            run_id="test123",
        )

        path = write_report_local(report, log_dir=tmp_path)
        assert "local_report_path" in report.artifacts
        assert report.artifacts["local_report_path"] == str(path)

    def test_uses_env_log_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Uses LOG_DIR env var when log_dir not specified."""
        monkeypatch.setenv("LOG_DIR", str(tmp_path / "env_logs"))

        report = ValidationReport(
            validator_name="test",
            env="local",
            run_id="env_test",
        )

        path = write_report_local(report)
        assert "env_logs" in str(path)
