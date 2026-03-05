"""Unit tests for validation runner."""

from __future__ import annotations

from typing import TYPE_CHECKING

from skill_radar.platform.validate.models import CheckResult, CheckStatus, NamedCheck
from skill_radar.platform.validate.runner import run_checks

if TYPE_CHECKING:
    import pytest


def _make_check(name: str, desc: str, fn) -> NamedCheck:
    """Helper to wrap a check function in NamedCheck."""
    return NamedCheck(name=name, description=desc, fn=fn)


class TestRunChecks:
    """Tests for run_checks function."""

    def test_empty_checks_list(self) -> None:
        """Empty checks list returns passing report."""
        report = run_checks(
            checks=[],
            validator_name="test",
            run_id="test123",
            quiet=True,
        )
        assert report.passed is True
        assert report.validator_name == "test"
        assert report.run_id == "test123"
        assert len(report.checks) == 0

    def test_all_passing_checks(self) -> None:
        """All passing checks returns passing report."""

        def pass_check() -> CheckResult:
            return CheckResult(
                name="test.pass",
                description="Passing check",
                status=CheckStatus.PASS,
            )

        checks = [
            _make_check("test.pass.1", "Pass 1", pass_check),
            _make_check("test.pass.2", "Pass 2", pass_check),
            _make_check("test.pass.3", "Pass 3", pass_check),
        ]
        report = run_checks(
            checks=checks,
            validator_name="test",
            quiet=True,
        )
        assert report.passed is True
        assert len(report.checks) == 3
        assert all(c.status == CheckStatus.PASS for c in report.checks)

    def test_one_failing_check(self) -> None:
        """One failing check returns failing report."""

        def pass_check() -> CheckResult:
            return CheckResult(
                name="test.pass",
                description="Passing",
                status=CheckStatus.PASS,
            )

        def fail_check() -> CheckResult:
            return CheckResult(
                name="test.fail",
                description="Failing",
                status=CheckStatus.FAIL,
            )

        checks = [
            _make_check("test.pass.1", "Pass 1", pass_check),
            _make_check("test.fail", "Fail", fail_check),
            _make_check("test.pass.2", "Pass 2", pass_check),
        ]
        report = run_checks(
            checks=checks,
            validator_name="test",
            quiet=True,
        )
        assert report.passed is False
        assert len(report.checks) == 3
        assert report.status == CheckStatus.FAIL

    def test_fail_fast_stops_on_failure(self) -> None:
        """Fail-fast mode stops after first failure."""
        call_count = 0

        def pass_check() -> CheckResult:
            nonlocal call_count
            call_count += 1
            return CheckResult(
                name="test.pass",
                description="Passing",
                status=CheckStatus.PASS,
            )

        def fail_check() -> CheckResult:
            nonlocal call_count
            call_count += 1
            return CheckResult(
                name="test.fail",
                description="Failing",
                status=CheckStatus.FAIL,
            )

        checks = [
            _make_check("test.pass.1", "Pass 1", pass_check),
            _make_check("test.fail", "Fail", fail_check),
            _make_check("test.pass.2", "Pass 2", pass_check),
            _make_check("test.pass.3", "Pass 3", pass_check),
        ]
        report = run_checks(
            checks=checks,
            validator_name="test",
            fail_fast=True,
            quiet=True,
        )
        assert report.passed is False
        assert call_count == 2  # Stopped after fail_check
        assert len(report.checks) == 2

    def test_skip_does_not_fail_report(self) -> None:
        """Skipped checks do not fail the report."""

        def skip_check() -> CheckResult:
            return CheckResult(
                name="test.skip",
                description="Skipped",
                status=CheckStatus.SKIP,
            )

        report = run_checks(
            checks=[_make_check("test.skip", "Skip", skip_check)],
            validator_name="test",
            quiet=True,
        )
        assert report.passed is True

    def test_warn_does_not_fail_report(self) -> None:
        """Warning checks do not fail the report."""

        def warn_check() -> CheckResult:
            return CheckResult(
                name="test.warn",
                description="Warning",
                status=CheckStatus.WARN,
            )

        report = run_checks(
            checks=[_make_check("test.warn", "Warn", warn_check)],
            validator_name="test",
            quiet=True,
        )
        assert report.passed is True
        assert len(report.warned_checks) == 1

    def test_exception_in_check_becomes_failure(self) -> None:
        """Exception in check function becomes a failure."""

        def bad_check() -> CheckResult:
            raise RuntimeError("Oops!")

        report = run_checks(
            checks=[_make_check("test.bad", "Bad", bad_check)],
            validator_name="test",
            quiet=True,
        )
        assert report.passed is False
        assert len(report.checks) == 1
        assert report.checks[0].status == CheckStatus.FAIL
        assert "Oops!" in report.checks[0].detail

    def test_timing_is_recorded(self) -> None:
        """Check timing is recorded in report."""
        import time

        def slow_check() -> CheckResult:
            time.sleep(0.05)  # 50ms
            return CheckResult(
                name="test.slow",
                description="Slow check",
                status=CheckStatus.PASS,
            )

        report = run_checks(
            checks=[_make_check("test.slow", "Slow", slow_check)],
            validator_name="test",
            quiet=True,
        )
        assert report.duration_ms >= 50
        assert report.checks[0].duration_ms >= 50
        assert report.started_at_utc != ""
        assert report.finished_at_utc != ""

    def test_env_defaults_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Environment defaults from SKILLRADAR_ENV."""
        monkeypatch.setenv("SKILLRADAR_ENV", "test_env")

        report = run_checks(
            checks=[],
            validator_name="test",
            quiet=True,
        )
        assert report.env == "test_env"
