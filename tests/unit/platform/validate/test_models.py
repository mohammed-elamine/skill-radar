"""Unit tests for validation models."""

from __future__ import annotations

from skill_radar.platform.validate.models import (
    CheckResult,
    CheckStatus,
    ExitCode,
    ValidationReport,
    create_check,
    exit_code_for_validator,
)


class TestCheckStatus:
    """Tests for CheckStatus enum."""

    def test_status_values(self) -> None:
        """Check status values are ordered correctly."""
        assert CheckStatus.PASS == 0
        assert CheckStatus.FAIL == 1
        assert CheckStatus.WARN == 2
        assert CheckStatus.SKIP == 3

    def test_status_symbols(self) -> None:
        """Check status symbols are correct."""
        assert CheckStatus.PASS.symbol == "✓"
        assert CheckStatus.FAIL.symbol == "✗"
        assert CheckStatus.WARN.symbol == "⚠"
        assert CheckStatus.SKIP.symbol == "↷"

    def test_status_color_codes(self) -> None:
        """Check status color codes are ANSI codes."""
        for status in CheckStatus:
            assert status.color_code.startswith("\033[")


class TestExitCode:
    """Tests for ExitCode enum."""

    def test_exit_codes_are_stable(self) -> None:
        """Exit codes should be stable across versions."""
        assert ExitCode.OK == 0
        assert ExitCode.INFRA_FAILURE == 10
        assert ExitCode.LANDING_FAILURE == 20
        assert ExitCode.BRONZE_FAILURE == 30
        assert ExitCode.UNEXPECTED == 50


class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_basic_check_result(self) -> None:
        """Create a basic CheckResult."""
        result = CheckResult(
            name="test.check",
            description="Test check",
            status=CheckStatus.PASS,
        )
        assert result.name == "test.check"
        assert result.description == "Test check"
        assert result.status == CheckStatus.PASS
        assert result.detail == ""
        assert result.metrics == {}

    def test_check_result_with_metrics(self) -> None:
        """Create CheckResult with metrics."""
        result = CheckResult(
            name="test.check",
            description="Test check",
            status=CheckStatus.PASS,
            metrics={"row_count": 100, "columns": 5},
        )
        assert result.metrics["row_count"] == 100
        assert result.metrics["columns"] == 5

    def test_check_result_as_dict(self) -> None:
        """CheckResult.as_dict() returns serializable dict."""
        result = CheckResult(
            name="test.check",
            description="Test check",
            status=CheckStatus.FAIL,
            detail="Something went wrong",
            metrics={"count": 42},
            started_at_utc="2025-01-01T00:00:00+00:00",
            duration_ms=123,
        )
        d = result.as_dict()
        assert d["name"] == "test.check"
        assert d["status"] == "FAIL"  # String, not enum
        assert d["detail"] == "Something went wrong"
        assert d["metrics"]["count"] == 42


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_empty_report(self) -> None:
        """Create an empty validation report."""
        report = ValidationReport(
            validator_name="test_validator",
            env="local",
        )
        assert report.validator_name == "test_validator"
        assert report.env == "local"
        assert report.passed is True
        assert report.checks == []
        assert len(report.report_id) == 8

    def test_report_with_checks(self) -> None:
        """Create report with checks."""
        checks = [
            CheckResult(name="check1", description="C1", status=CheckStatus.PASS),
            CheckResult(name="check2", description="C2", status=CheckStatus.FAIL),
        ]
        report = ValidationReport(
            validator_name="test",
            env="test",
            checks=checks,
            status=CheckStatus.FAIL,
        )
        assert len(report.checks) == 2
        assert report.passed is False
        assert len(report.failed_checks) == 1
        assert report.failed_checks[0].name == "check2"

    def test_report_as_dict(self) -> None:
        """ValidationReport.as_dict() returns serializable dict."""
        report = ValidationReport(
            validator_name="test",
            env="local",
            run_id="abc123",
            checks=[
                CheckResult(name="c1", description="Check 1", status=CheckStatus.PASS),
            ],
        )
        d = report.as_dict()
        assert d["validator_name"] == "test"
        assert d["env"] == "local"
        assert d["run_id"] == "abc123"
        assert d["status"] == "PASS"
        assert len(d["checks"]) == 1
        assert d["checks"][0]["status"] == "PASS"


class TestCreateCheck:
    """Tests for create_check factory function."""

    def test_create_passing_check(self) -> None:
        """Create a passing check."""
        result = create_check(
            name="test.pass",
            description="Passing check",
            passed=True,
        )
        assert result.status == CheckStatus.PASS
        assert result.started_at_utc != ""

    def test_create_failing_check(self) -> None:
        """Create a failing check."""
        result = create_check(
            name="test.fail",
            description="Failing check",
            passed=False,
            detail="Something is broken",
        )
        assert result.status == CheckStatus.FAIL
        assert result.detail == "Something is broken"

    def test_create_skipped_check(self) -> None:
        """Create a skipped check."""
        result = create_check(
            name="test.skip",
            description="Skipped check",
            passed=False,  # Ignored when skip=True
            skip=True,
            skip_reason="Not applicable",
        )
        assert result.status == CheckStatus.SKIP
        assert result.detail == "Not applicable"

    def test_create_warning_check(self) -> None:
        """Create a warning check."""
        result = create_check(
            name="test.warn",
            description="Warning check",
            passed=True,
            warn=True,
            warn_reason="Heads up!",
        )
        assert result.status == CheckStatus.WARN
        assert result.detail == "Heads up!"

    def test_create_check_with_metrics(self) -> None:
        """Create check with metrics."""
        result = create_check(
            name="test.metrics",
            description="Check with metrics",
            passed=True,
            metrics={"rows": 100},
        )
        assert result.metrics["rows"] == 100


class TestExitCodeMapping:
    """Tests for exit_code_for_validator function."""

    def test_infra_validator(self) -> None:
        """Infra validators return INFRA_FAILURE."""
        assert exit_code_for_validator("infra") == ExitCode.INFRA_FAILURE
        assert exit_code_for_validator("validate_infra") == ExitCode.INFRA_FAILURE
        assert exit_code_for_validator("INFRA_CHECK") == ExitCode.INFRA_FAILURE

    def test_landing_validator(self) -> None:
        """Landing validators return LANDING_FAILURE."""
        assert exit_code_for_validator("esco_landing") == ExitCode.LANDING_FAILURE
        assert exit_code_for_validator("landing_check") == ExitCode.LANDING_FAILURE

    def test_bronze_validator(self) -> None:
        """Bronze validators return BRONZE_FAILURE."""
        assert exit_code_for_validator("esco_bronze") == ExitCode.BRONZE_FAILURE
        assert exit_code_for_validator("bronze_e2e") == ExitCode.BRONZE_FAILURE

    def test_unknown_validator(self) -> None:
        """Unknown validators return UNEXPECTED."""
        assert exit_code_for_validator("something_else") == ExitCode.UNEXPECTED
