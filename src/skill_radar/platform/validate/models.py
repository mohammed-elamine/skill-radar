"""Validation models: CheckResult, ValidationReport, enums, and exit codes.

This module provides the core data structures for validation results:
- :class:`CheckStatus`: Pass/fail/warn/skip status for individual checks
- :class:`CheckResult`: Result of a single validation check
- :class:`NamedCheck`: A check definition with identity (name, description, callable)
- :class:`ValidationReport`: Aggregated result of a validation run
- :class:`ExitCode`: Consistent exit codes by failure type
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import IntEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class CheckStatus(IntEnum):
    """Status of a single validation check."""

    PASS = 0
    FAIL = 1
    WARN = 2
    SKIP = 3

    @property
    def symbol(self) -> str:
        """Return console symbol for the status."""
        return {
            CheckStatus.PASS: "✓",
            CheckStatus.FAIL: "✗",
            CheckStatus.WARN: "⚠",
            CheckStatus.SKIP: "↷",
        }[self]

    @property
    def color_code(self) -> str:
        """Return ANSI color code for the status."""
        return {
            CheckStatus.PASS: "\033[1;32m",
            CheckStatus.FAIL: "\033[1;31m",
            CheckStatus.WARN: "\033[1;33m",
            CheckStatus.SKIP: "\033[2m",
        }[self]


class ExitCode(IntEnum):
    """Consistent exit codes by failure type.

    These exit codes are stable and should not change across versions.
    """

    OK = 0
    INFRA_FAILURE = 10
    LANDING_FAILURE = 20
    BRONZE_FAILURE = 30
    SILVER_FAILURE = 40
    GOLD_FAILURE = 45
    UNEXPECTED = 50


@dataclass
class CheckResult:
    """Result of a single validation check.

    Attributes
    ----------
    name:
        Stable identifier (e.g. "landing.manifest.exists").
    description:
        Human-readable description of the check.
    status:
        Pass/fail/warn/skip status.
    detail:
        Optional short detail message (keep small).
    metrics:
        Optional dict with numeric metrics (e.g. row_count).
    started_at_utc:
        When the check started.
    duration_ms:
        How long the check took.
    """

    name: str
    description: str
    status: CheckStatus
    detail: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)
    started_at_utc: str = ""
    duration_ms: int = 0

    def as_dict(self) -> dict[str, Any]:
        """Return as a JSON-serializable dict."""
        d = asdict(self)
        d["status"] = self.status.name
        return d


@dataclass(frozen=True)
class NamedCheck:
    """A check definition with identity preserved.

    Use this to pass checks to `run_checks()` so that the runner can always
    identify a check by name/description, even when the callable is a lambda.

    Attributes
    ----------
    name:
        Stable identifier for the check (e.g. "infra.minio.reachable").
    description:
        Human-readable description of the check.
    fn:
        Callable that executes the check and returns a CheckResult.

    Example
    -------
    >>> check = NamedCheck(
    ...     name="infra.minio.reachable",
    ...     description="MinIO endpoint is reachable",
    ...     fn=lambda: check_minio_reachable(config),
    ... )
    >>> result = check.fn()  # Execute the check
    """

    name: str
    description: str
    fn: Callable[[], CheckResult]

    def __call__(self) -> CheckResult:
        """Execute the check callable."""
        return self.fn()


@dataclass
class ValidationReport:
    """Full validation run report.

    Attributes
    ----------
    report_id:
        Unique short identifier for this report.
    run_id:
        Run ID from logging context.
    validator_name:
        Name of the validator (e.g. "esco_bronze_e2e").
    env:
        Environment (SKILLRADAR_ENV).
    started_at_utc:
        When the validation started.
    finished_at_utc:
        When the validation finished.
    duration_ms:
        Total duration of the validation.
    status:
        Overall PASS or FAIL.
    checks:
        List of individual check results.
    artifacts:
        Dict of artifact paths/keys (e.g. report path, spark log).
    """

    validator_name: str
    env: str
    run_id: str = ""
    report_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    started_at_utc: str = ""
    finished_at_utc: str = ""
    duration_ms: int = 0
    status: CheckStatus = CheckStatus.PASS
    checks: list[CheckResult] = field(default_factory=list)
    artifacts: dict[str, str] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """Return True if the overall status is PASS."""
        return self.status == CheckStatus.PASS

    @property
    def failed_checks(self) -> list[CheckResult]:
        """Return list of failed checks."""
        return [c for c in self.checks if c.status == CheckStatus.FAIL]

    @property
    def warned_checks(self) -> list[CheckResult]:
        """Return list of warned checks."""
        return [c for c in self.checks if c.status == CheckStatus.WARN]

    def as_dict(self) -> dict[str, Any]:
        """Return as a JSON-serializable dict."""
        return {
            "report_id": self.report_id,
            "run_id": self.run_id,
            "validator_name": self.validator_name,
            "env": self.env,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "duration_ms": self.duration_ms,
            "status": self.status.name,
            "checks": [c.as_dict() for c in self.checks],
            "artifacts": self.artifacts,
        }


def exit_code_for_validator(validator_name: str) -> ExitCode:
    """Map a validator name to its failure exit code.

    Parameters
    ----------
    validator_name:
        The validator name (e.g. "infra", "esco_landing", "esco_bronze").

    Returns
    -------
    ExitCode:
        The exit code to use on failure for this validator.
    """
    if "infra" in validator_name.lower():
        return ExitCode.INFRA_FAILURE
    if "landing" in validator_name.lower():
        return ExitCode.LANDING_FAILURE
    if "bronze" in validator_name.lower():
        return ExitCode.BRONZE_FAILURE
    if "gold" in validator_name.lower():
        return ExitCode.GOLD_FAILURE
    if "silver" in validator_name.lower():
        return ExitCode.SILVER_FAILURE
    return ExitCode.UNEXPECTED


def create_check(
    name: str,
    description: str,
    passed: bool,
    *,
    detail: str = "",
    metrics: dict[str, Any] | None = None,
    skip: bool = False,
    skip_reason: str = "",
    warn: bool = False,
    warn_reason: str = "",
) -> CheckResult:
    """Factory to create a CheckResult with timing.

    This is a convenience function for creating checks with proper status.

    Parameters
    ----------
    name:
        Stable identifier for the check.
    description:
        Human-readable description.
    passed:
        Whether the check passed (ignored if skip=True).
    detail:
        Optional detail message.
    metrics:
        Optional metrics dict.
    skip:
        Mark as skipped instead of pass/fail.
    skip_reason:
        Reason for skipping (overrides detail).
    warn:
        Mark as warning instead of pass/fail.
    warn_reason:
        Warning message (overrides detail).
    """
    now = datetime.now(UTC).isoformat()

    if skip:
        status = CheckStatus.SKIP
        final_detail = skip_reason or detail
    elif warn:
        status = CheckStatus.WARN
        final_detail = warn_reason or detail
    elif passed:
        status = CheckStatus.PASS
        final_detail = detail
    else:
        status = CheckStatus.FAIL
        final_detail = detail

    return CheckResult(
        name=name,
        description=description,
        status=status,
        detail=final_detail,
        metrics=metrics or {},
        started_at_utc=now,
        duration_ms=0,
    )
