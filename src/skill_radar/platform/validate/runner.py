"""Validation runner: check execution and console output."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

from .models import CheckResult, CheckStatus, NamedCheck, ValidationReport, create_check

logger = logging.getLogger(__name__)

# ANSI color codes
C_RESET = "\033[0m"
C_BOLD = "\033[1m"
C_DIM = "\033[2m"
C_GREEN = "\033[1;32m"
C_RED = "\033[1;31m"
C_BLUE = "\033[1;34m"


def _color_enabled() -> bool:
    """Check if terminal supports colors."""
    return os.environ.get("NO_COLOR") is None and os.isatty(1)


def _status_line(check: CheckResult) -> str:
    """Format a single-line status for a check."""
    use_color = _color_enabled()
    symbol = check.status.symbol
    color = check.status.color_code if use_color else ""
    reset = C_RESET if use_color else ""
    dim = C_DIM if use_color else ""

    line = f"  {color}{symbol}{reset} {check.name}"
    if check.detail:
        line += f" {dim}({check.detail}){reset}"
    if check.duration_ms > 0:
        line += f" {dim}[{check.duration_ms}ms]{reset}"
    return line


def print_header(validator_name: str, run_id: str, env: str) -> None:
    """Print validation header to console."""
    use_color = _color_enabled()
    blue = C_BLUE if use_color else ""
    bold = C_BOLD if use_color else ""
    dim = C_DIM if use_color else ""
    reset = C_RESET if use_color else ""

    print()
    print(f"{blue}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{reset}")
    print(f"  {bold}Validator:{reset} {validator_name}")
    print(f"  {dim}run_id:{reset} {run_id}  {dim}env:{reset} {env}")
    print(f"{blue}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{reset}")
    print()


def print_check_result(check: CheckResult) -> None:
    """Print a single check result line."""
    print(_status_line(check))


def print_footer(report: ValidationReport, report_path: str | None = None) -> None:
    """Print validation footer with summary."""
    use_color = _color_enabled()
    reset = C_RESET if use_color else ""
    dim = C_DIM if use_color else ""

    passed = sum(1 for c in report.checks if c.status == CheckStatus.PASS)
    failed = sum(1 for c in report.checks if c.status == CheckStatus.FAIL)
    warned = sum(1 for c in report.checks if c.status == CheckStatus.WARN)
    skipped = sum(1 for c in report.checks if c.status == CheckStatus.SKIP)

    status_color = C_GREEN if report.passed else C_RED
    status_color = status_color if use_color else ""
    status_text = "PASS" if report.passed else "FAIL"

    print()
    print(f"  {dim}────────────────────────────────────────────────{reset}")
    summary_parts = [f"✓ {passed}"]
    if failed:
        summary_parts.append(f"✗ {failed}")
    if warned:
        summary_parts.append(f"⚠ {warned}")
    if skipped:
        summary_parts.append(f"↷ {skipped}")

    print(f"  {status_color}{status_text}{reset}  ({' / '.join(summary_parts)})")
    if report.duration_ms:
        print(f"  {dim}Duration: {report.duration_ms}ms{reset}")
    if report_path:
        print(f"  {dim}Report: {report_path}{reset}")
    print()


def _is_spark_jvm_down(exc: BaseException) -> bool:
    """
    Heuristic: treat these as terminal Spark JVM failures.
    We prefer catching Py4JNetworkError explicitly, but also handle
    common 'connection refused' strings that sometimes get wrapped.
    """
    try:
        from py4j.protocol import Py4JNetworkError  # type: ignore
    except Exception:
        Py4JNetworkError = None  # type: ignore

    if Py4JNetworkError is not None and isinstance(exc, Py4JNetworkError):
        return True

    msg = str(exc).lower()
    # Common signatures when JVM is gone
    signatures = [
        "answer from java side is empty",
        "error while sending or receiving",
        "connection refused",
        "connection reset by peer",
        "gateway is not connected",
        "java gateway process exited",
    ]
    return any(s in msg for s in signatures)


def run_checks(
    checks: list[NamedCheck],
    *,
    validator_name: str,
    run_id: str = "",
    env: str = "",
    fail_fast: bool = False,
    quiet: bool = False,
) -> ValidationReport:
    """Execute *checks* and return an aggregated :class:`ValidationReport`."""
    resolved_env = env or os.environ.get("SKILLRADAR_ENV", "local")
    resolved_run_id = run_id

    if not resolved_run_id:
        try:
            from skill_radar.platform.logging.context import get_context

            ctx = get_context()
            resolved_run_id = ctx.run_id
        except (ImportError, RuntimeError):
            resolved_run_id = "n/a"

    report = ValidationReport(
        validator_name=validator_name,
        env=resolved_env,
        run_id=resolved_run_id,
        started_at_utc=datetime.now(UTC).isoformat(),
    )

    if not quiet:
        print_header(validator_name, resolved_run_id, resolved_env)

    start_time = datetime.now(UTC)

    # If JVM dies, we set this and skip the remainder
    terminal_skip_reason: str | None = None

    for check in checks:
        check_start = datetime.now(UTC)

        # If JVM already dead, skip remaining checks without executing them
        if terminal_skip_reason is not None:
            # Use the NamedCheck identity for proper skip reporting
            result = create_check(
                name=check.name,
                description=check.description,
                passed=False,
                skip=True,
                skip_reason=terminal_skip_reason,
            )
            # timing
            result.started_at_utc = check_start.isoformat()
            result.duration_ms = 0
            report.checks.append(result)
            if not quiet:
                print_check_result(result)
            continue

        try:
            result = check.fn()

        except Exception as exc:
            # Detect JVM death and stop the cascade
            if _is_spark_jvm_down(exc):
                terminal_skip_reason = (
                    "Spark JVM became unavailable during validation "
                    f"(Py4J/network failure). Root error: {str(exc)[:200]}"
                )
                logger.error("Terminal Spark JVM failure detected. Skipping remaining checks.")
                # Mark current check as FAIL (the one that observed the crash)
                result = CheckResult(
                    name=check.name,
                    description=check.description,
                    status=CheckStatus.FAIL,
                    detail=str(exc)[:200],
                    started_at_utc=check_start.isoformat(),
                    duration_ms=0,
                )
            else:
                logger.exception("Check failed with exception: %s", check.name)
                result = CheckResult(
                    name=check.name,
                    description=check.description,
                    status=CheckStatus.FAIL,
                    detail=str(exc)[:200],
                    started_at_utc=check_start.isoformat(),
                    duration_ms=0,
                )

        # Ensure timing is set
        check_end = datetime.now(UTC)
        if not result.started_at_utc:
            result.started_at_utc = check_start.isoformat()
        if not result.duration_ms:
            result.duration_ms = int((check_end - check_start).total_seconds() * 1000)

        report.checks.append(result)

        if not quiet:
            print_check_result(result)

        # Update overall status
        if result.status == CheckStatus.FAIL:
            report.status = CheckStatus.FAIL

            # If JVM died, we want to skip the rest (already set terminal_skip_reason)
            if terminal_skip_reason is not None:
                # Optionally: if you want the report to stop immediately (no SKIP rows),
                # you could `break` here. But SKIP rows are useful to explain why.
                continue

            if fail_fast:
                logger.warning("Fail-fast: stopping after failed check %s", result.name)
                break

    # Finalize report timing
    end_time = datetime.now(UTC)
    report.finished_at_utc = end_time.isoformat()
    report.duration_ms = int((end_time - start_time).total_seconds() * 1000)

    return report
