"""CLI commands for infrastructure provisioning and status."""

from __future__ import annotations

import logging
import sys

import click

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.logging import finalize_logging, init_logging
from skill_radar.platform.runtime import RuntimeContext, get_runtime_context
from skill_radar.platform.validate.models import (
    CheckStatus,
    ExitCode,
    ValidationReport,
)
from skill_radar.platform.validate.runner import (
    print_check_result,
    print_footer,
    print_header,
)
from skill_radar.platform.validate.sinks import finalize_report

logger = logging.getLogger(__name__)


@click.group("infra")
def infra_group() -> None:
    """Infrastructure provisioning and status commands."""


@infra_group.command("apply")
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
@click.option(
    "--skip-namespaces",
    is_flag=True,
    default=False,
    help="Skip namespace creation (buckets only)",
)
@click.option(
    "--context",
    type=click.Choice(["host", "docker"], case_sensitive=False),
    default=None,
    help="Runtime context (auto-detected if not specified)",
)
def infra_apply(upload: bool, quiet: bool, skip_namespaces: bool, context: str | None) -> None:
    """Provision infrastructure (buckets + Iceberg namespaces).

    Creates required platform resources idempotently:
    - S3 buckets (lake + logs)
    - Iceberg namespaces (sr_bronze, sr_silver, sr_gold)

    This command is idempotent - safe to run multiple times.
    When pyspark is unavailable, namespace creation is skipped with a warning.
    """
    ctx = init_logging("infra_apply", enable_file=True)
    config = load_platform_config()

    # Resolve runtime context
    runtime_ctx: RuntimeContext | None = None
    if context:
        runtime_ctx = RuntimeContext(context.lower())

    resolved_ctx = get_runtime_context(runtime_ctx)
    logger.info("Runtime context: %s", resolved_ctx.value)

    from skill_radar.platform.infra.apply import apply_infra

    results, artifacts = apply_infra(
        config,
        create_namespaces=not skip_namespaces,
        context=runtime_ctx,
    )

    # Build report
    report = ValidationReport(
        validator_name="infra_apply",
        env=config.platform.environment,
        run_id=ctx.run_id,
        checks=results,
    )

    # Compute overall status
    failed = [r for r in results if r.status == CheckStatus.FAIL]
    warned = [r for r in results if r.status == CheckStatus.WARN]

    if failed:
        report.status = CheckStatus.FAIL
    elif warned:
        report.status = CheckStatus.WARN
    else:
        report.status = CheckStatus.PASS

    # Store artifacts
    report.artifacts.update(artifacts)

    # Write report
    local_path, _s3_key = finalize_report(report, config=config, upload_s3=upload)

    if not quiet:
        print_header("infra_apply", ctx.run_id, config.platform.environment)
        for check in results:
            print_check_result(check)
        print_footer(report, str(local_path))

    finalize_logging()

    # Exit code logic:
    # - FAIL if any apply failed
    # - OK if all passed (including skipped namespaces)
    if failed:
        sys.exit(ExitCode.INFRA_FAILURE)
    else:
        sys.exit(ExitCode.OK)


@infra_group.command("status")
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
@click.option(
    "--context",
    type=click.Choice(["host", "docker"], case_sensitive=False),
    default=None,
    help="Runtime context (auto-detected if not specified)",
)
def infra_status(upload: bool, quiet: bool, context: str | None) -> None:
    """Show infrastructure status (read-only validation).

    Equivalent to 'skill-radar validate infra'.
    Uses fast-fail timeouts for quick detection of unreachable endpoints.
    """
    ctx = init_logging("validate_infra", enable_file=True)
    config = load_platform_config()

    # Resolve runtime context
    runtime_ctx: RuntimeContext | None = None
    if context:
        runtime_ctx = RuntimeContext(context.lower())

    resolved_ctx = get_runtime_context(runtime_ctx)
    logger.info("Runtime context: %s", resolved_ctx.value)

    from skill_radar.platform.validate.checks.infra import get_infra_checks
    from skill_radar.platform.validate.runner import run_checks

    checks = get_infra_checks(config, context=runtime_ctx)

    report = run_checks(
        checks,
        validator_name="infra",
        run_id=ctx.run_id,
        quiet=quiet,
    )

    # Add runtime context to report artifacts
    report.artifacts["runtime_context"] = resolved_ctx.value

    # Write report
    local_path, _s3_key = finalize_report(report, config=config, upload_s3=upload)

    if not quiet:
        print_footer(report, str(local_path))

    finalize_logging()

    if report.passed:
        sys.exit(ExitCode.OK)
    else:
        sys.exit(ExitCode.INFRA_FAILURE)
