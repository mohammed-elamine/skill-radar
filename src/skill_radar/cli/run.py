"""CLI commands for orchestration (apply + validate).

Provides convenience commands that combine provisioning and validation:
- skill-radar run infra       (infra apply + validate infra)
- skill-radar run esco-bronze (landing + bronze + validation)
"""

from __future__ import annotations

import logging
import sys

import click

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.logging import finalize_logging, init_logging, set_context
from skill_radar.platform.validate.models import (
    CheckResult,
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


# ---------------------------------------------------------------------------
# Command group
# ---------------------------------------------------------------------------


@click.group("run")
def run_group() -> None:
    """Orchestration commands (apply + validate)."""


# ---------------------------------------------------------------------------
# Run infra (apply + validate)
# ---------------------------------------------------------------------------


@run_group.command("infra")
@click.option("--upload", is_flag=True, default=False, help="Upload reports to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_infra(upload: bool, quiet: bool) -> None:
    """Provision and validate infrastructure.

    Orchestrates:
    1. infra apply (create buckets + namespaces)
    2. validate infra (verify all checks pass)

    Stops early if apply fails.
    """
    ctx = init_logging("run_infra", enable_file=True)
    config = load_platform_config()

    all_results = []
    artifacts: dict[str, str] = {}

    if not quiet:
        print_header("run_infra", ctx.run_id, config.platform.environment)
        click.echo("\n  ── Phase 1: Apply ──\n")

    # Phase 1: Apply
    from skill_radar.platform.infra.apply import apply_infra

    apply_results, apply_artifacts = apply_infra(config, create_namespaces=True)
    all_results.extend(apply_results)
    artifacts.update(apply_artifacts)

    if not quiet:
        for apply_check in apply_results:
            print_check_result(apply_check)

    # Check if apply failed
    apply_failed = [r for r in apply_results if r.status == CheckStatus.FAIL]
    if apply_failed:
        # Build report and exit
        report = ValidationReport(
            validator_name="run_infra",
            env=config.platform.environment,
            run_id=ctx.run_id,
            checks=all_results,
            status=CheckStatus.FAIL,
        )
        report.artifacts.update(artifacts)
        report.artifacts["phase_stopped"] = "apply"

        local_path, _ = finalize_report(report, config=config, upload_s3=upload)

        if not quiet:
            click.echo("\n  ✗ Apply failed, skipping validation\n")
            print_footer(report, str(local_path))

        finalize_logging()
        sys.exit(ExitCode.INFRA_FAILURE)

    # Phase 2: Validate
    if not quiet:
        click.echo("\n  ── Phase 2: Validate ──\n")

    from skill_radar.platform.validate.checks.infra import get_infra_checks

    validate_checks = get_infra_checks(config)

    # Run validation checks manually to collect results
    validate_results: list[CheckResult] = []
    for named_check in validate_checks:
        result = named_check.fn()
        validate_results.append(result)
        if not quiet:
            print_check_result(result)

    all_results.extend(validate_results)

    # Build final report
    failed = [r for r in all_results if r.status == CheckStatus.FAIL]
    warned = [r for r in all_results if r.status == CheckStatus.WARN]

    if failed:
        status = CheckStatus.FAIL
    elif warned:
        status = CheckStatus.WARN
    else:
        status = CheckStatus.PASS

    report = ValidationReport(
        validator_name="run_infra",
        env=config.platform.environment,
        run_id=ctx.run_id,
        checks=all_results,
        status=status,
    )
    report.artifacts.update(artifacts)

    local_path, _ = finalize_report(report, config=config, upload_s3=upload)

    if not quiet:
        print_footer(report, str(local_path))

    finalize_logging()

    if failed:
        sys.exit(ExitCode.INFRA_FAILURE)
    else:
        sys.exit(ExitCode.OK)


# ---------------------------------------------------------------------------
# Run ESCO bronze (extraction + validation) - Spark-side only
# ---------------------------------------------------------------------------


@run_group.command("esco-bronze")
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.0)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option(
    "--entities",
    default=None,
    help="Comma-separated subset of entities (default: all)",
)
@click.option(
    "--fail-fast/--no-fail-fast",
    default=True,
    help="Abort on first entity error",
)
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_esco_bronze(
    version: str,
    lang: str,
    entities: str | None,
    fail_fast: bool,
    upload: bool,
    quiet: bool,
) -> None:
    """Run ESCO bronze pipeline: extraction + validation (Spark-side).

    This command runs INSIDE the Spark container and:
    1. Reads artifact from S3 landing zone (must be uploaded first)
    2. Extracts CSVs → Iceberg bronze tables
    3. Validates bronze tables

    Prerequisites:
    - Infrastructure provisioned (make run-infra)
    - Artifact uploaded to landing (make upload-esco VERSION=... LANG=... FILE=...)

    Example (inside Spark container):
        skill-radar run esco-bronze --version v1.2.0 --lang fr

    For full pipeline from host:
        make run-esco-bronze VERSION=v1.2.0 LANG=fr FILE=data/esco.zip
    """
    # Check pyspark availability - this command MUST run inside Spark container
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo(
            "Error: PySpark is not installed in this environment.\n"
            "This command must be run inside the Spark container:\n\n"
            "  docker compose exec -T spark bash -lc \\\n"
            f'    "uv run skill-radar run esco-bronze --version {version} --lang {lang}"\n\n'
            "Or use the Makefile (includes upload):\n\n"
            f"  make run-esco-bronze VERSION={version} LANG={lang} FILE=data/esco.zip\n"
        )
        sys.exit(ExitCode.BRONZE_FAILURE)

    ctx = init_logging("run_esco_bronze", enable_file=True)
    set_context(dataset="esco", version=version, lang=lang)
    config = load_platform_config()

    all_results = []
    artifacts: dict[str, str] = {"version": version, "lang": lang}

    if not quiet:
        print_header("run_esco_bronze", ctx.run_id, config.platform.environment)

    spark = None
    try:
        spark = SparkSession.builder.appName(f"run_esco_bronze_{version}_{lang}").getOrCreate()
        artifacts["spark_app_id"] = spark.sparkContext.applicationId
        set_context(spark_app_id=spark.sparkContext.applicationId)

        # Phase 1: Bronze extraction
        if not quiet:
            click.echo("\n  ── Phase 1: Bronze Extraction ──\n")

        from skill_radar.domains.esco.bronze.extract import run_bronze_extraction

        entity_list = entities.split(",") if entities else None

        extract_result = run_bronze_extraction(
            spark=spark,
            version=version,
            lang=lang,
            entities=entity_list,
            fail_fast=fail_fast,
            run_id=ctx.run_id,
        )

        from skill_radar.platform.validate.models import create_check

        # Create check results for each entity
        for er in extract_result.entities:
            if er.status == "success":
                entity_check = create_check(
                    name=f"run.bronze.extract.{er.entity}",
                    description=f"Extract {er.entity} to Iceberg",
                    passed=True,
                    detail=f"{er.row_count} rows → {er.table}",
                )
            else:
                entity_check = create_check(
                    name=f"run.bronze.extract.{er.entity}",
                    description=f"Extract {er.entity} to Iceberg",
                    passed=False,
                    detail=er.error or "Unknown error",
                )
            all_results.append(entity_check)
            if not quiet:
                print_check_result(entity_check)

        # Summary check for extraction
        if extract_result.success:
            summary_check = create_check(
                name="run.bronze.extract.summary",
                description="Bronze extraction summary",
                passed=True,
                detail=f"All {len(extract_result.entities)} entities succeeded",
            )
        else:
            failed_entities = [e.entity for e in extract_result.entities if e.status == "failed"]
            summary_check = create_check(
                name="run.bronze.extract.summary",
                description="Bronze extraction summary",
                passed=False,
                detail=f"Failed: {', '.join(failed_entities)}",
            )

        all_results.append(summary_check)
        if not quiet:
            print_check_result(summary_check)

        # If extraction failed, stop here
        if not extract_result.success:
            report = ValidationReport(
                validator_name="run_esco_bronze",
                env=config.platform.environment,
                run_id=ctx.run_id,
                checks=all_results,
                status=CheckStatus.FAIL,
            )
            report.artifacts.update(artifacts)
            report.artifacts["phase_stopped"] = "bronze_extraction"

            local_path, _ = finalize_report(report, config=config, upload_s3=upload)

            if not quiet:
                click.echo("\n  ✗ Bronze extraction failed\n")
                print_footer(report, str(local_path))

            finalize_logging()
            sys.exit(ExitCode.BRONZE_FAILURE)

        # Phase 2: Bronze validation
        if not quiet:
            click.echo("\n  ── Phase 2: Bronze Validation ──\n")

        from skill_radar.platform.validate.checks.esco import get_bronze_checks

        bronze_checks = get_bronze_checks(spark, config, version, lang, entities=entity_list)

        for named_check in bronze_checks:
            result = named_check.fn()
            all_results.append(result)
            if not quiet:
                print_check_result(result)

        # Build final report
        failed = [r for r in all_results if r.status == CheckStatus.FAIL]
        warned = [r for r in all_results if r.status == CheckStatus.WARN]

        if failed:
            status = CheckStatus.FAIL
        elif warned:
            status = CheckStatus.WARN
        else:
            status = CheckStatus.PASS

        report = ValidationReport(
            validator_name="run_esco_bronze",
            env=config.platform.environment,
            run_id=ctx.run_id,
            checks=all_results,
            status=status,
        )
        report.artifacts.update(artifacts)

        local_path, _ = finalize_report(report, config=config, upload_s3=upload)

        if not quiet:
            print_footer(report, str(local_path))

        finalize_logging()

        if failed:
            sys.exit(ExitCode.BRONZE_FAILURE)
        else:
            sys.exit(ExitCode.OK)

    finally:
        if spark:
            spark.stop()
