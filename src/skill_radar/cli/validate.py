"""CLI commands for validation operations.

Provides a thin wrapper around the core validation framework:
- skill-radar validate infra
- skill-radar validate esco-landing
- skill-radar validate esco-bronze
- skill-radar validate esco-bronze-e2e
- skill-radar validate bronze (group validator)
"""

from __future__ import annotations

import contextlib
import logging
import sys

import click

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.logging import finalize_logging, init_logging, set_context
from skill_radar.platform.runtime import RuntimeContext, get_runtime_context
from skill_radar.platform.validate.models import (
    CheckStatus,
    ExitCode,
    NamedCheck,
    ValidationReport,
)
from skill_radar.platform.validate.runner import print_footer, run_checks
from skill_radar.platform.validate.sinks import finalize_report

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Command group
# ---------------------------------------------------------------------------


@click.group("validate")
def validate_group() -> None:
    """Validation commands for infrastructure and datasets."""


# ---------------------------------------------------------------------------
# Infrastructure validation
# ---------------------------------------------------------------------------


@validate_group.command("infra")
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
@click.option(
    "--scope",
    type=click.Choice(["host", "runtime", "all"], case_sensitive=False),
    default="host",
    show_default=True,
    help="Validation scope: host (boto3 checks), runtime (Spark/Iceberg checks), all (both)",
)
def validate_infra(upload: bool, quiet: bool, scope: str) -> None:
    """Validate infrastructure health.

    Scope determines which checks run:

    \b
    --scope host (default):
        Host-level checks via boto3 (no Spark required):
        - MinIO/S3 reachable
        - Required buckets exist

    \b
    --scope runtime:
        Runtime checks requiring Spark (run inside container):
        - Spark session starts
        - Iceberg integration configured
        - S3A access from Spark
        - Required namespaces exist

    \b
    --scope all:
        Both host and runtime checks (run inside container).

    Examples:
        uv run skill-radar validate infra
        docker compose exec spark skill-radar validate infra --scope runtime
        docker compose exec spark skill-radar validate infra --scope all
    """
    scope_lower = scope.lower()

    # For runtime or all scopes, enforce pyspark availability upfront
    if scope_lower in ("runtime", "all"):
        try:
            import pyspark  # noqa: F401
        except ImportError:
            click.echo(
                "Error: PySpark is not installed in this environment.\n\n"
                "Runtime checks (--scope runtime, --scope all) must be run inside "
                "the Spark container:\n\n"
                "  docker compose exec -T spark bash -lc \\\n"
                f'    "uv run skill-radar validate infra --scope {scope_lower}"\n\n'
                "Or use the Makefile:\n\n"
                f"  make validate-infra-{scope_lower}\n"
            )
            sys.exit(ExitCode.INFRA_FAILURE)

    # Determine job name based on scope
    job_name = f"validate_infra_{scope_lower}" if scope_lower != "host" else "validate_infra"
    ctx = init_logging(job_name, enable_file=True)

    config = load_platform_config()

    # Import check functions
    from skill_radar.platform.validate.checks.infra import (
        get_infra_checks_host,
        get_infra_checks_runtime,
    )

    # Build checks based on scope
    # Resolve runtime context ONCE (env var override applies)
    resolved_ctx = get_runtime_context(None)
    artifacts: dict[str, str] = {"scope": scope_lower, "runtime_context": resolved_ctx.value}

    all_checks: list[NamedCheck] = []

    def _fail(msg: str) -> None:
        click.echo(msg)
        finalize_logging()
        sys.exit(ExitCode.INFRA_FAILURE)

    if scope_lower == "host":
        # Deterministic: Makefile should set SKILLRADAR_RUNTIME_CONTEXT=host
        if resolved_ctx != RuntimeContext.HOST:
            _fail(
                "Error: infra --scope host must run with host context.\n"
                f"Resolved context: {resolved_ctx.value}\n\n"
                "Run from host with:\n"
                "  SKILLRADAR_RUNTIME_CONTEXT=host uv run skill-radar validate infra --scope host\n"
                "Or:\n"
                "  make validate-infra\n"
            )
        all_checks = get_infra_checks_host(config, context=resolved_ctx)

    elif scope_lower in ("runtime", "all"):
        # Deterministic: Makefile should set SKILLRADAR_RUNTIME_CONTEXT=docker
        if resolved_ctx != RuntimeContext.DOCKER:
            _fail(
                f"Error: infra --scope {scope_lower} must run inside Docker.\n"
                f"Resolved context: {resolved_ctx.value}\n\n"
                "Run inside the Spark container with:\n"
                f'  docker compose exec -T spark bash -lc "SKILLRADAR_RUNTIME_CONTEXT=docker uv run skill-radar validate infra --scope {scope_lower}"\n'
                "Or:\n"
                f"  make validate-infra-{scope_lower}\n"
            )

        # In docker scope, all checks use docker endpoints.
        # 'runtime' => runtime checks only, 'all' => host+boto3 checks + runtime checks (both using docker endpoint)
        host_checks = (
            get_infra_checks_host(config, context=resolved_ctx) if scope_lower == "all" else []
        )
        runtime_checks = get_infra_checks_runtime(config, context=resolved_ctx)

        all_checks = host_checks + runtime_checks
    else:
        _fail(f"Unexpected scope: {scope_lower}")

    # Run checks
    report = run_checks(
        all_checks,
        validator_name="infra",
        run_id=ctx.run_id,
        quiet=quiet,
    )

    # Add artifacts to report
    report.artifacts.update(artifacts)

    # Write report
    local_path, _s3_key = finalize_report(report, config=config, upload_s3=upload)

    if not quiet:
        print_footer(report, str(local_path))

    finalize_logging()

    if report.passed:
        sys.exit(ExitCode.OK)
    else:
        sys.exit(ExitCode.INFRA_FAILURE)


# ---------------------------------------------------------------------------
# ESCO landing validation
# ---------------------------------------------------------------------------


@validate_group.command("esco-landing")
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.1)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def validate_esco_landing(version: str, lang: str, upload: bool, quiet: bool) -> None:
    """Validate ESCO landing zone (artifact + manifest)."""
    ctx = init_logging("validate_esco_landing", enable_file=True)
    set_context(dataset="esco", version=version, lang=lang)

    config = load_platform_config()

    from skill_radar.platform.validate.checks.esco import get_landing_checks

    checks = get_landing_checks(config, version, lang)

    report = run_checks(
        checks,
        validator_name="esco_landing",
        run_id=ctx.run_id,
        quiet=quiet,
    )

    # Write report
    local_path, _s3_key = finalize_report(report, config=config, upload_s3=upload)

    if not quiet:
        print_footer(report, str(local_path))

    finalize_logging()

    if report.passed:
        sys.exit(ExitCode.OK)
    else:
        sys.exit(ExitCode.LANDING_FAILURE)


# ---------------------------------------------------------------------------
# ESCO bronze validation
# ---------------------------------------------------------------------------


@validate_group.command("esco-bronze")
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.0)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option("--entities", default=None, help="Comma-separated entities (default: all)")
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def validate_esco_bronze(
    version: str,
    lang: str,
    entities: str | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Validate ESCO bronze tables (requires Spark/Iceberg).

    Run inside Spark container or with pyspark installed.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo(
            "Error: PySpark is not installed in this environment.\n"
            "Run this validation inside the Spark container:\n\n"
            "  docker compose exec -T spark bash -lc \\\n"
            f'    "uv run skill-radar validate esco-bronze --version {version} --lang {lang}"\n'
        )
        sys.exit(ExitCode.BRONZE_FAILURE)

    ctx = init_logging("validate_esco_bronze", enable_file=True)
    set_context(dataset="esco", version=version, lang=lang)
    config = load_platform_config()

    spark = (
        SparkSession.builder.appName(f"validate_esco_bronze_{version}_{lang}")
        # mitigate JVM crashes in scans
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        # optional: also reduce aggressive optimizations
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark_app_id: str | None = None
    try:
        try:
            spark_app_id = spark.sparkContext.applicationId
            set_context(spark_app_id=spark_app_id)
        except Exception:
            # JVM might already be unhealthy; don't fail the command here.
            spark_app_id = None

        from skill_radar.platform.validate.checks.esco import get_bronze_checks

        entity_list = entities.split(",") if entities else None
        checks = get_bronze_checks(spark, config, version, lang, entities=entity_list)

        report = run_checks(
            checks,
            validator_name="esco_bronze",
            run_id=ctx.run_id,
            quiet=quiet,
        )

        # Store spark app id if available
        report.artifacts["spark_app_id"] = spark_app_id or "unavailable"

        local_path, _s3_key = finalize_report(report, config=config, upload_s3=upload)

        if not quiet:
            print_footer(report, str(local_path))

        finalize_logging()
        sys.exit(ExitCode.OK if report.passed else ExitCode.BRONZE_FAILURE)

    finally:
        with contextlib.suppress(Exception):
            # Spark JVM may have crashed; ignore shutdown errors.
            spark.stop()


# ---------------------------------------------------------------------------
# ESCO bronze E2E validation (Spark-side only)
# ---------------------------------------------------------------------------


@validate_group.command("esco-bronze-e2e")
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.0)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option(
    "--entities",
    default=None,
    help="Comma-separated entities to validate (default: all)",
)
@click.option(
    "--fail-fast/--no-fail-fast",
    default=True,
    help="Abort on first entity error",
)
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def validate_esco_bronze_e2e(
    version: str,
    lang: str,
    entities: str | None,
    fail_fast: bool,
    upload: bool,
    quiet: bool,
) -> None:
    """Run ESCO bronze E2E validation: extraction + table checks (Spark-side).

    This command runs INSIDE the Spark container and:
    1. Reads artifact from S3 landing zone (must be uploaded first)
    2. Extracts CSVs → Iceberg bronze tables
    3. Validates bronze tables (schema, row counts, lineage)

    Prerequisites:
    - Infrastructure provisioned (make run-infra)
    - Artifact uploaded to landing (make upload-esco VERSION=... LANG=... FILE=...)

    Example (inside Spark container):
        skill-radar validate esco-bronze-e2e --version v1.2.0 --lang fr
    """
    # Check pyspark availability - this command MUST run inside Spark container
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo(
            "Error: PySpark is not installed in this environment.\n"
            "This command must be run inside the Spark container:\n\n"
            "  docker compose exec -T spark bash -lc \\\n"
            f'    "uv run skill-radar validate esco-bronze-e2e --version {version} --lang {lang}"\n'
        )
        sys.exit(ExitCode.BRONZE_FAILURE)

    ctx = init_logging("validate_esco_bronze_e2e", enable_file=True)
    set_context(dataset="esco", version=version, lang=lang)

    config = load_platform_config()

    spark = None
    try:
        spark = SparkSession.builder.appName(
            f"validate_esco_bronze_e2e_{version}_{lang}"
        ).getOrCreate()
        set_context(spark_app_id=spark.sparkContext.applicationId)

        from skill_radar.platform.validate.checks.esco import run_bronze_e2e_checks

        entity_list = entities.split(",") if entities else None

        results = run_bronze_e2e_checks(
            spark=spark,
            config=config,
            version=version,
            lang=lang,
            entities=entity_list,
            fail_fast=fail_fast,
            run_id=ctx.run_id,
        )

        # Build report from results
        report = ValidationReport(
            validator_name="esco_bronze_e2e",
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
        report.artifacts["spark_app_id"] = spark.sparkContext.applicationId
        report.artifacts["version"] = version
        report.artifacts["lang"] = lang

        # Write report
        local_path, _s3_key = finalize_report(report, config=config, upload_s3=upload)

        if not quiet:
            # Print header + results manually
            from skill_radar.platform.validate.runner import print_check_result, print_header

            print_header("esco_bronze_e2e", ctx.run_id, config.platform.environment)
            for check in results:
                print_check_result(check)
            print_footer(report, str(local_path))

        finalize_logging()

        if report.passed:
            sys.exit(ExitCode.OK)
        else:
            sys.exit(ExitCode.BRONZE_FAILURE)

    finally:
        if spark:
            spark.stop()


# ---------------------------------------------------------------------------
# Bronze group validation
# ---------------------------------------------------------------------------


@validate_group.command("bronze")
@click.option("--dataset", default="esco", help="Dataset to validate (default: esco)")
@click.option("--version", required=True, help="Artifact version")
@click.option("--lang", required=True, help="Language code")
@click.option("--entities", default=None, help="Comma-separated entities (default: all)")
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def validate_bronze_group(
    dataset: str,
    version: str,
    lang: str,
    entities: str | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Validate bronze tables for a dataset (group validator).

    Currently supports: esco
    """
    if dataset.lower() != "esco":
        click.echo(f"Error: Dataset '{dataset}' not yet supported. Available: esco")
        sys.exit(ExitCode.UNEXPECTED)

    # Delegate to esco-bronze command
    ctx = click.get_current_context()
    ctx.invoke(
        validate_esco_bronze,
        version=version,
        lang=lang,
        entities=entities,
        upload=upload,
        quiet=quiet,
    )


# ---------------------------------------------------------------------------
# ESCO silver validation
# ---------------------------------------------------------------------------


@validate_group.command("esco-silver")
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.1)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option("--entities", default=None, help="Comma-separated entities (default: all)")
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def validate_esco_silver(
    version: str,
    lang: str,
    entities: str | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Validate ESCO silver tables (requires Spark/Iceberg).

    Validates:
    - Silver namespace exists
    - Silver tables exist and have rows for the partition
    - Required columns present
    - Uniqueness constraints (no duplicate keys)
    - Referential integrity (relations → occupations/skills)

    Run inside Spark container or with pyspark installed.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo(
            "Error: PySpark is not installed in this environment.\n"
            "Run this validation inside the Spark container:\n\n"
            "  docker compose exec -T spark bash -lc \\\n"
            f'    "uv run skill-radar validate esco-silver --version {version} --lang {lang}"\n'
        )
        sys.exit(ExitCode.SILVER_FAILURE)

    ctx = init_logging("validate_esco_silver", enable_file=True)
    set_context(dataset="esco", version=version, lang=lang)
    config = load_platform_config()

    spark = (
        SparkSession.builder.appName(f"validate_esco_silver_{version}_{lang}")
        # mitigate JVM crashes in scans
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark_app_id: str | None = None
    try:
        try:
            spark_app_id = spark.sparkContext.applicationId
            set_context(spark_app_id=spark_app_id)
        except Exception:
            spark_app_id = None

        from skill_radar.platform.validate.checks.esco import get_silver_checks

        entity_list = entities.split(",") if entities else None
        checks = get_silver_checks(spark, config, version, lang, entities=entity_list)

        report = run_checks(
            checks,
            validator_name="esco_silver",
            run_id=ctx.run_id,
            quiet=quiet,
        )

        # Store spark app id if available
        report.artifacts["spark_app_id"] = spark_app_id or "unavailable"
        report.artifacts["version"] = version
        report.artifacts["lang"] = lang

        local_path, _s3_key = finalize_report(report, config=config, upload_s3=upload)

        if not quiet:
            print_footer(report, str(local_path))

        finalize_logging()
        sys.exit(ExitCode.OK if report.passed else ExitCode.SILVER_FAILURE)

    finally:
        with contextlib.suppress(Exception):
            spark.stop()
