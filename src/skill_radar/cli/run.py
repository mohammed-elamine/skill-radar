"""CLI commands for coarse-grained pipeline stage units.

Each ``run`` sub-command represents **one stage unit** — a logically
grouped sequence of processing + validation steps that share a single
Spark/JVM session.  Airflow DAGs call these commands so that only one
Docker container is launched per stage, eliminating redundant startup
overhead.

Available units
---------------
- ``skill-radar run infra``           — infra apply + validate
- ``skill-radar run esco-bronze``     — (optional landing validation) + bronze extraction + validate bronze
- ``skill-radar run esco-silver``     — silver formatting + validate silver
- ``skill-radar run adzuna-bronze``   — bronze extraction + validate bronze
- ``skill-radar run adzuna-silver``   — silver formatting + validate silver
- ``skill-radar run gold``            — matching + analytics + validate gold
- ``skill-radar run search``          — search export + validate search
"""

from __future__ import annotations

import logging
import sys
import time

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
# Helpers
# ---------------------------------------------------------------------------


def _phase_banner(label: str, quiet: bool) -> float:
    """Print a phase header and return the start timestamp."""
    if not quiet:
        click.echo(f"\n  ── {label} ──\n")
    return time.monotonic()


def _phase_elapsed(start: float) -> str:
    """Return a human-readable elapsed string."""
    elapsed = time.monotonic() - start
    return f"{elapsed:.1f}s"


# ---------------------------------------------------------------------------
# Command group
# ---------------------------------------------------------------------------


@click.group("run")
def run_group() -> None:
    """Coarse-grained stage-unit commands (process + validate)."""


# ---------------------------------------------------------------------------
# Runtime diagnostics
# ---------------------------------------------------------------------------


@run_group.command("diagnostics")
def run_diagnostics() -> None:
    """Print Spark runtime environment diagnostics.

    Useful for verifying that the container has the expected Spark,
    Java, Python versions and CPU architecture.
    """
    import platform
    import subprocess

    lines: list[str] = []

    # Spark version
    try:
        import pyspark

        lines.append(f"Spark version : {pyspark.__version__}")
    except ImportError:
        lines.append("Spark version : (pyspark not installed)")

    # Java version
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        # java -version prints to stderr
        raw = result.stderr.strip().split("\n")[0] if result.stderr else "(unknown)"
        lines.append(f"Java version  : {raw}")
    except FileNotFoundError:
        lines.append("Java version  : (java not found on PATH)")

    # Architecture
    lines.append(f"Architecture  : {platform.machine()}")

    # Python version
    lines.append(f"Python version: {platform.python_version()}")

    click.echo("\n".join(lines))


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
@click.option(
    "--validate-landing/--no-validate-landing",
    default=True,
    help="Run landing zone validation before bronze extraction",
)
@click.option("--upload", is_flag=True, default=False, help="Upload report to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_esco_bronze(
    version: str,
    lang: str,
    entities: str | None,
    fail_fast: bool,
    validate_landing: bool,
    upload: bool,
    quiet: bool,
) -> None:
    """Run ESCO bronze stage unit: (landing validation) + extraction + validation.

    Executes inside a single Spark session:
    1. (Optional) Validate ESCO landing zone in S3
    2. Extract CSVs → Iceberg bronze tables
    3. Validate bronze tables

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

        # Phase 0 (optional): Landing validation
        if validate_landing:
            t0 = _phase_banner("Phase 0: Landing Validation", quiet)

            from skill_radar.platform.validate.checks.esco import get_landing_checks

            landing_checks = get_landing_checks(config, version, lang)
            for named_check in landing_checks:
                result = named_check.fn()
                all_results.append(result)
                if not quiet:
                    print_check_result(result)

            landing_failed = [r for r in all_results if r.status == CheckStatus.FAIL]
            if landing_failed:
                logger.error("Landing validation failed — aborting bronze extraction")
                report = ValidationReport(
                    validator_name="run_esco_bronze",
                    env=config.platform.environment,
                    run_id=ctx.run_id,
                    checks=all_results,
                    status=CheckStatus.FAIL,
                )
                report.artifacts.update(artifacts)
                report.artifacts["phase_stopped"] = "landing_validation"
                local_path, _ = finalize_report(report, config=config, upload_s3=upload)
                if not quiet:
                    click.echo(f"\n  ✗ Landing validation failed ({_phase_elapsed(t0)})\n")
                    print_footer(report, str(local_path))
                finalize_logging()
                sys.exit(ExitCode.BRONZE_FAILURE)

            if not quiet:
                click.echo(f"  ✓ Landing validation passed ({_phase_elapsed(t0)})")

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


# ---------------------------------------------------------------------------
# Run Gold pipeline (matching + analytics + validation)
# ---------------------------------------------------------------------------


@run_group.command("gold")
@click.option("--ingestion-date", "ingestion_date", required=True, help="Adzuna date YYYY-MM-DD.")
@click.option("--country", required=True, help="Country code (e.g. fr).")
@click.option("--esco-version", "esco_version", required=True, help="ESCO version (e.g. v1.2.1).")
@click.option("--esco-lang", "esco_lang", required=True, help="ESCO language (e.g. fr).")
@click.option("--job-limit", "job_limit", default=None, type=int, help="Debug: limit Adzuna jobs.")
@click.option("--upload", is_flag=True, default=False, help="Upload reports to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_gold(
    ingestion_date: str,
    country: str,
    esco_version: str,
    esco_lang: str,
    job_limit: int | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Run full Gold pipeline: matching → analytics → validation.

    Orchestrates:
    1. Gold matching (job-skill + job-occupation)
    2. Gold analytics (KPIs + occupation-skill graph)
    3. Gold validation (quality checks on outputs)
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Gold requires Spark/Iceberg.")
        sys.exit(ExitCode.UNEXPECTED)

    ctx = init_logging("run_gold", enable_file=True)
    set_context(dataset="gold")
    config = load_platform_config()

    all_results: list[CheckResult] = []
    artifacts: dict[str, str] = {
        "ingestion_date": ingestion_date,
        "country": country,
        "esco_version": esco_version,
        "esco_lang": esco_lang,
    }

    if not quiet:
        print_header("run_gold", ctx.run_id, config.platform.environment)

    spark = None
    try:
        spark = SparkSession.builder.appName("run_gold").getOrCreate()
        artifacts["spark_app_id"] = spark.sparkContext.applicationId
        set_context(spark_app_id=spark.sparkContext.applicationId)

        from skill_radar.domains.gold.analytics.orchestrator import run_gold_analytics
        from skill_radar.domains.gold.matching.orchestrator import run_gold_matching
        from skill_radar.platform.validate.models import create_check

        # Phase 1: Gold matching
        if not quiet:
            click.echo("\n  ── Phase 1: Gold Matching ──\n")

        match_result = run_gold_matching(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            esco_version=esco_version,
            esco_lang=esco_lang,
            job_limit=job_limit,
            run_id=ctx.run_id,
        )

        match_check = create_check(
            name="run.gold.matching",
            description="Gold matching pipeline",
            passed=match_result.success,
            detail=(
                f"skills={match_result.job_skill_matches_count} "
                f"occs={match_result.job_occupation_matches_count}"
                if match_result.success
                else match_result.error
            ),
        )
        all_results.append(match_check)
        if not quiet:
            print_check_result(match_check)

        if not match_result.success:
            report = ValidationReport(
                validator_name="run_gold",
                env=config.platform.environment,
                run_id=ctx.run_id,
                checks=all_results,
                status=CheckStatus.FAIL,
            )
            report.artifacts.update(artifacts)
            report.artifacts["phase_stopped"] = "matching"
            local_path, _ = finalize_report(report, config=config, upload_s3=upload)
            if not quiet:
                click.echo("\n  ✗ Gold matching failed\n")
                print_footer(report, str(local_path))
            finalize_logging()
            sys.exit(ExitCode.UNEXPECTED)

        # Phase 2: Gold analytics
        if not quiet:
            click.echo("\n  ── Phase 2: Gold Analytics ──\n")

        analytics_result = run_gold_analytics(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            esco_version=esco_version,
            esco_lang=esco_lang,
            run_id=ctx.run_id,
        )

        analytics_check = create_check(
            name="run.gold.analytics",
            description="Gold analytics pipeline",
            passed=analytics_result.success,
            detail=(
                f"demand={analytics_result.skill_demand_rows} "
                f"salary={analytics_result.salary_by_skill_rows} "
                f"graph={analytics_result.occupation_skill_graph_rows}"
                if analytics_result.success
                else analytics_result.error
            ),
        )
        all_results.append(analytics_check)
        if not quiet:
            print_check_result(analytics_check)

        if not analytics_result.success:
            report = ValidationReport(
                validator_name="run_gold",
                env=config.platform.environment,
                run_id=ctx.run_id,
                checks=all_results,
                status=CheckStatus.FAIL,
            )
            report.artifacts.update(artifacts)
            report.artifacts["phase_stopped"] = "analytics"
            local_path, _ = finalize_report(report, config=config, upload_s3=upload)
            if not quiet:
                click.echo("\n  ✗ Gold analytics failed\n")
                print_footer(report, str(local_path))
            finalize_logging()
            sys.exit(ExitCode.UNEXPECTED)

        # Phase 3: Gold validation
        if not quiet:
            click.echo("\n  ── Phase 3: Gold Validation ──\n")

        from skill_radar.platform.validate.checks.gold import get_gold_checks

        gold_checks = get_gold_checks(
            spark,
            config,
            ingestion_date=ingestion_date,
            country=country,
            esco_version=esco_version,
            esco_lang=esco_lang,
        )

        for named_check in gold_checks:
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
            validator_name="run_gold",
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
            sys.exit(ExitCode.UNEXPECTED)
        else:
            sys.exit(ExitCode.OK)

    finally:
        if spark:
            spark.stop()


# ---------------------------------------------------------------------------
# Run Adzuna bronze (extraction + validation)
# ---------------------------------------------------------------------------


@run_group.command("adzuna-bronze")
@click.option("--preset", default=None, help="Extraction preset name (default: from config).")
@click.option("--country", default=None, help="Country code override (default: from config).")
@click.option(
    "--max-pages",
    "max_pages",
    default=None,
    type=int,
    help="Max pages to fetch (default: from config).",
)
@click.option(
    "--results-per-page",
    "results_per_page",
    default=None,
    type=int,
    help="Results per page (default: from config).",
)
@click.option(
    "--ingestion-date",
    "ingestion_date",
    default=None,
    help="Ingestion date YYYY-MM-DD (default: today).",
)
@click.option("--upload", is_flag=True, default=False, help="Upload reports to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_adzuna_bronze(
    preset: str | None,
    country: str | None,
    max_pages: int | None,
    results_per_page: int | None,
    ingestion_date: str | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Run Adzuna bronze stage unit: extraction + validation.

    Executes inside a single Spark session:
    1. Fetch job postings from Adzuna API → Iceberg Bronze
    2. Validate bronze tables
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Adzuna bronze requires Spark/Iceberg.")
        sys.exit(ExitCode.BRONZE_FAILURE)

    from skill_radar.domains.adzuna.bronze.extract import (
        run_bronze_extraction,
        upload_run_summary,
    )

    ctx = init_logging("run_adzuna_bronze", enable_file=True)
    set_context(dataset="adzuna")
    config = load_platform_config()

    all_results: list[CheckResult] = []
    artifacts: dict[str, str] = {}
    if country:
        artifacts["country"] = country
    if ingestion_date:
        artifacts["ingestion_date"] = ingestion_date

    if not quiet:
        print_header("run_adzuna_bronze", ctx.run_id, config.platform.environment)

    spark = None
    try:
        spark = SparkSession.builder.appName("run_adzuna_bronze").getOrCreate()
        artifacts["spark_app_id"] = spark.sparkContext.applicationId
        set_context(spark_app_id=spark.sparkContext.applicationId)

        # Phase 1 ── Bronze extraction
        t1 = _phase_banner("Phase 1: Adzuna Bronze Extraction", quiet)

        extract_result = run_bronze_extraction(
            spark,
            preset=preset,
            country=country,
            max_pages=max_pages,
            results_per_page=results_per_page,
            ingestion_date=ingestion_date,
            run_id=ctx.run_id,
        )

        from skill_radar.platform.validate.models import create_check

        extract_check = create_check(
            name="run.adzuna_bronze.extract",
            description="Adzuna bronze extraction",
            passed=extract_result.success,
            detail=(
                f"{extract_result.rows_written} rows, "
                f"{extract_result.pages_fetched} pages → {extract_result.target_table}"
                if extract_result.success
                else (extract_result.error or "Unknown error")
            ),
        )
        all_results.append(extract_check)
        if not quiet:
            print_check_result(extract_check)

        if not extract_result.success:
            report = ValidationReport(
                validator_name="run_adzuna_bronze",
                env=config.platform.environment,
                run_id=ctx.run_id,
                checks=all_results,
                status=CheckStatus.FAIL,
            )
            report.artifacts.update(artifacts)
            report.artifacts["phase_stopped"] = "extraction"
            local_path, _ = finalize_report(report, config=config, upload_s3=upload)
            if not quiet:
                click.echo(f"\n  ✗ Bronze extraction failed ({_phase_elapsed(t1)})\n")
                print_footer(report, str(local_path))
            finalize_logging()
            sys.exit(ExitCode.BRONZE_FAILURE)

        upload_run_summary(extract_result)
        artifacts["country"] = extract_result.country
        artifacts["ingestion_date"] = extract_result.ingestion_date
        if not quiet:
            click.echo(f"  ✓ Bronze extraction done ({_phase_elapsed(t1)})")

        # Phase 2 ── Bronze validation
        t2 = _phase_banner("Phase 2: Adzuna Bronze Validation", quiet)

        from skill_radar.platform.validate.checks.adzuna import (
            get_bronze_checks as get_adzuna_bronze_checks,
        )

        checks = get_adzuna_bronze_checks(
            spark,
            config,
            country=extract_result.country,
            ingestion_date=extract_result.ingestion_date,
        )

        for named_check in checks:
            result = named_check.fn()
            all_results.append(result)
            if not quiet:
                print_check_result(result)

        if not quiet:
            click.echo(f"  ✓ Bronze validation done ({_phase_elapsed(t2)})")

        # ── Final report
        failed = [r for r in all_results if r.status == CheckStatus.FAIL]
        warned = [r for r in all_results if r.status == CheckStatus.WARN]

        status = CheckStatus.FAIL if failed else CheckStatus.WARN if warned else CheckStatus.PASS

        report = ValidationReport(
            validator_name="run_adzuna_bronze",
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
        sys.exit(ExitCode.OK if not failed else ExitCode.BRONZE_FAILURE)

    finally:
        if spark:
            spark.stop()


# ---------------------------------------------------------------------------
# Run Adzuna silver (formatting + validation)
# ---------------------------------------------------------------------------


@run_group.command("adzuna-silver")
@click.option("--country", default=None, help="Country code (default: from config).")
@click.option(
    "--ingestion-date",
    "ingestion_date",
    default=None,
    help="Ingestion date YYYY-MM-DD (default: today).",
)
@click.option("--upload", is_flag=True, default=False, help="Upload reports to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_adzuna_silver(
    country: str | None,
    ingestion_date: str | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Run Adzuna silver stage unit: formatting + validation.

    Executes inside a single Spark session:
    1. Normalize / deduplicate Bronze → Silver Iceberg
    2. Validate silver tables
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Adzuna silver requires Spark/Iceberg.")
        sys.exit(ExitCode.SILVER_FAILURE)

    from skill_radar.domains.adzuna.silver.format import (
        run_silver_format,
        upload_run_summary,
    )

    ctx = init_logging("run_adzuna_silver", enable_file=True)
    set_context(dataset="adzuna")
    config = load_platform_config()

    all_results: list[CheckResult] = []
    artifacts: dict[str, str] = {}
    if country:
        artifacts["country"] = country
    if ingestion_date:
        artifacts["ingestion_date"] = ingestion_date

    if not quiet:
        print_header("run_adzuna_silver", ctx.run_id, config.platform.environment)

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("run_adzuna_silver")
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        artifacts["spark_app_id"] = spark.sparkContext.applicationId
        set_context(spark_app_id=spark.sparkContext.applicationId)

        # Phase 1 ── Silver formatting
        t1 = _phase_banner("Phase 1: Adzuna Silver Formatting", quiet)

        format_result = run_silver_format(
            spark,
            country=country,
            ingestion_date=ingestion_date,
            run_id=ctx.run_id,
        )

        from skill_radar.platform.validate.models import create_check

        format_check = create_check(
            name="run.adzuna_silver.format",
            description="Adzuna silver formatting",
            passed=format_result.success,
            detail=(
                f"{format_result.input_row_count} → {format_result.output_row_count} rows "
                f"({format_result.duplicates_removed} dupes removed)"
                if format_result.success
                else (format_result.error or "Unknown error")
            ),
        )
        all_results.append(format_check)
        if not quiet:
            print_check_result(format_check)

        if not format_result.success:
            report = ValidationReport(
                validator_name="run_adzuna_silver",
                env=config.platform.environment,
                run_id=ctx.run_id,
                checks=all_results,
                status=CheckStatus.FAIL,
            )
            report.artifacts.update(artifacts)
            report.artifacts["phase_stopped"] = "formatting"
            local_path, _ = finalize_report(report, config=config, upload_s3=upload)
            if not quiet:
                click.echo(f"\n  ✗ Silver formatting failed ({_phase_elapsed(t1)})\n")
                print_footer(report, str(local_path))
            finalize_logging()
            sys.exit(ExitCode.SILVER_FAILURE)

        upload_run_summary(format_result)
        artifacts["country"] = format_result.country
        artifacts["ingestion_date"] = format_result.ingestion_date
        if not quiet:
            click.echo(f"  ✓ Silver formatting done ({_phase_elapsed(t1)})")

        # Phase 2 ── Silver validation
        t2 = _phase_banner("Phase 2: Adzuna Silver Validation", quiet)

        from skill_radar.platform.validate.checks.adzuna import (
            get_silver_checks as get_adzuna_silver_checks,
        )

        checks = get_adzuna_silver_checks(
            spark,
            config,
            country=format_result.country,
            ingestion_date=format_result.ingestion_date,
        )

        for named_check in checks:
            result = named_check.fn()
            all_results.append(result)
            if not quiet:
                print_check_result(result)

        if not quiet:
            click.echo(f"  ✓ Silver validation done ({_phase_elapsed(t2)})")

        # ── Final report
        failed = [r for r in all_results if r.status == CheckStatus.FAIL]
        warned = [r for r in all_results if r.status == CheckStatus.WARN]

        status = CheckStatus.FAIL if failed else CheckStatus.WARN if warned else CheckStatus.PASS

        report = ValidationReport(
            validator_name="run_adzuna_silver",
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
        sys.exit(ExitCode.OK if not failed else ExitCode.SILVER_FAILURE)

    finally:
        if spark:
            spark.stop()


# ---------------------------------------------------------------------------
# Run ESCO silver (formatting + validation)
# ---------------------------------------------------------------------------


@run_group.command("esco-silver")
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.1)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option("--entities", default=None, help="Comma-separated entities subset (default: all)")
@click.option("--upload", is_flag=True, default=False, help="Upload reports to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_esco_silver(
    version: str,
    lang: str,
    entities: str | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Run ESCO silver stage unit: formatting + validation.

    Executes inside a single Spark session:
    1. Format Bronze → Silver Iceberg tables
    2. Validate silver tables (schema, uniqueness, referential integrity)
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. ESCO silver requires Spark/Iceberg.")
        sys.exit(ExitCode.SILVER_FAILURE)

    from skill_radar.domains.esco.silver.format import (
        run_silver_format,
        upload_run_summary,
    )

    ctx = init_logging("run_esco_silver", enable_file=True)
    set_context(dataset="esco", version=version, lang=lang)
    config = load_platform_config()

    all_results: list[CheckResult] = []
    artifacts: dict[str, str] = {"version": version, "lang": lang}

    if not quiet:
        print_header("run_esco_silver", ctx.run_id, config.platform.environment)

    spark = None
    entity_list = entities.split(",") if entities else None

    try:
        spark = (
            SparkSession.builder.appName(f"run_esco_silver_{version}_{lang}")
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        artifacts["spark_app_id"] = spark.sparkContext.applicationId
        set_context(spark_app_id=spark.sparkContext.applicationId)

        # Phase 1 ── Silver formatting
        t1 = _phase_banner("Phase 1: ESCO Silver Formatting", quiet)

        format_result = run_silver_format(
            spark,
            version=version,
            lang=lang,
            entities=entity_list,
            dry_run=False,
            run_id=ctx.run_id,
        )

        from skill_radar.platform.validate.models import create_check

        if format_result.success:
            for er in format_result.entities:
                entity_check = create_check(
                    name=f"run.esco_silver.format.{er.entity}",
                    description=f"Format {er.entity} → Silver",
                    passed=True,
                    detail=(
                        f"{er.input_row_count} → {er.output_row_count} rows "
                        f"({er.duplicates_removed} dupes) → {er.table}"
                    ),
                )
                all_results.append(entity_check)
                if not quiet:
                    print_check_result(entity_check)
            upload_run_summary(format_result)
        else:
            for er in format_result.entities:
                entity_check = create_check(
                    name=f"run.esco_silver.format.{er.entity}",
                    description=f"Format {er.entity} → Silver",
                    passed=er.status == "success",
                    detail=er.error or f"{er.output_row_count} rows",
                )
                all_results.append(entity_check)
                if not quiet:
                    print_check_result(entity_check)

            report = ValidationReport(
                validator_name="run_esco_silver",
                env=config.platform.environment,
                run_id=ctx.run_id,
                checks=all_results,
                status=CheckStatus.FAIL,
            )
            report.artifacts.update(artifacts)
            report.artifacts["phase_stopped"] = "formatting"
            local_path, _ = finalize_report(report, config=config, upload_s3=upload)
            if not quiet:
                click.echo(f"\n  ✗ Silver formatting failed ({_phase_elapsed(t1)})\n")
                print_footer(report, str(local_path))
            finalize_logging()
            sys.exit(ExitCode.SILVER_FAILURE)

        if not quiet:
            click.echo(f"  ✓ Silver formatting done ({_phase_elapsed(t1)})")

        # Phase 2 ── Silver validation
        t2 = _phase_banner("Phase 2: ESCO Silver Validation", quiet)

        from skill_radar.platform.validate.checks.esco import get_silver_checks

        silver_checks = get_silver_checks(spark, config, version, lang, entities=entity_list)

        for named_check in silver_checks:
            result = named_check.fn()
            all_results.append(result)
            if not quiet:
                print_check_result(result)

        if not quiet:
            click.echo(f"  ✓ Silver validation done ({_phase_elapsed(t2)})")

        # ── Final report
        failed = [r for r in all_results if r.status == CheckStatus.FAIL]
        warned = [r for r in all_results if r.status == CheckStatus.WARN]

        status = CheckStatus.FAIL if failed else CheckStatus.WARN if warned else CheckStatus.PASS

        report = ValidationReport(
            validator_name="run_esco_silver",
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
        sys.exit(ExitCode.OK if not failed else ExitCode.SILVER_FAILURE)

    finally:
        if spark:
            spark.stop()


# ---------------------------------------------------------------------------
# Run Search (export + validation)
# ---------------------------------------------------------------------------


@run_group.command("search")
@click.option(
    "--ingestion-date", "ingestion_date", required=True, help="Partition date YYYY-MM-DD."
)
@click.option("--country", required=True, help="Country code (e.g. fr).")
@click.option("--es-url", "es_url", default=None, help="Elasticsearch URL override.")
@click.option("--upload", is_flag=True, default=False, help="Upload reports to S3 logs bucket")
@click.option("--quiet", is_flag=True, default=False, help="Suppress console output")
def run_search_cmd(
    ingestion_date: str,
    country: str,
    es_url: str | None,
    upload: bool,
    quiet: bool,
) -> None:
    """Run search stage unit: export to Elasticsearch + validation.

    Executes inside a single Spark session:
    1. Export Gold tables to Elasticsearch
    2. Validate Elasticsearch indices (health, docs, mappings)
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Search export requires Spark/Iceberg.")
        sys.exit(ExitCode.SEARCH_FAILURE)

    from skill_radar.domains.search.export import run_search_export

    ctx = init_logging("run_search", enable_file=True)
    set_context(dataset="search")
    config = load_platform_config()

    all_results: list[CheckResult] = []
    artifacts: dict[str, str] = {
        "ingestion_date": ingestion_date,
        "country": country,
    }

    if not quiet:
        print_header("run_search", ctx.run_id, config.platform.environment)

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("run_search")
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        artifacts["spark_app_id"] = spark.sparkContext.applicationId
        set_context(spark_app_id=spark.sparkContext.applicationId)

        # Phase 1 ── Search export
        t1 = _phase_banner("Phase 1: Search Export to Elasticsearch", quiet)

        from skill_radar.cli.search import PRIMARY_DATASETS

        export_result = run_search_export(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            datasets=PRIMARY_DATASETS,
            config=config,
            es_url=es_url,
            create_index=True,
            refresh=True,
            alias_swap=True,
            dry_run=False,
            run_id=ctx.run_id,
        )

        from skill_radar.platform.validate.models import create_check

        export_check = create_check(
            name="run.search.export",
            description="Search export to Elasticsearch",
            passed=export_result.success,
            detail=(
                f"{export_result.datasets_exported} datasets exported"
                if export_result.success
                else (export_result.error or "Unknown error")
            ),
        )
        all_results.append(export_check)
        if not quiet:
            print_check_result(export_check)

        if not export_result.success:
            report = ValidationReport(
                validator_name="run_search",
                env=config.platform.environment,
                run_id=ctx.run_id,
                checks=all_results,
                status=CheckStatus.FAIL,
            )
            report.artifacts.update(artifacts)
            report.artifacts["phase_stopped"] = "export"
            local_path, _ = finalize_report(report, config=config, upload_s3=upload)
            if not quiet:
                click.echo(f"\n  ✗ Search export failed ({_phase_elapsed(t1)})\n")
                print_footer(report, str(local_path))
            finalize_logging()
            sys.exit(ExitCode.SEARCH_FAILURE)

        if not quiet:
            click.echo(f"  ✓ Search export done ({_phase_elapsed(t1)})")

        # Phase 2 ── Search validation
        t2 = _phase_banner("Phase 2: Search Validation", quiet)

        from skill_radar.platform.validate.checks.search import get_search_checks

        search_checks = get_search_checks(
            config,
            ingestion_date=ingestion_date,
            country=country,
            es_url=es_url,
        )

        for named_check in search_checks:
            result = named_check.fn()
            all_results.append(result)
            if not quiet:
                print_check_result(result)

        if not quiet:
            click.echo(f"  ✓ Search validation done ({_phase_elapsed(t2)})")

        # ── Final report
        failed = [r for r in all_results if r.status == CheckStatus.FAIL]
        warned = [r for r in all_results if r.status == CheckStatus.WARN]

        status = CheckStatus.FAIL if failed else CheckStatus.WARN if warned else CheckStatus.PASS

        report = ValidationReport(
            validator_name="run_search",
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
        sys.exit(ExitCode.OK if not failed else ExitCode.SEARCH_FAILURE)

    finally:
        if spark:
            spark.stop()
