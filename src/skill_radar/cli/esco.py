"""CLI commands for ESCO dataset operations."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

from skill_radar.domains.esco.landing.intake import run_intake
from skill_radar.platform.logging import finalize_logging, init_logging, set_context

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------
EXIT_SUCCESS = 0
EXIT_VALIDATION_FAILURE = 2
EXIT_UPLOAD_ERROR = 3
EXIT_IDEMPOTENCY_CONFLICT = 4


# ---------------------------------------------------------------------------
# Command group
# ---------------------------------------------------------------------------


@click.group("esco")
def esco_group() -> None:
    """ESCO dataset operations."""


@esco_group.command()
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.1)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option(
    "--file",
    "file_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to ESCO ZIP file",
)
@click.option("--dry-run", is_flag=True, default=False, help="Validate without uploading")
@click.option("--force", is_flag=True, default=False, help="Overwrite existing artifact")
def upload(
    version: str,
    lang: str,
    file_path: Path,
    dry_run: bool,
    force: bool,
) -> None:
    """Upload an ESCO artifact to the landing zone."""
    init_logging("esco_intake_upload", enable_file=not dry_run)
    set_context(dataset="esco", version=version, lang=lang)

    result = run_intake(file_path, version, lang, dry_run=dry_run, force=force)

    if result.success:
        click.echo("")
        click.echo("  Upload successful" if not dry_run else "  [DRY-RUN] Validation passed")
        click.echo(f"    Bucket   : {result.bucket}")
        click.echo(f"    Artifact : {result.artifact_key}")
        click.echo(f"    Manifest : {result.manifest_key}")
        click.echo(f"    Checksum : {result.checksum}")
        click.echo("")
        click.echo("  Next step: run Bronze extraction to ingest CSVs into Iceberg tables.")
        finalize_logging()
        sys.exit(EXIT_SUCCESS)

    # --- Failure paths -----------------------------------------------------
    click.echo("")
    click.echo(f"  Intake failed: {result.error}")

    if result.validation and not result.validation.passed:
        click.echo("  Validation failures:")
        for c in result.validation.checks:
            if not c.passed:
                click.echo(f"    - [{c.name}] {c.message}")
        finalize_logging()
        sys.exit(EXIT_VALIDATION_FAILURE)

    if "already exists" in result.error.lower():
        finalize_logging()
        sys.exit(EXIT_IDEMPOTENCY_CONFLICT)

    finalize_logging()
    sys.exit(EXIT_UPLOAD_ERROR)


# ---------------------------------------------------------------------------
# Bronze extraction (delegates to Spark entrypoint logic)
# ---------------------------------------------------------------------------

EXIT_BRONZE_ERROR = 5


@esco_group.command()
@click.option("--version", required=True, help="Artifact version (e.g. v1.2.0)")
@click.option("--lang", required=True, help="Language code (e.g. fr)")
@click.option(
    "--entities",
    default=None,
    help="Comma-separated subset of entities (default: all)",
)
@click.option("--dry-run", is_flag=True, default=False, help="Validate plan without writing")
@click.option(
    "--fail-fast/--no-fail-fast",
    default=True,
    help="Abort on first entity error",
)
def bronze(
    version: str,
    lang: str,
    entities: str | None,
    dry_run: bool,
    fail_fast: bool,
) -> None:
    """Run ESCO Bronze extraction (CSV → Iceberg).

    Requires the Spark container (run via *docker compose exec spark*)
    or a local Spark installation with the Iceberg catalog configured.

    Note: This command is a convenience wrapper.  In production, use
    ``spark-submit jobs/esco/bronze_esco_to_iceberg.py`` directly.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo(
            "Error: PySpark is not installed in this environment.\n"
            "Use spark-submit inside the Spark container instead:\n\n"
            "  docker compose exec -T spark bash -lc \\\n"
            '    "spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py '
            f'--version {version} --lang {lang}"\n'
        )
        sys.exit(EXIT_BRONZE_ERROR)

    from skill_radar.domains.esco.bronze.extract import (
        run_bronze_extraction,
        upload_run_summary,
    )

    ctx = init_logging("esco_bronze_extract", enable_file=not dry_run)
    set_context(dataset="esco", version=version, lang=lang)

    spark = SparkSession.builder.appName(f"esco_bronze_{version}_{lang}").getOrCreate()
    set_context(spark_app_id=spark.sparkContext.applicationId)

    entity_list = entities.split(",") if entities else None

    try:
        result = run_bronze_extraction(
            spark,
            version,
            lang,
            entities=entity_list,
            fail_fast=fail_fast,
            dry_run=dry_run,
            run_id=ctx.run_id,
        )

        if result.success:
            click.echo("")
            if dry_run:
                click.echo("  [DRY-RUN] Bronze extraction plan validated.")
            else:
                click.echo("  Bronze extraction complete.")
                for er in result.entities:
                    click.echo(f"    {er.entity}: {er.row_count} rows → {er.table} [{er.status}]")
                upload_run_summary(result)
            finalize_logging()
            sys.exit(EXIT_SUCCESS)

        click.echo("")
        click.echo("  Bronze extraction failed:")
        for er in result.entities:
            if er.status == "failed":
                click.echo(f"    {er.entity}: {er.error}")
        finalize_logging()
        sys.exit(EXIT_BRONZE_ERROR)

    except Exception as exc:
        logger.exception("Bronze extraction failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_BRONZE_ERROR)
    finally:
        spark.stop()
