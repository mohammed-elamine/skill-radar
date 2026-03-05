"""CLI commands for ESCO dataset operations."""

from __future__ import annotations

import hashlib
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
EXIT_FILE_NOT_FOUND = 6


# ---------------------------------------------------------------------------
# Dropzone file resolution
# ---------------------------------------------------------------------------
DEFAULT_DROPZONE_DIR = "/opt/skillradar/incoming"


def _compute_file_hash(file_path: Path, algorithm: str = "sha256") -> str:
    """Compute hash of a file."""
    hasher = hashlib.new(algorithm)
    with Path.open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _get_dropzone_candidates(dropzone_dir: Path, version: str, lang: str) -> list[Path]:
    """Return ordered list of candidate file paths in the dropzone.

    Candidates are checked in order of specificity (most general first):
      1) {dropzone}/esco/esco.zip
      2) {dropzone}/esco/esco_{lang}.zip
      3) {dropzone}/esco/{version}/esco.zip
      4) {dropzone}/esco/{version}/{lang}/esco.zip
    """
    esco_dir = dropzone_dir / "esco"
    return [
        esco_dir / "esco.zip",
        esco_dir / f"esco_{lang}.zip",
        esco_dir / version / "esco.zip",
        esco_dir / version / lang / "esco.zip",
    ]


def resolve_dropzone_file(
    dropzone_dir: Path,
    version: str,
    lang: str,
) -> Path | None:
    """Search dropzone for ESCO artifact, return first existing file or None."""
    candidates = _get_dropzone_candidates(dropzone_dir, version, lang)
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _format_missing_file_error(dropzone_dir: Path, version: str, lang: str) -> str:
    """Format helpful error message when no file found in dropzone."""
    candidates = _get_dropzone_candidates(dropzone_dir, version, lang)
    host_dropzone = "./data/incoming"
    container_dropzone = "/opt/skillradar/incoming"

    lines = [
        "Error: No ESCO artifact found in the dropzone.",
        "",
        "The ESCO dataset requires manual download due to authentication requirements.",
        "",
        "Steps to resolve:",
        "  1. Download the ESCO ZIP from the official portal (manual process).",
        "  2. Place the file in one of these host locations:",
    ]

    for candidate in candidates:
        # Convert container path to host path for display
        rel_path = str(candidate).replace(str(dropzone_dir), "").lstrip("/")
        lines.append(f"       {host_dropzone}/{rel_path}")

    lines.extend(
        [
            "",
            "  3. Run upload inside the Spark container:",
            f"       make upload-esco VERSION={version} LANG={lang}",
            "",
            "     Or manually:",
            "       docker compose exec -T spark bash -lc \\",
            f'         "uv run skill-radar esco upload --version {version} --lang {lang}"',
            "",
            f"Container sees: {container_dropzone}/esco/...",
            "",
            "Checked paths (all missing):",
        ]
    )

    for candidate in candidates:
        lines.append(f"  - {candidate}")

    return "\n".join(lines)


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
    required=False,
    default=None,
    type=click.Path(exists=True, path_type=Path),
    help="Path to ESCO ZIP file (optional if using dropzone)",
)
@click.option(
    "--dropzone-dir",
    "dropzone_dir",
    default=DEFAULT_DROPZONE_DIR,
    type=click.Path(path_type=Path),
    show_default=True,
    help="Dropzone directory to search for ESCO files",
)
@click.option("--dry-run", is_flag=True, default=False, help="Validate without uploading")
@click.option("--force", is_flag=True, default=False, help="Overwrite existing artifact")
def upload(
    version: str,
    lang: str,
    file_path: Path | None,
    dropzone_dir: Path,
    dry_run: bool,
    force: bool,
) -> None:
    """Upload an ESCO artifact to the landing zone.

    File resolution order:\n
      1) If --file is provided, use that path directly.\n
      2) Otherwise, search the dropzone directory for:\n
         - {dropzone}/esco/esco.zip\n
         - {dropzone}/esco/esco_{lang}.zip\n
         - {dropzone}/esco/{version}/esco.zip\n
         - {dropzone}/esco/{version}/{lang}/esco.zip\n

    Typical workflow (manual dropzone):\n
      1. Download ESCO ZIP manually from the official portal\n
      2. Place in ./data/incoming/esco/esco.zip on host\n
      3. Run: make upload-esco VERSION=v1.2.1 LANG=fr\n
    """
    # Resolve file path
    resolved_path: Path | None = file_path

    if resolved_path is None:
        resolved_path = resolve_dropzone_file(dropzone_dir, version, lang)
        if resolved_path is None:
            click.echo(_format_missing_file_error(dropzone_dir, version, lang))
            sys.exit(EXIT_FILE_NOT_FOUND)

    # Initialize logging after file resolution (avoid logging setup on early exit)
    init_logging("esco_intake_upload", enable_file=not dry_run)
    set_context(dataset="esco", version=version, lang=lang)

    # Log resolved file details
    file_size = resolved_path.stat().st_size
    file_hash = _compute_file_hash(resolved_path)
    logger.info(
        "Resolved ESCO artifact: path=%s size=%d sha256=%s",
        resolved_path,
        file_size,
        file_hash,
    )
    click.echo(f"  File: {resolved_path}")
    click.echo(f"  Size: {file_size:,} bytes")
    click.echo(f"  SHA256: {file_hash[:16]}...")

    result = run_intake(resolved_path, version, lang, dry_run=dry_run, force=force)

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
