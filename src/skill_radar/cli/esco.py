"""CLI commands for ESCO dataset operations."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

from skill_radar.domains.esco.intake import run_intake
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
