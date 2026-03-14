"""ESCO artifact intake orchestrator (control plane).

Orchestrates the full validated landing workflow: contract validation,
checksum computation, idempotency check, upload, and manifest generation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from skill_radar.config.loader import load_platform_config
from skill_radar.platform.lake.enums import Domain, Source
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.manifest.builder import ManifestBuilder
from skill_radar.platform.runtime import get_runtime_context, resolve_s3_endpoint
from skill_radar.platform.storage.exceptions import ObjectAlreadyExistsError
from skill_radar.platform.storage.s3_client import S3Client
from skill_radar.utils.hashing import sha256_file

from ..contract import load_esco_contract
from .validation import ValidationResult, validate_artifact

if TYPE_CHECKING:
    from pathlib import Path

    from skill_radar.config.models import PlatformSettings
    from skill_radar.domains.esco.contract.models import EscoContract

logger = logging.getLogger(__name__)


@dataclass
class IntakeResult:
    """Structured result of an intake operation."""

    success: bool
    bucket: str = ""
    artifact_key: str = ""
    manifest_key: str = ""
    checksum: str = ""
    validation: ValidationResult | None = None
    error: str = ""


def run_intake(
    zip_path: Path,
    version: str,
    lang: str,
    *,
    dry_run: bool = False,
    force: bool = False,
    config: PlatformSettings | None = None,
    contract: EscoContract | None = None,
) -> IntakeResult:
    """Execute the full ESCO artifact landing workflow.

    Parameters
    ----------
    zip_path:
        Local path to the ESCO ZIP file.
    version:
        Semantic version tag (e.g. ``v1.2.1``).
    lang:
        Language code (e.g. ``fr``).
    dry_run:
        If ``True``, validate and compute paths but do not upload.
    force:
        If ``True``, overwrite an existing artifact in the landing zone.
    config:
        Optional pre-loaded platform config (otherwise loaded from defaults).
    contract:
        Optional pre-loaded ESCO contract (otherwise loaded from YAML).
    """
    cfg = config or load_platform_config()
    esco_contract = contract or load_esco_contract()

    # --- 1. Validate -------------------------------------------------------
    logger.info(
        "Validating artifact: %s (version=%s, lang=%s)",
        zip_path.name,
        version,
        lang,
    )
    validation = validate_artifact(zip_path, version, lang, esco_contract)

    if not validation.passed:
        logger.error("Validation failed for %s", zip_path.name)
        return IntakeResult(
            success=False,
            validation=validation,
            error="Validation failed",
        )

    logger.info("Validation passed (%d checks)", len(validation.checks))

    # --- 2. Checksum -------------------------------------------------------
    checksum = sha256_file(zip_path)
    size_bytes = zip_path.stat().st_size
    logger.info("SHA-256: %s (%d bytes)", checksum, size_bytes)

    # --- 3. Build paths (single source of truth via LakeLayout) ------------
    layout = LakeLayout(cfg)
    artifact_key = layout.landing_zip_key(
        domain=Domain.TAXONOMY.value,
        source=Source.ESCO.value,
        version=version,
        lang=lang,
    )
    manifest_key = layout.landing_manifest_key(
        domain=Domain.TAXONOMY.value,
        source=Source.ESCO.value,
        version=version,
        lang=lang,
    )

    # --- 4. Dry-run --------------------------------------------------------
    if dry_run:
        logger.info(
            "[DRY-RUN] Would upload to s3://%s/%s",
            cfg.storage.s3.bucket,
            artifact_key,
        )
        return IntakeResult(
            success=True,
            bucket=cfg.storage.s3.bucket,
            artifact_key=artifact_key,
            manifest_key=manifest_key,
            checksum=checksum,
            validation=validation,
        )

    # --- 5. Upload ---------------------------------------------------------
    ctx = get_runtime_context()
    endpoint = resolve_s3_endpoint(cfg.storage.s3, ctx)
    s3 = S3Client(cfg, endpoint_url=endpoint)

    # Idempotency check
    if s3.object_exists(artifact_key) and not force:
        logger.warning(
            "Artifact already exists: s3://%s/%s",
            s3.bucket,
            artifact_key,
        )
        return IntakeResult(
            success=False,
            bucket=s3.bucket,
            artifact_key=artifact_key,
            checksum=checksum,
            validation=validation,
            error=(
                f"Artifact already exists at s3://{s3.bucket}/{artifact_key}. "
                "Use --force to overwrite."
            ),
        )

    try:
        s3.upload_file(zip_path, artifact_key, overwrite=force)
    except ObjectAlreadyExistsError:
        return IntakeResult(
            success=False,
            artifact_key=artifact_key,
            error="Idempotency conflict: object exists with different content.",
        )

    # --- 6. Manifest -------------------------------------------------------
    builder = ManifestBuilder(cfg)
    manifest = builder.build(
        dataset=esco_contract.dataset,
        artifact_type=esco_contract.artifact.type,
        content=esco_contract.artifact.content,
        file_type=esco_contract.artifact.file_type,
        version=version,
        language=lang,
        checksum=checksum,
        size_bytes=size_bytes,
        storage_key=artifact_key,
        provider=esco_contract.source.provider,
        acquisition_method=esco_contract.source.acquisition_method,
        provider_url=esco_contract.source.provider_url,
        validation_status="passed",
        validation_checks=[
            {"name": c.name, "passed": c.passed, "message": c.message} for c in validation.checks
        ],
    )
    manifest_bytes = ManifestBuilder.to_json(manifest)
    s3.upload_bytes(
        manifest_bytes,
        manifest_key,
        overwrite=force,
        content_type="application/json",
    )

    logger.info("Intake complete: s3://%s/%s", s3.bucket, artifact_key)

    return IntakeResult(
        success=True,
        bucket=s3.bucket,
        artifact_key=artifact_key,
        manifest_key=manifest_key,
        checksum=checksum,
        validation=validation,
    )
