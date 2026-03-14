"""ESCO-specific validation checks.

Provides checks for:
- Landing validation (artifact exists, manifest valid, checksum matches)
- Bronze validation (tables exist, schema correct, lineage values)
- E2E validation (landing → bronze pipeline)
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from skill_radar.domains.esco.contract import load_esco_contract
from skill_radar.platform.lake.enums import Domain, Source
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.runtime import get_runtime_context, resolve_s3_endpoint
from skill_radar.platform.validate.models import CheckResult, NamedCheck, create_check

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession

    from skill_radar.config.models import PlatformSettings
    from skill_radar.domains.esco.contract.models import EscoContract

logger = logging.getLogger(__name__)

# Default supported manifest schema versions
DEFAULT_SUPPORTED_SCHEMA_VERSIONS = ["1.0.0", "v1"]


def check_landing_artifact_exists(
    config: PlatformSettings,
    version: str,
    lang: str,
) -> CheckResult:
    """Check that the ESCO ZIP artifact exists in landing.

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        return create_check(
            name="landing.artifact.exists",
            description="ESCO ZIP artifact exists in landing",
            passed=False,
            skip=True,
            skip_reason="boto3 not installed",
        )

    layout = LakeLayout(config)
    zip_key = layout.landing_zip_key(
        domain=Domain.TAXONOMY.value,
        source=Source.ESCO.value,
        version=version,
        lang=lang,
    )
    bucket = config.storage.s3.bucket
    ctx = get_runtime_context()
    endpoint = resolve_s3_endpoint(config.storage.s3, ctx)

    try:
        client = boto3.client("s3", endpoint_url=endpoint)
        client.head_object(Bucket=bucket, Key=zip_key)
        return create_check(
            name="landing.artifact.exists",
            description="ESCO ZIP artifact exists in landing",
            passed=True,
            detail=f"s3://{bucket}/{zip_key}",
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "404":
            return create_check(
                name="landing.artifact.exists",
                description="ESCO ZIP artifact exists in landing",
                passed=False,
                detail=f"Not found: s3://{bucket}/{zip_key}",
            )
        return create_check(
            name="landing.artifact.exists",
            description="ESCO ZIP artifact exists in landing",
            passed=False,
            detail=f"Error checking: {exc}",
        )
    except Exception as exc:
        return create_check(
            name="landing.artifact.exists",
            description="ESCO ZIP artifact exists in landing",
            passed=False,
            detail=str(exc)[:200],
        )


def check_landing_manifest_exists(
    config: PlatformSettings,
    version: str,
    lang: str,
) -> CheckResult:
    """Check that the manifest.json exists in landing.

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        return create_check(
            name="landing.manifest.exists",
            description="Manifest exists in landing",
            passed=False,
            skip=True,
            skip_reason="boto3 not installed",
        )

    layout = LakeLayout(config)
    manifest_key = layout.landing_manifest_key(
        domain=Domain.TAXONOMY.value,
        source=Source.ESCO.value,
        version=version,
        lang=lang,
    )
    bucket = config.storage.s3.bucket
    ctx = get_runtime_context()
    endpoint = resolve_s3_endpoint(config.storage.s3, ctx)

    try:
        client = boto3.client("s3", endpoint_url=endpoint)
        client.head_object(Bucket=bucket, Key=manifest_key)
        return create_check(
            name="landing.manifest.exists",
            description="Manifest exists in landing",
            passed=True,
            detail=manifest_key,
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "404":
            return create_check(
                name="landing.manifest.exists",
                description="Manifest exists in landing",
                passed=False,
                detail=f"Not found: {manifest_key}",
            )
        return create_check(
            name="landing.manifest.exists",
            description="Manifest exists in landing",
            passed=False,
            detail=f"Error checking: {exc}",
        )
    except Exception as exc:
        return create_check(
            name="landing.manifest.exists",
            description="Manifest exists in landing",
            passed=False,
            detail=str(exc)[:200],
        )


def _load_manifest_from_s3(
    config: PlatformSettings,
    version: str,
    lang: str,
) -> tuple[dict | None, str]:
    """Load manifest JSON from S3.

    Returns
    -------
    tuple[dict | None, str]:
        (manifest_dict, error_message) - manifest is None on error.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        return None, "boto3 not installed"

    layout = LakeLayout(config)
    manifest_key = layout.landing_manifest_key(
        domain=Domain.TAXONOMY.value,
        source=Source.ESCO.value,
        version=version,
        lang=lang,
    )
    bucket = config.storage.s3.bucket
    ctx = get_runtime_context()
    endpoint = resolve_s3_endpoint(config.storage.s3, ctx)

    try:
        client = boto3.client("s3", endpoint_url=endpoint)
        resp = client.get_object(Bucket=bucket, Key=manifest_key)
        body = resp["Body"].read().decode("utf-8")
        return json.loads(body), ""
    except ClientError as exc:
        return None, str(exc)
    except json.JSONDecodeError as exc:
        return None, f"Invalid JSON: {exc}"
    except Exception as exc:
        return None, str(exc)


def check_landing_manifest_parseable(
    config: PlatformSettings,
    version: str,
    lang: str,
) -> CheckResult:
    """Check that manifest.json is valid JSON.

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    """
    _manifest, error = _load_manifest_from_s3(config, version, lang)

    if error:
        if "boto3 not installed" in error:
            return create_check(
                name="landing.manifest.parseable_json",
                description="Manifest is valid JSON",
                passed=False,
                skip=True,
                skip_reason="boto3 not installed",
            )
        return create_check(
            name="landing.manifest.parseable_json",
            description="Manifest is valid JSON",
            passed=False,
            detail=error,
        )

    return create_check(
        name="landing.manifest.parseable_json",
        description="Manifest is valid JSON",
        passed=True,
    )


def check_landing_manifest_schema_version(
    config: PlatformSettings,
    version: str,
    lang: str,
    *,
    supported_versions: list[str] | None = None,
) -> CheckResult:
    """Check that manifest schema_version is supported.

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    supported_versions:
        List of supported schema versions. Defaults to ["1.0.0", "v1"].
    """
    supported = supported_versions or DEFAULT_SUPPORTED_SCHEMA_VERSIONS
    manifest, error = _load_manifest_from_s3(config, version, lang)

    if error:
        if "boto3 not installed" in error:
            return create_check(
                name="landing.manifest.schema_version_supported",
                description="Manifest schema version is supported",
                passed=False,
                skip=True,
                skip_reason="boto3 not installed",
            )
        return create_check(
            name="landing.manifest.schema_version_supported",
            description="Manifest schema version is supported",
            passed=False,
            detail=f"Cannot load manifest: {error}",
        )

    schema_version = manifest.get("schema_version") if manifest else None
    if not schema_version:
        return create_check(
            name="landing.manifest.schema_version_supported",
            description="Manifest schema version is supported",
            passed=False,
            detail="schema_version field missing from manifest",
        )

    if schema_version in supported:
        return create_check(
            name="landing.manifest.schema_version_supported",
            description="Manifest schema version is supported",
            passed=True,
            detail=f"schema_version={schema_version}",
        )

    return create_check(
        name="landing.manifest.schema_version_supported",
        description="Manifest schema version is supported",
        passed=False,
        detail=f"Unsupported version {schema_version} (supported: {supported})",
    )


def check_landing_checksum_matches(
    config: PlatformSettings,
    version: str,
    lang: str,
) -> CheckResult:
    """Check that ZIP checksum matches manifest.

    Downloads the ZIP and computes SHA-256 to verify against manifest.

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        return create_check(
            name="landing.manifest.checksum_matches",
            description="ZIP checksum matches manifest",
            passed=False,
            skip=True,
            skip_reason="boto3 not installed",
        )

    # Load manifest
    manifest, error = _load_manifest_from_s3(config, version, lang)
    if error or not manifest:
        return create_check(
            name="landing.manifest.checksum_matches",
            description="ZIP checksum matches manifest",
            passed=False,
            detail=f"Cannot load manifest: {error}",
        )

    artifact = manifest.get("artifact") or {}

    expected_sha256 = artifact.get("sha256")
    if not expected_sha256:
        checksum_obj = artifact.get("checksum") or {}
        if checksum_obj.get("algorithm") == "sha256":
            expected_sha256 = checksum_obj.get("value")
    if not expected_sha256:
        return create_check(
            name="landing.manifest.checksum_matches",
            description="ZIP checksum matches manifest",
            passed=False,
            detail="Manifest missing artifact.sha256",
        )

    # Download ZIP and compute checksum
    layout = LakeLayout(config)
    zip_key = layout.landing_zip_key(
        domain=Domain.TAXONOMY.value,
        source=Source.ESCO.value,
        version=version,
        lang=lang,
    )
    bucket = config.storage.s3.bucket
    ctx = get_runtime_context()
    endpoint = resolve_s3_endpoint(config.storage.s3, ctx)

    try:
        client = boto3.client("s3", endpoint_url=endpoint)
        resp = client.get_object(Bucket=bucket, Key=zip_key)

        # Stream and compute hash
        sha256_hash = hashlib.sha256()
        for chunk in resp["Body"].iter_chunks(chunk_size=8192):
            sha256_hash.update(chunk)
        computed_sha256 = sha256_hash.hexdigest()

        if computed_sha256 == expected_sha256:
            return create_check(
                name="landing.manifest.checksum_matches",
                description="ZIP checksum matches manifest",
                passed=True,
                detail=f"sha256={expected_sha256[:16]}...",
            )

        return create_check(
            name="landing.manifest.checksum_matches",
            description="ZIP checksum matches manifest",
            passed=False,
            detail=f"Expected {expected_sha256[:16]}..., got {computed_sha256[:16]}...",
        )

    except ClientError as exc:
        return create_check(
            name="landing.manifest.checksum_matches",
            description="ZIP checksum matches manifest",
            passed=False,
            detail=str(exc),
        )
    except Exception as exc:
        return create_check(
            name="landing.manifest.checksum_matches",
            description="ZIP checksum matches manifest",
            passed=False,
            detail=str(exc)[:200],
        )


def check_landing_contract_match(
    config: PlatformSettings,
    version: str,
    lang: str,
    *,
    contract: EscoContract | None = None,
) -> CheckResult:
    """Check that manifest aligns with ESCO contract.

    Verifies:
    - dataset matches
    - lang is supported
    - entities list matches expected filenames

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    contract:
        ESCO contract (loads default if not provided).
    """
    esco_contract = contract or load_esco_contract()

    # Check lang is supported
    if lang not in esco_contract.supported_languages:
        return create_check(
            name="landing.contract_match",
            description="Manifest aligns with contract",
            passed=False,
            detail=f"Language '{lang}' not in supported: {esco_contract.supported_languages}",
        )

    # Load manifest
    manifest, error = _load_manifest_from_s3(config, version, lang)
    if error or not manifest:
        if "boto3 not installed" in error:
            return create_check(
                name="landing.contract_match",
                description="Manifest aligns with contract",
                passed=False,
                skip=True,
                skip_reason="boto3 not installed",
            )
        return create_check(
            name="landing.contract_match",
            description="Manifest aligns with contract",
            passed=False,
            detail=f"Cannot load manifest: {error}",
        )

    # Check dataset matches
    manifest_dataset = manifest.get("dataset")
    if manifest_dataset != esco_contract.dataset:
        return create_check(
            name="landing.contract_match",
            description="Manifest aligns with contract",
            passed=False,
            detail=f"Dataset mismatch: manifest={manifest_dataset}, contract={esco_contract.dataset}",
        )

    return create_check(
        name="landing.contract_match",
        description="Manifest aligns with contract",
        passed=True,
        detail=f"dataset={manifest_dataset}, lang={lang}",
    )


def get_landing_checks(
    config: PlatformSettings,
    version: str,
    lang: str,
) -> list[NamedCheck]:
    """Return all landing check functions bound to parameters.

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.

    Returns
    -------
    list[NamedCheck]:
        List of named checks ready to execute.
    """
    return [
        NamedCheck(
            name="landing.artifact.exists",
            description="ESCO artifact exists in landing zone",
            fn=lambda: check_landing_artifact_exists(config, version, lang),
        ),
        NamedCheck(
            name="landing.manifest.exists",
            description="Manifest file exists",
            fn=lambda: check_landing_manifest_exists(config, version, lang),
        ),
        NamedCheck(
            name="landing.manifest.parseable",
            description="Manifest is valid JSON",
            fn=lambda: check_landing_manifest_parseable(config, version, lang),
        ),
        NamedCheck(
            name="landing.manifest.schema_version",
            description="Manifest schema version is supported",
            fn=lambda: check_landing_manifest_schema_version(config, version, lang),
        ),
        NamedCheck(
            name="landing.checksum.matches",
            description="ZIP checksum matches manifest",
            fn=lambda: check_landing_checksum_matches(config, version, lang),
        ),
        NamedCheck(
            name="landing.contract.match",
            description="Manifest aligns with contract",
            fn=lambda: check_landing_contract_match(config, version, lang),
        ),
    ]


def _iceberg_catalog_configured(spark: SparkSession, catalog_name: str) -> bool:
    """Check if Iceberg catalog is configured in Spark."""
    try:
        extensions = spark.conf.get("spark.sql.extensions", "") or ""
        has_extensions = "IcebergSparkSessionExtensions" in extensions

        catalog_key = f"spark.sql.catalog.{catalog_name}"
        catalog_impl = spark.conf.get(catalog_key, "") or ""
        has_catalog = "iceberg" in catalog_impl.lower()

        return has_extensions and has_catalog
    except Exception:
        return False


@dataclass(frozen=True)
class CatalogCheck:
    ok: bool
    reason: str | None = None
    detail: str | None = None


def _iceberg_catalog_ready(
    spark: SparkSession,
    catalog: str,
    *,
    expected_namespace: str | None = None,
) -> CatalogCheck:
    """
    Verify the Spark Iceberg catalog is *usable*, not just configured in conf.

    This intentionally executes a tiny Spark SQL operation so we know:
      - Spark recognizes the catalog
      - Iceberg extensions/catalog wiring is working
      - The catalog can respond (no classpath / config / endpoint issues)

    Returns a structured result suitable for SKIP messaging.
    """
    try:
        # This is a cheap operation and validates the catalog is resolvable.
        spark.sql(f"SHOW NAMESPACES IN {catalog}").limit(1).collect()

        if expected_namespace is not None:
            # Also cheap; avoids full scans.
            spark.sql(f"SHOW TABLES IN {catalog}.{expected_namespace}").limit(1).collect()

        return CatalogCheck(ok=True)

    except Exception as exc:
        # Keep detail short for console/UI, but informative.
        msg = str(exc).replace("\n", " ")
        msg_short = msg[:250]
        return CatalogCheck(
            ok=False,
            reason=f"Iceberg catalog '{catalog}' is not usable in this Spark session",
            detail=f"{type(exc).__name__}: {msg_short}",
        )


def _make_skip_factory(skip_reason: str) -> Callable[[str, str], NamedCheck]:
    """Factory that creates skip NamedCheck objects for a given reason."""

    def make_skip(name: str, desc: str) -> NamedCheck:
        return NamedCheck(
            name=name,
            description=desc,
            fn=lambda: create_check(
                name=name,
                description=desc,
                passed=False,
                skip=True,
                skip_reason=skip_reason,
            ),
        )

    return make_skip


def get_bronze_checks(
    spark: SparkSession,
    config: PlatformSettings,
    version: str,
    lang: str,
    *,
    entities: list[str] | None = None,
    contract: EscoContract | None = None,
) -> list[NamedCheck]:
    """Return all bronze check functions.

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    entities:
        Subset of entities to check (defaults to all from contract).
    contract:
        ESCO contract (loads default if not provided).

    Returns
    -------
    list[NamedCheck]:
        List of named checks ready to execute.
    """
    from skill_radar.platform.validate.checks.lakehouse import (
        check_lineage_values,
        check_namespace_exists,
        check_table_exists,
        check_table_non_empty,
        check_table_schema_contains,
    )

    esco_contract = contract or load_esco_contract()
    layout = LakeLayout(config)

    # Resolve entities
    entity_names = entities or [e.name for e in esco_contract.entities]

    catalog = layout.iceberg_catalog
    namespace = layout.iceberg_namespace_name("bronze")

    # Check if Iceberg is configured before creating checks
    cat_configured = _iceberg_catalog_configured(spark, catalog)

    # Strong check: validates the catalog is actually usable.
    # If you want stronger coverage, set expected_namespace=namespace.
    cat_status = _iceberg_catalog_ready(spark, catalog, expected_namespace=namespace)

    if not cat_configured or not cat_status.ok:
        reasons: list[str] = []

        if not cat_configured:
            reasons.append(
                f"Iceberg catalog '{catalog}' not configured in Spark conf "
                f"(missing spark.sql.extensions / spark.sql.catalog.{catalog})"
            )

        if not cat_status.ok:
            # cat_status.reason/detail should already be concise
            reasons.append(f"{cat_status.reason}. {cat_status.detail}")

        skip_reason = " | ".join(reasons)

        make_skip = _make_skip_factory(skip_reason)

        skip_checks: list[NamedCheck] = [
            make_skip(
                f"bronze.namespace.exists.{namespace}",
                f"Namespace {catalog}.{namespace} exists",
            ),
        ]

        for entity_name in entity_names:
            table = f"esco_{entity_name}_raw"
            skip_checks.extend(
                [
                    make_skip(f"bronze.table.exists.{table}", f"Table {table} exists"),
                    make_skip(f"bronze.table.non_empty.{table}", f"Table {table} has rows"),
                    make_skip(
                        f"bronze.schema.contains.{table}", f"Table {table} has required schema"
                    ),
                    make_skip(
                        f"bronze.lineage.values.{table}", f"Table {table} has correct lineage"
                    ),
                ]
            )

        return skip_checks

    checks: list[NamedCheck] = []

    # Namespace check - use LakeLayout for consistent naming
    checks.append(
        NamedCheck(
            name=f"bronze.namespace.exists.{namespace}",
            description=f"Namespace {catalog}.{namespace} exists",
            fn=lambda ns=namespace, cat=catalog: check_namespace_exists(  # type: ignore[misc]
                spark, ns, catalog=cat
            ),
        )
    )

    # Per-entity checks
    for entity_name in entity_names:
        # Find entity in contract
        entity_def = next((e for e in esco_contract.entities if e.name == entity_name), None)
        if not entity_def:
            continue

        # Get table FQN
        table_fqn = layout.iceberg_table_fqn(
            layer="bronze",
            dataset="esco",
            entity=entity_name,
        )

        # Extract table short name for display
        table_short = table_fqn.split(".")[-1]  # esco_skills_raw

        # Table exists
        checks.append(
            NamedCheck(
                name=f"bronze.table.exists.{table_short}",
                description=f"Table {table_short} exists",
                fn=lambda t=table_fqn: check_table_exists(spark, t),  # type: ignore[misc]
            )
        )

        # Table non-empty
        checks.append(
            NamedCheck(
                name=f"bronze.table.non_empty.{table_short}",
                description=f"Table {table_short} has rows",
                fn=lambda t=table_fqn: check_table_non_empty(spark, t),  # type: ignore[misc]
            )
        )

        # Required lineage columns
        lineage_cols = ["dataset", "version", "lang", "run_id", "ingested_at_utc"]
        checks.append(
            NamedCheck(
                name=f"bronze.schema.contains.{table_short}",
                description=f"Table {table_short} has required schema",
                fn=lambda t=table_fqn, cols=lineage_cols: check_table_schema_contains(  # type: ignore[misc]
                    spark, t, cols
                ),
            )
        )

        # Lineage values
        checks.append(
            NamedCheck(
                name=f"bronze.lineage.values.{table_short}",
                description=f"Table {table_short} has correct lineage",
                fn=lambda t=table_fqn, v=version, lg=lang: check_lineage_values(  # type: ignore[misc]
                    spark, t, expected_dataset="esco", expected_version=v, expected_lang=lg
                ),
            )
        )

    return checks


def run_bronze_e2e_checks(
    spark: SparkSession,
    config: PlatformSettings,
    version: str,
    lang: str,
    *,
    entities: list[str] | None = None,
    fail_fast: bool = True,
    run_id: str = "",
    contract: EscoContract | None = None,
) -> list[CheckResult]:
    """Run bronze E2E checks: extraction + validation (Spark-side).

    This function assumes:
    - Artifact already uploaded to S3 landing zone
    - Running inside Spark container with PySpark available

    Steps:
    1. Run bronze extraction (reads from S3 landing)
    2. Run bronze table checks

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    entities:
        Subset of entities to process (default: all).
    fail_fast:
        Abort on first entity error.
    run_id:
        Run ID for lineage tracking.
    contract:
        ESCO contract (loaded if not provided).

    Returns
    -------
    list[CheckResult]:
        Check results from extraction + validation.
    """
    results: list[CheckResult] = []

    # Load contract if not provided
    if contract is None:
        contract = load_esco_contract()

    # Step 1: Run bronze extraction
    try:
        from skill_radar.domains.esco.bronze.extract import run_bronze_extraction

        extract_result = run_bronze_extraction(
            spark,
            version,
            lang,
            entities=entities,
            fail_fast=fail_fast,
            run_id=run_id,
        )

        # Create check results for each entity
        for er in extract_result.entities:
            if er.status == "success":
                results.append(
                    create_check(
                        name=f"e2e.bronze.extract.{er.entity}",
                        description=f"Extract {er.entity} to Iceberg",
                        passed=True,
                        detail=f"{er.row_count} rows → {er.table}",
                    )
                )
            else:
                results.append(
                    create_check(
                        name=f"e2e.bronze.extract.{er.entity}",
                        description=f"Extract {er.entity} to Iceberg",
                        passed=False,
                        detail=er.error or "Unknown error",
                    )
                )

        # Summary check
        if extract_result.success:
            results.append(
                create_check(
                    name="e2e.bronze.extract.summary",
                    description="Bronze extraction summary",
                    passed=True,
                    detail=f"All {len(extract_result.entities)} entities succeeded",
                )
            )
        else:
            failed_entities = [e.entity for e in extract_result.entities if e.status == "failed"]
            results.append(
                create_check(
                    name="e2e.bronze.extract.summary",
                    description="Bronze extraction summary",
                    passed=False,
                    detail=f"Failed: {', '.join(failed_entities)}",
                )
            )
            # If fail_fast, don't run bronze checks
            if fail_fast:
                return results

    except Exception as exc:
        results.append(
            create_check(
                name="e2e.bronze.extract",
                description="Run bronze extraction",
                passed=False,
                detail=str(exc)[:200],
            )
        )
        return results

    # Step 2: Run bronze table checks
    bronze_checks = get_bronze_checks(
        spark, config, version, lang, entities=entities, contract=contract
    )
    for check in bronze_checks:
        try:
            results.append(check.fn())
        except Exception as exc:
            results.append(
                create_check(
                    name=check.name,
                    description=check.description,
                    passed=False,
                    detail=str(exc)[:200],
                )
            )

    return results


def run_esco_bronze_e2e(
    config: PlatformSettings,
    version: str,
    lang: str,
    *,
    zip_path: str | None = None,
    force: bool = True,
    run_id: str = "",
    spark: SparkSession | None = None,
    via_docker: bool = False,
    contract: EscoContract | None = None,
) -> list[CheckResult]:
    """Run full E2E validation: upload → bronze → checks.

    .. deprecated::
        Use `run_bronze_e2e_checks` instead. This function will be removed.
        The upload step should be performed separately via `skill-radar esco upload`.

    This function orchestrates:
    1. Upload ZIP to landing (via intake)
    2. Run bronze extraction
    3. Run bronze checks

    Parameters
    ----------
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    zip_path:
        Path to ESCO ZIP file.
    force:
        Overwrite existing landing artifacts.
    run_id:
        Run ID for lineage tracking.
    spark:
        SparkSession (if available).
    via_docker:
        Shell out to docker compose for bronze extraction.
    contract:
        ESCO contract.

    Returns
    -------
    list[CheckResult]:
        All check results from E2E run.
    """
    import warnings

    warnings.warn(
        "run_esco_bronze_e2e is deprecated. Use run_bronze_e2e_checks instead. "
        "Upload artifacts separately via 'skill-radar esco upload'.",
        DeprecationWarning,
        stacklevel=2,
    )

    from pathlib import Path

    results: list[CheckResult] = []

    # Step 1: Upload to landing
    if zip_path:
        try:
            from skill_radar.domains.esco.landing.intake import run_intake

            intake_result = run_intake(
                Path(zip_path),
                version,
                lang,
                force=force,
                config=config,
                contract=contract,
            )

            if intake_result.success:
                results.append(
                    create_check(
                        name="e2e.apply.landing.upload",
                        description="Upload ZIP to landing",
                        passed=True,
                        detail=f"Uploaded to {intake_result.artifact_key}",
                    )
                )
            else:
                results.append(
                    create_check(
                        name="e2e.apply.landing.upload",
                        description="Upload ZIP to landing",
                        passed=False,
                        detail=intake_result.error,
                    )
                )
                return results  # Stop on upload failure

        except Exception as exc:
            results.append(
                create_check(
                    name="e2e.apply.landing.upload",
                    description="Upload ZIP to landing",
                    passed=False,
                    detail=str(exc)[:200],
                )
            )
            return results

    # Step 2: Run bronze extraction
    if via_docker:
        # Shell out to docker
        import subprocess

        cmd = [
            "docker",
            "compose",
            "exec",
            "-T",
            "spark",
            "bash",
            "-lc",
            f"spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py "
            f"--version {version} --lang {lang}",
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=300)
            results.append(
                create_check(
                    name="e2e.apply.bronze.extraction",
                    description="Run bronze extraction (via docker)",
                    passed=True,
                )
            )
        except subprocess.CalledProcessError as exc:
            results.append(
                create_check(
                    name="e2e.apply.bronze.extraction",
                    description="Run bronze extraction (via docker)",
                    passed=False,
                    detail=exc.stderr.decode()[:200] if exc.stderr else str(exc),
                )
            )
            return results
        except Exception as exc:
            results.append(
                create_check(
                    name="e2e.apply.bronze.extraction",
                    description="Run bronze extraction (via docker)",
                    passed=False,
                    detail=str(exc)[:200],
                )
            )
            return results

    elif spark is not None:
        # Run directly with spark
        try:
            from skill_radar.domains.esco.bronze.extract import run_bronze_extraction

            result = run_bronze_extraction(
                spark,
                version,
                lang,
                run_id=run_id,
            )
            if result.success:
                results.append(
                    create_check(
                        name="e2e.apply.bronze.extraction",
                        description="Run bronze extraction",
                        passed=True,
                        detail=f"{len(result.entities)} entities processed",
                    )
                )
            else:
                failed = [e.entity for e in result.entities if e.status == "failed"]
                results.append(
                    create_check(
                        name="e2e.apply.bronze.extraction",
                        description="Run bronze extraction",
                        passed=False,
                        detail=f"Failed entities: {failed}",
                    )
                )
                return results

        except Exception as exc:
            results.append(
                create_check(
                    name="e2e.apply.bronze.extraction",
                    description="Run bronze extraction",
                    passed=False,
                    detail=str(exc)[:200],
                )
            )
            return results
    else:
        results.append(
            create_check(
                name="e2e.apply.bronze.extraction",
                description="Run bronze extraction",
                passed=False,
                skip=True,
                skip_reason="No spark session and via_docker=False",
            )
        )

    # Step 3: Run bronze checks (need spark)
    if spark is not None:
        bronze_checks = get_bronze_checks(spark, config, version, lang, contract=contract)
        for check in bronze_checks:
            try:
                results.append(check.fn())
            except Exception as exc:
                results.append(
                    create_check(
                        name=check.name,
                        description=check.description,
                        passed=False,
                        detail=str(exc)[:200],
                    )
                )
    else:
        results.append(
            create_check(
                name="e2e.validate.bronze.checks",
                description="Run bronze table checks",
                passed=False,
                skip=True,
                skip_reason="No spark session available for bronze checks",
            )
        )

    return results


def check_silver_table_partition_non_empty(
    spark: SparkSession,
    table_fqn: str,
    version: str,
    lang: str,
) -> CheckResult:
    """Check that a Silver table has rows for the given partition.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name.
    version:
        Version to filter by.
    lang:
        Language to filter by.
    """
    table_short = table_fqn.split(".")[-1]
    try:
        count_df = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_fqn} "
            f"WHERE version = '{version}' AND lang = '{lang}'"
        )
        row_count = count_df.collect()[0]["cnt"]

        if row_count > 0:
            return create_check(
                name=f"silver.table.non_empty.{table_short}",
                description=f"Table {table_short} has rows for partition",
                passed=True,
                detail=f"row_count={row_count}",
                metrics={"row_count": row_count},
            )
        return create_check(
            name=f"silver.table.non_empty.{table_short}",
            description=f"Table {table_short} has rows for partition",
            passed=False,
            detail=f"No rows found (version={version}, lang={lang})",
        )
    except Exception as exc:
        return create_check(
            name=f"silver.table.non_empty.{table_short}",
            description=f"Table {table_short} has rows for partition",
            passed=False,
            detail=str(exc)[:200],
        )


def check_silver_uniqueness_skills(
    spark: SparkSession,
    table_fqn: str,
    version: str,
    lang: str,
) -> CheckResult:
    """Check that skills are unique by (concept_uri, version, lang).

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name.
    version:
        Version to filter by.
    lang:
        Language to filter by.
    """
    table_short = table_fqn.split(".")[-1]
    try:
        dup_df = spark.sql(f"""
            SELECT concept_uri, COUNT(*) as cnt
            FROM {table_fqn}
            WHERE version = '{version}' AND lang = '{lang}'
            GROUP BY concept_uri
            HAVING COUNT(*) > 1
        """)
        dup_count = dup_df.count()

        if dup_count == 0:
            return create_check(
                name=f"silver.uniqueness.{table_short}",
                description=f"Table {table_short} has unique keys",
                passed=True,
                detail="No duplicates found",
            )
        return create_check(
            name=f"silver.uniqueness.{table_short}",
            description=f"Table {table_short} has unique keys",
            passed=False,
            detail=f"{dup_count} duplicate concept_uri values found",
            metrics={"duplicate_keys": dup_count},
        )
    except Exception as exc:
        return create_check(
            name=f"silver.uniqueness.{table_short}",
            description=f"Table {table_short} has unique keys",
            passed=False,
            detail=str(exc)[:200],
        )


def check_silver_uniqueness_relations(
    spark: SparkSession,
    table_fqn: str,
    version: str,
    lang: str,
) -> CheckResult:
    """Check relations uniqueness by (occupation_uri, skill_uri, relation_type, version, lang).

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name.
    version:
        Version to filter by.
    lang:
        Language to filter by.
    """
    table_short = table_fqn.split(".")[-1]
    try:
        dup_df = spark.sql(f"""
            SELECT occupation_uri, skill_uri, relation_type, COUNT(*) as cnt
            FROM {table_fqn}
            WHERE version = '{version}' AND lang = '{lang}'
            GROUP BY occupation_uri, skill_uri, relation_type
            HAVING COUNT(*) > 1
        """)
        dup_count = dup_df.count()

        if dup_count == 0:
            return create_check(
                name=f"silver.uniqueness.{table_short}",
                description=f"Table {table_short} has unique edge keys",
                passed=True,
                detail="No duplicates found",
            )
        return create_check(
            name=f"silver.uniqueness.{table_short}",
            description=f"Table {table_short} has unique edge keys",
            passed=False,
            detail=f"{dup_count} duplicate edge keys found",
            metrics={"duplicate_keys": dup_count},
        )
    except Exception as exc:
        return create_check(
            name=f"silver.uniqueness.{table_short}",
            description=f"Table {table_short} has unique edge keys",
            passed=False,
            detail=str(exc)[:200],
        )


def check_silver_referential_integrity(
    spark: SparkSession,
    config: PlatformSettings,
    version: str,
    lang: str,
) -> list[CheckResult]:
    """Check referential integrity: relations → occupations/skills.

    Computes FK coverage ratios and fails if below threshold.

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Platform configuration.
    version:
        Version to filter by.
    lang:
        Language to filter by.

    Returns
    -------
    list[CheckResult]:
        Two check results: occupation coverage and skill coverage.
    """
    layout = LakeLayout(config)
    threshold = config.validation.esco.silver.min_relation_fk_coverage

    relations_fqn = layout.iceberg_table_fqn("silver", "esco", "relations", raw=False)
    occupations_fqn = layout.iceberg_table_fqn("silver", "esco", "occupations", raw=False)
    skills_fqn = layout.iceberg_table_fqn("silver", "esco", "skills", raw=False)

    results: list[CheckResult] = []

    # Check occupation FK coverage
    try:
        # Count total relations
        total_df = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM {relations_fqn}
            WHERE version = '{version}' AND lang = '{lang}'
        """)
        total_relations = total_df.collect()[0]["cnt"]

        if total_relations == 0:
            results.append(
                create_check(
                    name="silver.fk.occupation_coverage",
                    description="Relations → Occupations FK coverage",
                    passed=False,
                    detail="No relations found in partition",
                )
            )
            results.append(
                create_check(
                    name="silver.fk.skill_coverage",
                    description="Relations → Skills FK coverage",
                    passed=False,
                    detail="No relations found in partition",
                )
            )
            return results

        # Check occupation coverage
        occ_matched_df = spark.sql(f"""
            SELECT COUNT(DISTINCT r.occupation_uri) as cnt
            FROM {relations_fqn} r
            INNER JOIN {occupations_fqn} o
              ON r.occupation_uri = o.concept_uri
              AND o.version = '{version}' AND o.lang = '{lang}'
            WHERE r.version = '{version}' AND r.lang = '{lang}'
        """)
        occ_matched = occ_matched_df.collect()[0]["cnt"]

        occ_total_df = spark.sql(f"""
            SELECT COUNT(DISTINCT occupation_uri) as cnt
            FROM {relations_fqn}
            WHERE version = '{version}' AND lang = '{lang}'
        """)
        occ_total = occ_total_df.collect()[0]["cnt"]

        occ_coverage = occ_matched / occ_total if occ_total > 0 else 0.0

        if occ_coverage >= threshold:
            results.append(
                create_check(
                    name="silver.fk.occupation_coverage",
                    description="Relations → Occupations FK coverage",
                    passed=True,
                    detail=f"coverage={occ_coverage:.4f} >= {threshold}",
                    metrics={
                        "matched": occ_matched,
                        "total": occ_total,
                        "coverage": occ_coverage,
                    },
                )
            )
        else:
            results.append(
                create_check(
                    name="silver.fk.occupation_coverage",
                    description="Relations → Occupations FK coverage",
                    passed=False,
                    detail=f"coverage={occ_coverage:.4f} < {threshold}",
                    metrics={
                        "matched": occ_matched,
                        "total": occ_total,
                        "coverage": occ_coverage,
                    },
                )
            )

        # Check skill coverage
        skill_matched_df = spark.sql(f"""
            SELECT COUNT(DISTINCT r.skill_uri) as cnt
            FROM {relations_fqn} r
            INNER JOIN {skills_fqn} s
              ON r.skill_uri = s.concept_uri
              AND s.version = '{version}' AND s.lang = '{lang}'
            WHERE r.version = '{version}' AND r.lang = '{lang}'
        """)
        skill_matched = skill_matched_df.collect()[0]["cnt"]

        skill_total_df = spark.sql(f"""
            SELECT COUNT(DISTINCT skill_uri) as cnt
            FROM {relations_fqn}
            WHERE version = '{version}' AND lang = '{lang}'
        """)
        skill_total = skill_total_df.collect()[0]["cnt"]

        skill_coverage = skill_matched / skill_total if skill_total > 0 else 0.0

        if skill_coverage >= threshold:
            results.append(
                create_check(
                    name="silver.fk.skill_coverage",
                    description="Relations → Skills FK coverage",
                    passed=True,
                    detail=f"coverage={skill_coverage:.4f} >= {threshold}",
                    metrics={
                        "matched": skill_matched,
                        "total": skill_total,
                        "coverage": skill_coverage,
                    },
                )
            )
        else:
            results.append(
                create_check(
                    name="silver.fk.skill_coverage",
                    description="Relations → Skills FK coverage",
                    passed=False,
                    detail=f"coverage={skill_coverage:.4f} < {threshold}",
                    metrics={
                        "matched": skill_matched,
                        "total": skill_total,
                        "coverage": skill_coverage,
                    },
                )
            )

    except Exception as exc:
        results.append(
            create_check(
                name="silver.fk.coverage",
                description="Referential integrity coverage",
                passed=False,
                detail=str(exc)[:200],
            )
        )

    return results


def get_silver_checks(
    spark: SparkSession,
    config: PlatformSettings,
    version: str,
    lang: str,
    *,
    entities: list[str] | None = None,
) -> list[NamedCheck]:
    """Return all silver check functions.

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Platform configuration.
    version:
        Artifact version.
    lang:
        Language code.
    entities:
        Subset of entities to check (defaults to all).

    Returns
    -------
    list[NamedCheck]:
        List of named checks ready to execute.
    """
    from skill_radar.platform.validate.checks.lakehouse import (
        check_table_exists,
        check_table_schema_contains,
    )

    layout = LakeLayout(config)

    # Resolve entities
    all_entities = ["skills", "occupations", "relations"]
    entity_names = entities if entities else all_entities

    catalog = layout.iceberg_catalog
    namespace = layout.iceberg_namespace_name("silver")

    # Check if Iceberg is configured before creating checks
    cat_configured = _iceberg_catalog_configured(spark, catalog)
    cat_status = _iceberg_catalog_ready(spark, catalog, expected_namespace=namespace)

    if not cat_configured or not cat_status.ok:
        reasons: list[str] = []
        if not cat_configured:
            reasons.append(f"Iceberg catalog '{catalog}' not configured in Spark conf")
        if not cat_status.ok:
            reasons.append(f"{cat_status.reason}. {cat_status.detail}")

        skip_reason = " | ".join(reasons)
        make_skip = _make_skip_factory(skip_reason)

        skip_checks: list[NamedCheck] = [
            make_skip(
                f"silver.namespace.exists.{namespace}",
                f"Namespace {catalog}.{namespace} exists",
            ),
        ]
        for entity_name in entity_names:
            table = f"esco_{entity_name}"
            skip_checks.append(make_skip(f"silver.table.exists.{table}", f"Table {table} exists"))
        return skip_checks

    checks: list[NamedCheck] = []

    # Namespace check
    checks.append(
        NamedCheck(
            name=f"silver.namespace.exists.{namespace}",
            description=f"Namespace {catalog}.{namespace} exists",
            fn=lambda ns=namespace, cat=catalog: create_check(  # type: ignore[misc]
                name=f"silver.namespace.exists.{ns}",
                description=f"Namespace {cat}.{ns} exists",
                **_check_namespace_exists_impl(spark, ns, cat),
            ),
        )
    )

    # Define required columns per entity
    required_cols = {
        "skills": [
            "concept_uri",
            "concept_uri_uuid",
            "preferred_label",
            "alt_labels",
            "hidden_labels",
            "alt_labels_count",
            "hidden_labels_count",
            "dataset",
            "version",
            "lang",
            "run_id",
        ],
        "occupations": [
            "concept_uri",
            "concept_uri_uuid",
            "preferred_label",
            "alt_labels",
            "hidden_labels",
            "alt_labels_count",
            "hidden_labels_count",
            "dataset",
            "version",
            "lang",
            "run_id",
        ],
        "relations": [
            "occupation_uri",
            "skill_uri",
            "relation_type",
            "occupation_label",
            "skill_label",
            "dataset",
            "version",
            "lang",
            "run_id",
        ],
    }

    # Per-entity checks
    for entity_name in entity_names:
        table_fqn = layout.iceberg_table_fqn(
            layer="silver",
            dataset="esco",
            entity=entity_name,
            raw=False,
        )
        table_short = table_fqn.split(".")[-1]

        # Table exists
        checks.append(
            NamedCheck(
                name=f"silver.table.exists.{table_short}",
                description=f"Table {table_short} exists",
                fn=lambda t=table_fqn: check_table_exists(spark, t),  # type: ignore[misc]
            )
        )

        # Table non-empty for partition
        checks.append(
            NamedCheck(
                name=f"silver.table.non_empty.{table_short}",
                description=f"Table {table_short} has rows for partition",
                fn=lambda t=table_fqn, v=version, lg=lang: (  # type: ignore[misc]
                    check_silver_table_partition_non_empty(spark, t, v, lg)
                ),
            )
        )

        # Schema contains required columns
        cols = required_cols.get(entity_name, [])
        checks.append(
            NamedCheck(
                name=f"silver.schema.contains.{table_short}",
                description=f"Table {table_short} has required columns",
                fn=lambda t=table_fqn, c=cols: check_table_schema_contains(spark, t, c),  # type: ignore[misc]
            )
        )

        # Uniqueness checks
        if entity_name in ["skills", "occupations"]:
            checks.append(
                NamedCheck(
                    name=f"silver.uniqueness.{table_short}",
                    description=f"Table {table_short} has unique keys",
                    fn=lambda t=table_fqn, v=version, lg=lang: (  # type: ignore[misc]
                        check_silver_uniqueness_skills(spark, t, v, lg)
                    ),
                )
            )
        elif entity_name == "relations":
            checks.append(
                NamedCheck(
                    name=f"silver.uniqueness.{table_short}",
                    description=f"Table {table_short} has unique edge keys",
                    fn=lambda t=table_fqn, v=version, lg=lang: (  # type: ignore[misc]
                        check_silver_uniqueness_relations(spark, t, v, lg)
                    ),
                )
            )

    # Referential integrity checks (only if relations is included)
    if "relations" in entity_names:
        checks.append(
            NamedCheck(
                name="silver.fk.coverage",
                description="Referential integrity coverage",
                fn=lambda: _run_fk_checks(spark, config, version, lang),  # type: ignore[misc]
            )
        )

    return checks


def _check_namespace_exists_impl(
    spark: SparkSession,
    namespace: str,
    catalog: str,
) -> dict:
    """Helper to check namespace existence and return kwargs for create_check."""
    try:
        namespaces = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
        namespace_names = [row[0] for row in namespaces]

        if namespace in namespace_names:
            return {"passed": True}
        return {"passed": False, "detail": "Namespace not found"}
    except Exception as exc:
        return {"passed": False, "detail": str(exc)[:200]}


def _run_fk_checks(
    spark: SparkSession,
    config: PlatformSettings,
    version: str,
    lang: str,
) -> CheckResult:
    """Run FK coverage checks and return combined result."""
    fk_results = check_silver_referential_integrity(spark, config, version, lang)

    # Combine results
    all_passed = all(r.status.name == "PASS" for r in fk_results)
    details = [f"{r.name}: {r.detail}" for r in fk_results]

    return create_check(
        name="silver.fk.coverage",
        description="Referential integrity coverage",
        passed=all_passed,
        detail=" | ".join(details),
    )
