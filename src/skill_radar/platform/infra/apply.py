"""Infrastructure provisioning (apply) operations.

Idempotent apply operations that create baseline platform objects:
- S3 buckets (lake + logs)
- Iceberg namespaces (sr_bronze, sr_silver, sr_gold)

All operations are idempotent (safe to re-run).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from skill_radar.platform.infra.requirements import get_platform_requirements
from skill_radar.platform.runtime import (
    RuntimeContext,
    get_runtime_context,
    resolve_s3_endpoint,
)
from skill_radar.platform.runtime.endpoints import format_endpoint_info
from skill_radar.platform.storage.s3_client import build_s3_client
from skill_radar.platform.validate.models import CheckResult, create_check

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


def _pyspark_available() -> bool:
    """Check if pyspark is available for import."""
    try:
        import pyspark  # noqa: F401

        return True
    except ImportError:
        return False


def ensure_buckets(
    config: PlatformSettings,
    *,
    buckets: list[str] | None = None,
    context: RuntimeContext | None = None,
) -> list[CheckResult]:
    """Ensure required S3 buckets exist, creating them if necessary.

    Parameters
    ----------
    config:
        Platform configuration.
    buckets:
        List of bucket names to ensure. Defaults to lake + logs buckets.
    context:
        Runtime context override (auto-detected if None).

    Returns
    -------
    list[CheckResult]:
        Results for each bucket operation.
    """
    results: list[CheckResult] = []

    from botocore.exceptions import ClientError

    # Resolve runtime context and endpoint
    ctx = get_runtime_context(context)
    s3_config = config.storage.s3
    endpoint = resolve_s3_endpoint(s3_config, ctx)
    format_endpoint_info(endpoint, ctx)
    region = s3_config.region

    # Get required buckets from requirements (single source of truth)
    reqs = get_platform_requirements(config)
    target_buckets = list(buckets) if buckets is not None else list(reqs.buckets)

    # Use default client for apply operations (more tolerant timeouts)
    client = build_s3_client(s3_config, endpoint, purpose="default")

    for bucket in target_buckets:
        try:
            # Check if bucket exists
            try:
                client.head_bucket(Bucket=bucket)
                results.append(
                    create_check(
                        name=f"apply.bucket.{bucket}",
                        description=f"Ensure bucket '{bucket}' exists",
                        passed=True,
                        detail="Already exists",
                    )
                )
                continue
            except ClientError as exc:
                error_code = exc.response.get("Error", {}).get("Code", "")
                if error_code not in {"404", "NoSuchBucket"}:
                    # Some other error
                    results.append(
                        create_check(
                            name=f"apply.bucket.{bucket}",
                            description=f"Ensure bucket '{bucket}' exists",
                            passed=False,
                            detail=f"Check failed: {error_code}",
                        )
                    )
                    continue

            # Bucket doesn't exist, create it
            # Note: MinIO ignores CreateBucketConfiguration for most cases
            try:
                if region == "us-east-1":
                    client.create_bucket(Bucket=bucket)
                else:
                    client.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={"LocationConstraint": region},
                    )

                results.append(
                    create_check(
                        name=f"apply.bucket.{bucket}",
                        description=f"Ensure bucket '{bucket}' exists",
                        passed=True,
                        detail="Created",
                    )
                )
                logger.info("Created bucket: %s", bucket)

            except ClientError as create_exc:
                results.append(
                    create_check(
                        name=f"apply.bucket.{bucket}",
                        description=f"Ensure bucket '{bucket}' exists",
                        passed=False,
                        detail=f"Create failed: {create_exc}",
                    )
                )

        except Exception as exc:
            results.append(
                create_check(
                    name=f"apply.bucket.{bucket}",
                    description=f"Ensure bucket '{bucket}' exists",
                    passed=False,
                    detail=str(exc)[:200],
                )
            )

    return results


def _iceberg_catalog_configured(spark: SparkSession, catalog_name: str) -> bool:
    """Check if Iceberg catalog is configured in Spark."""
    try:
        # Check for Iceberg extensions
        extensions = spark.conf.get("spark.sql.extensions", "") or ""
        has_extensions = "IcebergSparkSessionExtensions" in extensions

        # Check for catalog config
        catalog_key = f"spark.sql.catalog.{catalog_name}"
        catalog_impl = spark.conf.get(catalog_key, "") or ""
        has_catalog = "iceberg" in catalog_impl.lower()

        return has_extensions and has_catalog
    except Exception:
        return False


def ensure_namespaces(
    spark: SparkSession,
    config: PlatformSettings,
    namespaces: list[str] | None = None,
) -> list[CheckResult]:
    """Ensure Iceberg namespaces exist, creating them if necessary.

    Parameters
    ----------
    spark:
        Active SparkSession with Iceberg configured.
    config:
        Platform configuration with Iceberg catalog settings.
    namespaces:
        List of namespace names to ensure. Defaults to bronze/silver/gold.

    Returns
    -------
    list[CheckResult]:
        Results for each namespace operation.
    """
    results: list[CheckResult] = []

    # Get requirements from single source of truth
    reqs = get_platform_requirements(config)
    catalog = reqs.iceberg.catalog
    target_namespaces = list(namespaces) if namespaces is not None else list(reqs.namespaces)

    # Check if Iceberg catalog is configured before attempting operations
    if not _iceberg_catalog_configured(spark, catalog):
        for ns in target_namespaces:
            results.append(
                create_check(
                    name=f"apply.namespace.{ns}",
                    description=f"Ensure namespace '{catalog}.{ns}' exists",
                    passed=False,
                    skip=True,
                    skip_reason=f"Iceberg catalog '{catalog}' not configured (run inside Spark container)",
                )
            )
        return results

    # Catalog is configured, proceed with namespace operations
    try:
        existing_namespaces = set()
        try:
            ns_rows = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
            existing_namespaces = {row[0] for row in ns_rows}
        except Exception:
            # Catalog may not have any namespaces yet, that's OK
            pass

        for ns in target_namespaces:
            fqn = f"{catalog}.{ns}"

            if ns in existing_namespaces:
                results.append(
                    create_check(
                        name=f"apply.namespace.{ns}",
                        description=f"Ensure namespace '{fqn}' exists",
                        passed=True,
                        detail="Already exists",
                    )
                )
                continue

            # Create namespace
            try:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fqn}")
                results.append(
                    create_check(
                        name=f"apply.namespace.{ns}",
                        description=f"Ensure namespace '{fqn}' exists",
                        passed=True,
                        detail="Created",
                    )
                )
                logger.info("Created namespace: %s", fqn)

            except Exception as create_exc:
                results.append(
                    create_check(
                        name=f"apply.namespace.{ns}",
                        description=f"Ensure namespace '{fqn}' exists",
                        passed=False,
                        detail=str(create_exc)[:200],
                    )
                )

    except Exception as exc:
        results.append(
            create_check(
                name="apply.namespaces",
                description="Ensure Iceberg namespaces exist",
                passed=False,
                detail=str(exc)[:200],
            )
        )

    return results


def apply_infra(
    config: PlatformSettings,
    *,
    create_namespaces: bool = True,
    namespaces: list[str] | None = None,
    context: RuntimeContext | None = None,
) -> tuple[list[CheckResult], dict[str, str]]:
    """Apply all infrastructure provisioning.

    Operations:
    1. Ensure MinIO reachable
    2. Ensure required buckets exist
    3. Ensure Iceberg namespaces exist (if pyspark available)

    Parameters
    ----------
    config:
        Platform configuration.
    create_namespaces:
        Whether to create Iceberg namespaces (requires pyspark).
    namespaces:
        Custom namespace list, or None for defaults.
    context:
        Runtime context override (auto-detected if None).

    Returns
    -------
    tuple[list[CheckResult], dict[str, str]]:
        (results, artifacts) where results are check results and
        artifacts is a dict of metadata.
    """
    results: list[CheckResult] = []
    artifacts: dict[str, str] = {}

    # Resolve context once and store in artifacts
    ctx = get_runtime_context(context)
    artifacts["runtime_context"] = ctx.value

    # Step 1: Check MinIO reachable
    from skill_radar.platform.validate.checks.infra import check_minio_reachable

    minio_result = check_minio_reachable(config, context=ctx)
    results.append(minio_result)

    if minio_result.status.value == "FAIL":
        # Can't proceed without MinIO
        return results, artifacts

    # Step 2: Ensure buckets
    bucket_results = ensure_buckets(config, context=ctx)
    results.extend(bucket_results)

    # Check if any bucket creation failed
    bucket_failures = [r for r in bucket_results if r.status.value == "FAIL"]
    if bucket_failures:
        # Mark as critical but continue anyway
        pass

    # Step 3: Ensure namespaces (if pyspark available)
    if create_namespaces:
        if not _pyspark_available():
            results.append(
                create_check(
                    name="apply.namespaces",
                    description="Ensure Iceberg namespaces exist",
                    passed=True,  # Not a failure, just skipped
                    skip=True,
                    skip_reason="pyspark not installed - run inside Spark container",
                    warn=True,
                    warn_reason="Namespaces not created (pyspark unavailable)",
                )
            )
            artifacts["namespaces_skipped"] = "true"
        else:
            spark = None
            try:
                from pyspark.sql import SparkSession

                from skill_radar.platform.runtime.spark_compat import (
                    check_spark_pyspark_compatible,
                )

                spark = SparkSession.builder.appName("infra_apply").getOrCreate()
                artifacts["spark_app_id"] = spark.sparkContext.applicationId

                # Check version compatibility before proceeding
                compatible, pyspark_ver, spark_ver, compat_msg = check_spark_pyspark_compatible(
                    spark
                )
                artifacts["pyspark_version"] = pyspark_ver
                artifacts["spark_jvm_version"] = spark_ver

                if not compatible:
                    results.append(
                        create_check(
                            name="apply.namespaces",
                            description="Ensure Iceberg namespaces exist",
                            passed=False,
                            detail=compat_msg,
                        )
                    )
                    return results, artifacts

                ns_results = ensure_namespaces(spark, config, namespaces)
                results.extend(ns_results)

            except Exception as exc:
                results.append(
                    create_check(
                        name="apply.namespaces",
                        description="Ensure Iceberg namespaces exist",
                        passed=False,
                        detail=str(exc)[:200],
                    )
                )
            finally:
                if spark:
                    spark.stop()

    return results, artifacts
