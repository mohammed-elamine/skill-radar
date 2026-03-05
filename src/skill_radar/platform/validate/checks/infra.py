"""Infrastructure health checks (read-only).

Checks platform components are healthy before running dataset pipelines:
- MinIO/S3 reachable
- Required buckets exist
- Spark session works (skipped if pyspark unavailable)
- Iceberg configured (skipped if pyspark unavailable)
- S3A from Spark (skipped if pyspark unavailable)

All checks are read-only - they never create resources.
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
from skill_radar.platform.storage.s3_client import build_s3_client, check_s3_connectivity
from skill_radar.platform.validate.models import CheckResult, NamedCheck, create_check

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# boto3-based checks (run on host or container)
# ---------------------------------------------------------------------------


def check_minio_reachable(
    config: PlatformSettings,
    *,
    context: RuntimeContext | None = None,
) -> CheckResult:
    """Check that MinIO/S3 endpoint is reachable via boto3 list_buckets.

    Uses fast-fail client with 2s connect timeout for quick detection of
    unreachable endpoints.

    Parameters
    ----------
    config:
        Platform configuration with S3 endpoint.
    context:
        Runtime context override (auto-detected if None).
    """
    # Resolve runtime context and endpoint
    ctx = get_runtime_context(context)
    s3_config = config.storage.s3
    endpoint = resolve_s3_endpoint(s3_config, ctx)
    endpoint_info = format_endpoint_info(endpoint, ctx)

    # Use fast-fail connectivity check
    success, detail = check_s3_connectivity(s3_config, endpoint)

    if success:
        return create_check(
            name="infra.minio.reachable",
            description="MinIO/S3 endpoint reachable",
            passed=True,
            detail=endpoint_info,
        )
    else:
        # Check if it looks like an auth issue (connection worked but auth failed)
        if "AccessDenied" in detail or "InvalidAccessKeyId" in detail:
            return create_check(
                name="infra.minio.reachable",
                description="MinIO/S3 endpoint reachable",
                passed=True,
                detail=f"Reachable at {endpoint_info} (auth issue: {detail})",
                warn=True,
                warn_reason=f"Reachable at {endpoint_info} (auth issue: {detail})",
            )
        return create_check(
            name="infra.minio.reachable",
            description="MinIO/S3 endpoint reachable",
            passed=False,
            detail=f"Cannot connect to {endpoint_info}: {detail}",
        )


def check_s3_buckets_exist(
    config: PlatformSettings,
    *,
    buckets: list[str] | None = None,
    context: RuntimeContext | None = None,
) -> CheckResult:
    """Check that required S3 buckets exist.

    Parameters
    ----------
    config:
        Platform configuration.
    buckets:
        List of bucket names to check. Defaults to lake + logs buckets.
    context:
        Runtime context override (auto-detected if None).
    """
    from botocore.exceptions import (
        ClientError,
        EndpointConnectionError,
        NoCredentialsError,
    )

    # Resolve runtime context and endpoint
    ctx = get_runtime_context(context)
    s3_config = config.storage.s3
    endpoint = resolve_s3_endpoint(s3_config, ctx)
    endpoint_info = format_endpoint_info(endpoint, ctx)

    # Get required buckets from requirements (single source of truth)
    reqs = get_platform_requirements(config)
    target_buckets = list(buckets) if buckets is not None else list(reqs.buckets)

    missing: list[str] = []
    found: list[str] = []

    try:
        # Use healthcheck client for fast-fail
        client = build_s3_client(s3_config, endpoint, purpose="healthcheck")

        for bucket in target_buckets:
            try:
                client.head_bucket(Bucket=bucket)
                found.append(bucket)
            except ClientError as exc:
                error_code = exc.response.get("Error", {}).get("Code", "")
                if error_code in {"404", "NoSuchBucket"}:
                    missing.append(bucket)
                else:
                    # Other error (access denied, etc.) - treat as found but warn
                    found.append(f"{bucket} (access issue)")

        if missing:
            return create_check(
                name="infra.s3.buckets.exist",
                description="Required S3 buckets exist",
                passed=False,
                detail=f"Missing: {', '.join(missing)} ({endpoint_info})",
            )

        return create_check(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            passed=True,
            detail=f"Found: {', '.join(found)} ({endpoint_info})",
        )

    except NoCredentialsError:
        return create_check(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            passed=False,
            detail=(
                "Missing AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                "or source .env"
            ),
        )

    except EndpointConnectionError:
        return create_check(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            passed=False,
            detail=f"Cannot reach endpoint {endpoint_info}",
        )

    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        error_msg = exc.response.get("Error", {}).get("Message", str(exc))
        return create_check(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            passed=False,
            detail=f"S3 error {error_code}: {error_msg} ({endpoint_info})",
        )

    except Exception as exc:
        return create_check(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            passed=False,
            detail=f"{exc} ({endpoint_info})",
        )


# ---------------------------------------------------------------------------
# Spark-based checks (require pyspark, skipped on host)
# ---------------------------------------------------------------------------


def _pyspark_available() -> bool:
    """Check if pyspark is available for import."""
    try:
        import pyspark  # noqa: F401

        return True
    except ImportError:
        return False


def check_spark_session(_config: PlatformSettings) -> CheckResult:
    """Check that Spark session starts and simple SQL works.

    This is a basic reachability check - it does NOT test Iceberg or S3.

    Parameters
    ----------
    _config:
        Platform configuration (reserved for future use).
    """
    if not _pyspark_available():
        return create_check(
            name="infra.spark.session",
            description="Spark session starts",
            passed=False,
            skip=True,
            skip_reason="pyspark not installed (run inside Spark container)",
        )

    spark = None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("infra_check_session").getOrCreate()

        # Run a simple SQL
        result = spark.sql("SELECT 1 as test").collect()
        if result and result[0]["test"] == 1:
            return create_check(
                name="infra.spark.session",
                description="Spark session starts",
                passed=True,
                detail="SELECT 1 successful",
            )
        else:
            return create_check(
                name="infra.spark.session",
                description="Spark session starts",
                passed=False,
                detail="SELECT 1 returned unexpected result",
            )

    except Exception as exc:
        return create_check(
            name="infra.spark.session",
            description="Spark session starts",
            passed=False,
            detail=str(exc)[:200],
        )
    finally:
        if spark:
            spark.stop()


def check_iceberg_configured(config: PlatformSettings) -> CheckResult:
    """Check that Iceberg integration is configured in Spark (no state mutation).

    Checks Spark configuration for:
    - ``spark.sql.extensions`` contains Iceberg extensions
    - ``spark.sql.catalog.<catalog_name>`` exists

    Parameters
    ----------
    config:
        Platform configuration with Iceberg catalog name.
    """
    if not _pyspark_available():
        return create_check(
            name="infra.iceberg.configured",
            description="Iceberg integration configured",
            passed=False,
            skip=True,
            skip_reason="pyspark not installed (run inside Spark container)",
        )

    spark = None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("infra_check_iceberg").getOrCreate()

        catalog_name = config.storage.iceberg.catalog_name

        # Check for Iceberg extensions
        extensions = spark.conf.get("spark.sql.extensions", "") or ""
        has_extensions = "IcebergSparkSessionExtensions" in extensions

        # Check for catalog config
        catalog_key = f"spark.sql.catalog.{catalog_name}"
        catalog_impl = spark.conf.get(catalog_key, "") or ""
        has_catalog = "iceberg" in catalog_impl.lower()

        if has_extensions and has_catalog:
            return create_check(
                name="infra.iceberg.configured",
                description="Iceberg integration configured",
                passed=True,
                detail=f"Catalog '{catalog_name}' configured with Iceberg extensions",
            )
        elif has_extensions or has_catalog:
            # Partial configuration
            return create_check(
                name="infra.iceberg.configured",
                description="Iceberg integration configured",
                passed=True,
                detail="Partial Iceberg config detected",
                warn=True,
                warn_reason=f"extensions={has_extensions}, catalog={has_catalog}",
            )
        else:
            # No Iceberg config - this is expected on host, skip rather than fail
            return create_check(
                name="infra.iceberg.configured",
                description="Iceberg integration configured",
                passed=False,
                skip=True,
                skip_reason="Iceberg not configured (run inside Spark container)",
            )

    except Exception as exc:
        return create_check(
            name="infra.iceberg.configured",
            description="Iceberg integration configured",
            passed=False,
            detail=str(exc)[:200],
        )
    finally:
        if spark:
            spark.stop()


def check_s3a_from_spark(config: PlatformSettings) -> CheckResult:
    """Check that Spark can reach MinIO via s3a filesystem.

    Attempts to list the configured bucket root using Hadoop FS APIs.

    Parameters
    ----------
    config:
        Platform configuration with S3 and Iceberg settings.
    """
    if not _pyspark_available():
        return create_check(
            name="infra.spark.s3a",
            description="Spark can access S3 via s3a",
            passed=False,
            skip=True,
            skip_reason="pyspark not installed (run inside Spark container)",
        )

    spark = None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("infra_check_s3a").getOrCreate()

        bucket = config.storage.s3.bucket

        # First check if s3a is configured
        endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint", "")
        if not endpoint:
            # No S3A config - expected on host, skip rather than fail
            return create_check(
                name="infra.spark.s3a",
                description="Spark can access S3 via s3a",
                passed=False,
                skip=True,
                skip_reason="S3A not configured (run inside Spark container)",
            )

        # Try to access the bucket via Hadoop FileSystem
        try:
            jvm = spark.sparkContext._jvm
            if jvm is None:
                raise RuntimeError("JVM not available")
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            uri = jvm.java.net.URI(f"s3a://{bucket}/")
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            path = jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/")

            # Try listing - this validates connectivity and auth
            fs.listStatus(path)

            return create_check(
                name="infra.spark.s3a",
                description="Spark can access S3 via s3a",
                passed=True,
                detail=f"Successfully accessed s3a://{bucket}/ (endpoint: {endpoint})",
            )

        except Exception as fs_exc:
            exc_str = str(fs_exc)
            # Check for auth vs connection errors
            if "AccessDenied" in exc_str or "403" in exc_str:
                return create_check(
                    name="infra.spark.s3a",
                    description="Spark can access S3 via s3a",
                    passed=True,
                    detail=f"Endpoint reachable but auth issue: {exc_str[:100]}",
                    warn=True,
                    warn_reason="Auth issue - check AWS credentials",
                )
            elif "UnknownHost" in exc_str or "connect" in exc_str.lower():
                return create_check(
                    name="infra.spark.s3a",
                    description="Spark can access S3 via s3a",
                    passed=False,
                    detail=f"Cannot connect: {exc_str[:100]}",
                )
            else:
                return create_check(
                    name="infra.spark.s3a",
                    description="Spark can access S3 via s3a",
                    passed=False,
                    detail=exc_str[:200],
                )

    except Exception as exc:
        return create_check(
            name="infra.spark.s3a",
            description="Spark can access S3 via s3a",
            passed=False,
            detail=str(exc)[:200],
        )
    finally:
        if spark:
            spark.stop()


def check_namespaces_exist(config: PlatformSettings) -> CheckResult:
    """Check that required Iceberg namespaces exist.

    This check verifies that all namespaces defined in requirements.iceberg.namespaces
    have been created. If namespaces don't exist, it indicates that `infra apply`
    needs to be run.

    Parameters
    ----------
    config:
        Platform configuration.
    """
    if not _pyspark_available():
        return create_check(
            name="infra.namespaces.exist",
            description="Required Iceberg namespaces exist",
            passed=False,
            skip=True,
            skip_reason="pyspark not installed (run inside Spark container)",
        )

    spark = None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("infra_check_namespaces").getOrCreate()

        reqs = get_platform_requirements(config)
        catalog = reqs.iceberg.catalog

        # First check if Iceberg is configured
        extensions = spark.conf.get("spark.sql.extensions", "") or ""
        has_extensions = "IcebergSparkSessionExtensions" in extensions

        catalog_key = f"spark.sql.catalog.{catalog}"
        catalog_impl = spark.conf.get(catalog_key, "") or ""
        has_catalog = "iceberg" in catalog_impl.lower()

        if not (has_extensions and has_catalog):
            return create_check(
                name="infra.namespaces.exist",
                description="Required Iceberg namespaces exist",
                passed=False,
                skip=True,
                skip_reason=f"Iceberg catalog '{catalog}' not configured (run inside Spark container)",
            )

        # Get existing namespaces
        try:
            ns_rows = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
            existing = {row[0] for row in ns_rows}
        except Exception:
            existing = set()

        # Check which required namespaces are missing
        required = set(reqs.namespaces)
        missing = required - existing
        found = required & existing

        if missing:
            return create_check(
                name="infra.namespaces.exist",
                description="Required Iceberg namespaces exist",
                passed=False,
                detail=f"Missing: {', '.join(sorted(missing))}. Run 'skill-radar infra apply'.",
            )

        return create_check(
            name="infra.namespaces.exist",
            description="Required Iceberg namespaces exist",
            passed=True,
            detail=f"Found: {', '.join(sorted(found))}",
        )

    except Exception as exc:
        return create_check(
            name="infra.namespaces.exist",
            description="Required Iceberg namespaces exist",
            passed=False,
            detail=str(exc)[:200],
        )
    finally:
        if spark:
            spark.stop()


# ---------------------------------------------------------------------------
# Check collection
# ---------------------------------------------------------------------------


def get_infra_checks_host(
    config: PlatformSettings,
    *,
    context: RuntimeContext | None = None,
) -> list[NamedCheck]:
    """Return host-level infrastructure checks (boto3-based, no Spark).

    These checks run on the host machine using boto3 and do NOT require pyspark.
    They validate MinIO/S3 reachability and bucket existence.

    Parameters
    ----------
    config:
        Platform configuration.
    context:
        Runtime context override (auto-detected if None). Affects which
        S3 endpoint is used (host vs docker).

    Returns
    -------
    list[NamedCheck]:
        List of named checks ready to execute.
    """
    ctx = get_runtime_context(context)

    return [
        NamedCheck(
            name="infra.minio.reachable",
            description="MinIO/S3 endpoint is reachable",
            fn=lambda: check_minio_reachable(config, context=ctx),
        ),
        NamedCheck(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            fn=lambda: check_s3_buckets_exist(config, context=ctx),
        ),
    ]


def get_infra_checks_runtime(
    config: PlatformSettings,
    *,
    context: RuntimeContext | None = None,
) -> list[NamedCheck]:
    """Return runtime-level infrastructure checks (Spark/Iceberg).

    These checks require pyspark and should be run inside the Spark container.
    They validate Spark session, Iceberg configuration, S3A access, and namespaces.

    IMPORTANT: The caller should verify that pyspark is available before calling
    this function. If pyspark is missing, these checks will skip individually,
    but for proper user experience the CLI should enforce pyspark availability
    and fail with a helpful message.

    Parameters
    ----------
    config:
        Platform configuration.
    context:
        Runtime context override (auto-detected if None). For runtime checks,
        this should typically be RuntimeContext.DOCKER.

    Returns
    -------
    list[NamedCheck]:
        List of named checks ready to execute.
    """
    # Note: context is not used by Spark checks (they use Spark's own config),
    # but we accept it for API consistency and potential future use.
    _ = get_runtime_context(context)

    return [
        NamedCheck(
            name="infra.spark.session",
            description="Spark session can be created",
            fn=lambda: check_spark_session(config),
        ),
        NamedCheck(
            name="infra.iceberg.configured",
            description="Iceberg catalog is configured",
            fn=lambda: check_iceberg_configured(config),
        ),
        NamedCheck(
            name="infra.spark.s3a",
            description="S3A access from Spark works",
            fn=lambda: check_s3a_from_spark(config),
        ),
        NamedCheck(
            name="infra.namespaces.exist",
            description="Required Iceberg namespaces exist",
            fn=lambda: check_namespaces_exist(config),
        ),
    ]


def get_infra_checks(
    config: PlatformSettings,
    *,
    context: RuntimeContext | None = None,
) -> list[NamedCheck]:
    """Return all infrastructure check functions bound to config.

    .. deprecated::
        Use `get_infra_checks_host()` or `get_infra_checks_runtime()` instead
        based on required scope. This function returns all checks which will
        attempt to start Spark even when run on host.

    Parameters
    ----------
    config:
        Platform configuration.
    context:
        Runtime context override (auto-detected if None). Affects which
        S3 endpoint is used (host vs docker).

    Returns
    -------
    list[NamedCheck]:
        List of named checks ready to execute.
    """
    # Resolve context once for all checks
    ctx = get_runtime_context(context)

    return [
        NamedCheck(
            name="infra.minio.reachable",
            description="MinIO/S3 endpoint is reachable",
            fn=lambda: check_minio_reachable(config, context=ctx),
        ),
        NamedCheck(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            fn=lambda: check_s3_buckets_exist(config, context=ctx),
        ),
        NamedCheck(
            name="infra.spark.session",
            description="Spark session can be created",
            fn=lambda: check_spark_session(config),
        ),
        NamedCheck(
            name="infra.iceberg.configured",
            description="Iceberg catalog is configured",
            fn=lambda: check_iceberg_configured(config),
        ),
        NamedCheck(
            name="infra.spark.s3a",
            description="S3A access from Spark works",
            fn=lambda: check_s3a_from_spark(config),
        ),
        NamedCheck(
            name="infra.namespaces.exist",
            description="Required Iceberg namespaces exist",
            fn=lambda: check_namespaces_exist(config),
        ),
    ]
