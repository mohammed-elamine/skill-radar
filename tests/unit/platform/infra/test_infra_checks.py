"""Tests for infrastructure checks with proper pyspark handling.

These tests verify that infra checks behave correctly:
- PASS when pyspark is available and configured properly
- SKIP when pyspark is not installed (not FAIL)
- Overall validation PASS when boto3 checks pass and spark checks are SKIPPED
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from skill_radar.config.models import (
    IcebergConfig,
    LoggingConfig,
    PlatformSettings,
    S3Config,
    StorageConfig,
)
from skill_radar.platform.validate.models import CheckStatus


@pytest.fixture
def mock_config() -> PlatformSettings:
    """Create a mock platform configuration."""
    return PlatformSettings(
        storage=StorageConfig(
            s3=S3Config(
                bucket="test-bucket",
                endpoint="http://localhost:9000",
                endpoint_host="http://localhost:9000",
                endpoint_docker="http://minio:9000",
                region="us-east-1",
            ),
            iceberg=IcebergConfig(
                catalog_name="sr",
                namespace_prefix="sr",
            ),
        ),
        logging=LoggingConfig(logs_bucket="test-logs"),
    )


class TestCheckSparkSessionNoPyspark:
    """Tests for check_spark_session when pyspark is not available."""

    def test_returns_skip_when_pyspark_not_installed(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When pyspark is not installed, check should SKIP, not FAIL."""
        # Simulate pyspark not being installed
        with patch.dict(sys.modules, {"pyspark": None}):
            # Force reimport to pick up the mock
            from skill_radar.platform.validate.checks import infra

            # Patch the _pyspark_available function directly
            with patch.object(infra, "_pyspark_available", return_value=False):
                result = infra.check_spark_session(mock_config)

        assert result.status == CheckStatus.SKIP
        assert "pyspark not installed" in (result.detail or "")

    def test_check_iceberg_configured_skips_without_pyspark(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When pyspark is not installed, iceberg check should SKIP."""
        from skill_radar.platform.validate.checks import infra

        with patch.object(infra, "_pyspark_available", return_value=False):
            result = infra.check_iceberg_configured(mock_config)

        assert result.status == CheckStatus.SKIP
        assert "pyspark not installed" in (result.detail or "")

    def test_check_s3a_from_spark_skips_without_pyspark(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When pyspark is not installed, s3a check should SKIP."""
        from skill_radar.platform.validate.checks import infra

        with patch.object(infra, "_pyspark_available", return_value=False):
            result = infra.check_s3a_from_spark(mock_config)

        assert result.status == CheckStatus.SKIP
        assert "pyspark not installed" in (result.detail or "")


class TestCheckMinioReachable:
    """Tests for check_minio_reachable."""

    def test_passes_when_minio_reachable(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When MinIO is reachable, check should PASS."""
        from skill_radar.platform.validate.checks import infra

        # Mock check_s3_connectivity to return success
        with patch.object(
            infra,
            "check_s3_connectivity",
            return_value=(True, "Connected to http://localhost:9000"),
        ):
            result = infra.check_minio_reachable(mock_config)

        assert result.status == CheckStatus.PASS
        assert "localhost:9000" in (result.detail or "")

    def test_fails_when_connection_error(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When connection fails, check should FAIL."""
        from skill_radar.platform.validate.checks import infra

        with patch.object(
            infra, "check_s3_connectivity", return_value=(False, "Connection refused")
        ):
            result = infra.check_minio_reachable(mock_config)

        assert result.status == CheckStatus.FAIL
        assert "Cannot connect" in (result.detail or "")

    def test_passes_with_warning_on_auth_issue(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When connection works but auth fails, check should PASS with warning."""
        from skill_radar.platform.validate.checks import infra

        with patch.object(
            infra, "check_s3_connectivity", return_value=(False, "AccessDenied: Access Denied")
        ):
            result = infra.check_minio_reachable(mock_config)

        # Auth issues mean connection worked, so PASS with warning
        assert result.status == CheckStatus.WARN
        assert "auth issue" in (result.detail or "").lower()


class TestCheckS3BucketsExist:
    """Tests for check_s3_buckets_exist."""

    def test_passes_when_all_buckets_exist(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When all buckets exist, check should PASS."""
        from skill_radar.platform.validate.checks import infra

        # Create a mock client that doesn't raise exceptions (bucket exists)
        mock_client = MagicMock()

        with patch.object(infra, "build_s3_client", return_value=mock_client):
            result = infra.check_s3_buckets_exist(mock_config)

        assert result.status == CheckStatus.PASS
        assert "Found" in (result.detail or "")

    def test_fails_when_bucket_missing(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """When a bucket is missing, check should FAIL."""
        from botocore.exceptions import ClientError

        from skill_radar.platform.validate.checks import infra

        # Create a mock client that raises 404 for bucket checks
        mock_client = MagicMock()
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")

        with patch.object(infra, "build_s3_client", return_value=mock_client):
            result = infra.check_s3_buckets_exist(mock_config)

        assert result.status == CheckStatus.FAIL
        assert "Missing" in (result.detail or "")


class TestGetInfraChecks:
    """Tests for get_infra_checks function."""

    def test_returns_six_checks(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """get_infra_checks should return 6 check functions."""
        from skill_radar.platform.validate.checks.infra import get_infra_checks

        checks = get_infra_checks(mock_config)

        # 6 checks: minio, buckets, spark_session, iceberg_configured, s3a, namespaces
        assert len(checks) == 6

    def test_checks_are_callable(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """Each check should be callable."""
        from skill_radar.platform.validate.checks.infra import get_infra_checks

        checks = get_infra_checks(mock_config)

        for check in checks:
            assert callable(check)


class TestValidateInfraPysparkSkipSemantics:
    """Tests for overall validation behavior when pyspark is unavailable."""

    def test_validation_passes_when_spark_checks_skipped(
        self,
        mock_config: PlatformSettings,  # noqa: ARG002
    ) -> None:
        """Validation should PASS overall when:
        - boto3 checks PASS
        - Spark checks are SKIPPED (not FAIL)
        """
        from skill_radar.platform.validate.models import NamedCheck
        from skill_radar.platform.validate.runner import run_checks

        # Create mock checks
        def mock_minio_check():
            from skill_radar.platform.validate.models import create_check

            return create_check(
                name="infra.minio.reachable",
                description="MinIO reachable",
                passed=True,
            )

        def mock_buckets_check():
            from skill_radar.platform.validate.models import create_check

            return create_check(
                name="infra.s3.buckets.exist",
                description="Buckets exist",
                passed=True,
            )

        def mock_spark_skipped():
            from skill_radar.platform.validate.models import create_check

            return create_check(
                name="infra.spark.session",
                description="Spark session",
                passed=False,
                skip=True,
                skip_reason="pyspark not installed",
            )

        checks = [
            NamedCheck("infra.minio.reachable", "MinIO reachable", mock_minio_check),
            NamedCheck("infra.s3.buckets.exist", "Buckets exist", mock_buckets_check),
            NamedCheck("infra.spark.session", "Spark session", mock_spark_skipped),
            NamedCheck("infra.spark.iceberg", "Iceberg", mock_spark_skipped),
            NamedCheck("infra.spark.s3a", "S3A", mock_spark_skipped),
        ]

        report = run_checks(checks, validator_name="infra", run_id="test")

        # Overall should PASS because no FAIL (only SKIP)
        assert report.passed is True
        assert report.status == CheckStatus.PASS
