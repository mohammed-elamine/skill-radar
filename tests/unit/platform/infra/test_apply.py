"""Tests for infrastructure apply module."""

from __future__ import annotations

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


class TestEnsureBuckets:
    """Tests for ensure_buckets function."""

    def test_creates_missing_bucket(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """Should create bucket when it doesn't exist."""
        from botocore.exceptions import ClientError

        from skill_radar.platform.infra import apply

        mock_client = MagicMock()
        # First call: bucket doesn't exist (404), then create succeeds
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")
        mock_client.create_bucket.return_value = {}

        with patch.object(apply, "build_s3_client", return_value=mock_client):
            results = apply.ensure_buckets(mock_config)

        # Should have 2 results (one per bucket)
        assert len(results) == 2
        # Both should be created (PASS with "Created" detail)
        for result in results:
            assert result.status == CheckStatus.PASS
            assert "Created" in (result.detail or "")

    def test_skips_existing_bucket(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """Should skip bucket creation when it already exists."""
        from skill_radar.platform.infra import apply

        mock_client = MagicMock()
        # head_bucket succeeds - bucket exists
        mock_client.head_bucket.return_value = {}

        with patch.object(apply, "build_s3_client", return_value=mock_client):
            results = apply.ensure_buckets(mock_config)

        # Should have 2 results
        assert len(results) == 2
        # Both should be PASS with "Already exists"
        for result in results:
            assert result.status == CheckStatus.PASS
            assert "Already exists" in (result.detail or "")


class TestApplyInfra:
    """Tests for apply_infra function."""

    def test_skips_namespaces_when_pyspark_unavailable(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """Should SKIP namespace creation when pyspark is unavailable."""
        from skill_radar.platform.infra import apply
        from skill_radar.platform.validate.checks import infra as infra_checks

        mock_client = MagicMock()
        # All bucket checks pass
        mock_client.head_bucket.return_value = {}
        mock_client.list_buckets.return_value = {"Buckets": []}

        # Mock both build_s3_client and check_s3_connectivity
        with (
            patch.object(apply, "build_s3_client", return_value=mock_client),
            patch.object(infra_checks, "check_s3_connectivity", return_value=(True, "Connected")),
            patch.object(apply, "_pyspark_available", return_value=False),
        ):
            results, artifacts = apply.apply_infra(mock_config, create_namespaces=True)

        # Should have results for minio + 2 buckets + 1 namespaces skip
        assert len(results) >= 3

        # Find namespace result
        ns_results = [r for r in results if "namespace" in r.name.lower()]
        assert len(ns_results) == 1
        assert ns_results[0].status == CheckStatus.SKIP
        assert "pyspark" in (ns_results[0].detail or "").lower()

        # artifacts should indicate namespaces were skipped
        assert artifacts.get("namespaces_skipped") == "true"

    def test_can_skip_namespace_creation(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """Should not attempt namespace creation when create_namespaces=False."""
        from skill_radar.platform.infra import apply
        from skill_radar.platform.validate.checks import infra as infra_checks

        mock_client = MagicMock()
        mock_client.head_bucket.return_value = {}
        mock_client.list_buckets.return_value = {"Buckets": []}

        with (
            patch.object(apply, "build_s3_client", return_value=mock_client),
            patch.object(
                infra_checks,
                "check_s3_connectivity",
                return_value=(True, "Connected"),
            ),
        ):
            results, _artifacts = apply.apply_infra(mock_config, create_namespaces=False)

        # Should only have minio + bucket results (no namespace)
        ns_results = [r for r in results if "namespace" in r.name.lower()]
        assert len(ns_results) == 0

    def test_returns_ok_when_buckets_pass_and_ns_skipped(
        self,
        mock_config: PlatformSettings,
    ) -> None:
        """Overall should succeed when buckets pass and namespaces are skipped."""
        from skill_radar.platform.infra import apply
        from skill_radar.platform.validate.checks import infra as infra_checks

        mock_client = MagicMock()
        mock_client.head_bucket.return_value = {}
        mock_client.list_buckets.return_value = {"Buckets": []}

        with (
            patch.object(apply, "build_s3_client", return_value=mock_client),
            patch.object(
                infra_checks,
                "check_s3_connectivity",
                return_value=(True, "Connected"),
            ),
            patch.object(apply, "_pyspark_available", return_value=False),
        ):
            results, _artifacts = apply.apply_infra(mock_config, create_namespaces=True)

        # No FAIL results
        failed = [r for r in results if r.status == CheckStatus.FAIL]
        assert len(failed) == 0
