"""Tests for S3 client factory and connectivity checks.

Tests cover:
- build_s3_client() with different purposes
- check_s3_connectivity() success and failure cases
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from skill_radar.platform.storage.s3_client import (
    _DEFAULT_TIMEOUTS,
    _HEALTHCHECK_TIMEOUTS,
    build_s3_client,
    check_s3_connectivity,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_s3_config() -> Mock:
    """Create a mock S3Config."""
    config = Mock()
    config.region = "us-east-1"
    config.secure = False
    return config


# ---------------------------------------------------------------------------
# build_s3_client() tests
# ---------------------------------------------------------------------------


class TestBuildS3Client:
    """Tests for build_s3_client()."""

    def test_creates_client_with_endpoint(self, mock_s3_config: Mock) -> None:
        """Client is created with specified endpoint."""
        with patch("skill_radar.platform.storage.s3_client.boto3") as mock_boto3:
            build_s3_client(mock_s3_config, "http://localhost:9000")

            mock_boto3.client.assert_called_once()
            call_kwargs = mock_boto3.client.call_args[1]
            assert call_kwargs["endpoint_url"] == "http://localhost:9000"

    def test_creates_client_with_region(self, mock_s3_config: Mock) -> None:
        """Client is created with config region."""
        with patch("skill_radar.platform.storage.s3_client.boto3") as mock_boto3:
            build_s3_client(mock_s3_config, "http://localhost:9000")

            call_kwargs = mock_boto3.client.call_args[1]
            assert call_kwargs["region_name"] == "us-east-1"

    def test_default_purpose_uses_default_timeouts(self, mock_s3_config: Mock) -> None:
        """Default purpose uses standard timeout configuration."""
        with patch("skill_radar.platform.storage.s3_client.boto3") as mock_boto3:
            build_s3_client(mock_s3_config, "http://localhost:9000", purpose="default")

            call_kwargs = mock_boto3.client.call_args[1]
            assert call_kwargs["config"] == _DEFAULT_TIMEOUTS

    def test_healthcheck_purpose_uses_fast_timeouts(self, mock_s3_config: Mock) -> None:
        """Healthcheck purpose uses fast-fail timeout configuration."""
        with patch("skill_radar.platform.storage.s3_client.boto3") as mock_boto3:
            build_s3_client(mock_s3_config, "http://localhost:9000", purpose="healthcheck")

            call_kwargs = mock_boto3.client.call_args[1]
            assert call_kwargs["config"] == _HEALTHCHECK_TIMEOUTS

    def test_purpose_defaults_to_default(self, mock_s3_config: Mock) -> None:
        """Purpose defaults to 'default'."""
        with patch("skill_radar.platform.storage.s3_client.boto3") as mock_boto3:
            build_s3_client(mock_s3_config, "http://localhost:9000")

            call_kwargs = mock_boto3.client.call_args[1]
            assert call_kwargs["config"] == _DEFAULT_TIMEOUTS


# ---------------------------------------------------------------------------
# Timeout configuration tests
# ---------------------------------------------------------------------------


class TestTimeoutConfigurations:
    """Tests for timeout configuration values."""

    def test_healthcheck_has_fast_connect_timeout(self) -> None:
        """Healthcheck config has short connect timeout."""
        assert _HEALTHCHECK_TIMEOUTS.connect_timeout == 2

    def test_healthcheck_has_fast_read_timeout(self) -> None:
        """Healthcheck config has short read timeout."""
        assert _HEALTHCHECK_TIMEOUTS.read_timeout == 3

    def test_healthcheck_has_single_retry(self) -> None:
        """Healthcheck config has minimal retries."""
        assert _HEALTHCHECK_TIMEOUTS.retries["max_attempts"] == 1

    def test_default_has_reasonable_timeouts(self) -> None:
        """Default config has reasonable timeouts for operations."""
        assert _DEFAULT_TIMEOUTS.connect_timeout >= 5
        assert _DEFAULT_TIMEOUTS.read_timeout >= 10


# ---------------------------------------------------------------------------
# check_s3_connectivity() tests
# ---------------------------------------------------------------------------


class TestCheckS3Connectivity:
    """Tests for check_s3_connectivity()."""

    def test_returns_success_on_list_buckets(self, mock_s3_config: Mock) -> None:
        """Returns (True, detail) when list_buckets succeeds."""
        with patch("skill_radar.platform.storage.s3_client.build_s3_client") as mock_build:
            mock_client = Mock()
            mock_client.list_buckets.return_value = {"Buckets": []}
            mock_build.return_value = mock_client

            success, detail = check_s3_connectivity(mock_s3_config, "http://localhost:9000")

            assert success is True
            assert "localhost:9000" in detail

    def test_returns_failure_on_connection_error(self, mock_s3_config: Mock) -> None:
        """Returns (False, detail) on connection error."""
        with patch("skill_radar.platform.storage.s3_client.build_s3_client") as mock_build:
            mock_client = Mock()
            mock_client.list_buckets.side_effect = ConnectionRefusedError("Connection refused")
            mock_build.return_value = mock_client

            success, detail = check_s3_connectivity(mock_s3_config, "http://localhost:9000")

            assert success is False
            assert "Connection" in detail

    def test_returns_failure_on_client_error(self, mock_s3_config: Mock) -> None:
        """Returns (False, detail) on S3 client error."""
        from botocore.exceptions import ClientError

        with patch("skill_radar.platform.storage.s3_client.build_s3_client") as mock_build:
            mock_client = Mock()
            mock_client.list_buckets.side_effect = ClientError(
                {"Error": {"Code": "InternalError", "Message": "Server Error"}},
                "ListBuckets",
            )
            mock_build.return_value = mock_client

            success, detail = check_s3_connectivity(mock_s3_config, "http://localhost:9000")

            assert success is False
            assert "S3 error InternalError" in detail

    def test_uses_healthcheck_client(self, mock_s3_config: Mock) -> None:
        """Uses healthcheck-optimized client."""
        with patch("skill_radar.platform.storage.s3_client.build_s3_client") as mock_build:
            mock_client = Mock()
            mock_client.list_buckets.return_value = {"Buckets": []}
            mock_build.return_value = mock_client

            check_s3_connectivity(mock_s3_config, "http://localhost:9000")

            mock_build.assert_called_once_with(
                mock_s3_config, "http://localhost:9000", purpose="healthcheck"
            )
