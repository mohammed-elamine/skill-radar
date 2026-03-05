"""Unit tests for ESCO landing checks.

Uses mocking to avoid real S3 calls.
"""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from skill_radar.config.models import (
    LakeConfig,
    LakeLayersConfig,
    LoggingConfig,
    PlatformConfig,
    PlatformSettings,
    S3Config,
    StorageConfig,
)
from skill_radar.platform.validate.checks.esco import (
    _load_manifest_from_s3,
    check_landing_artifact_exists,
    check_landing_contract_match,
    check_landing_manifest_exists,
    check_landing_manifest_parseable,
    check_landing_manifest_schema_version,
)
from skill_radar.platform.validate.models import CheckStatus


@pytest.fixture
def mock_config() -> PlatformSettings:
    """Create a mock platform config."""
    return PlatformSettings(
        platform=PlatformConfig(environment="test"),
        storage=StorageConfig(
            s3=S3Config(
                bucket="test-bucket",
                endpoint="http://localhost:9000",
            )
        ),
        lake=LakeConfig(
            root_prefix="data",
            layers=LakeLayersConfig(),
        ),
        logging=LoggingConfig(logs_bucket="test-logs"),
    )


@pytest.fixture
def mock_boto3_client():
    """Create a mock boto3 client."""
    mock_client = MagicMock()
    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_client
    return mock_boto3, mock_client


class TestCheckLandingArtifactExists:
    """Tests for check_landing_artifact_exists."""

    def test_artifact_exists(self, mock_config: PlatformSettings, mock_boto3_client: tuple) -> None:
        """Returns PASS when artifact exists."""
        mock_boto3, mock_client = mock_boto3_client
        mock_client.head_object.return_value = {}

        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            # Re-import to pick up the mock
            result = check_landing_artifact_exists(mock_config, "v1.2.0", "fr")

        assert result.status == CheckStatus.PASS
        assert "s3://" in result.detail

    def test_artifact_not_found(
        self, mock_config: PlatformSettings, mock_boto3_client: tuple
    ) -> None:
        """Returns FAIL when artifact doesn't exist."""
        from botocore.exceptions import ClientError

        mock_boto3, mock_client = mock_boto3_client
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            result = check_landing_artifact_exists(mock_config, "v1.2.0", "fr")

        assert result.status == CheckStatus.FAIL
        assert "Not found" in result.detail


class TestCheckLandingManifestExists:
    """Tests for check_landing_manifest_exists."""

    def test_manifest_exists(self, mock_config: PlatformSettings, mock_boto3_client: tuple) -> None:
        """Returns PASS when manifest exists."""
        mock_boto3, mock_client = mock_boto3_client
        mock_client.head_object.return_value = {}

        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            result = check_landing_manifest_exists(mock_config, "v1.2.0", "fr")

        assert result.status == CheckStatus.PASS

    def test_manifest_not_found(
        self, mock_config: PlatformSettings, mock_boto3_client: tuple
    ) -> None:
        """Returns FAIL when manifest doesn't exist."""
        from botocore.exceptions import ClientError

        mock_boto3, mock_client = mock_boto3_client
        mock_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            result = check_landing_manifest_exists(mock_config, "v1.2.0", "fr")

        assert result.status == CheckStatus.FAIL


class TestLoadManifestFromS3:
    """Tests for _load_manifest_from_s3 helper."""

    def test_load_valid_manifest(
        self, mock_config: PlatformSettings, mock_boto3_client: tuple
    ) -> None:
        """Loads valid manifest JSON."""
        manifest_data = {"dataset": "esco", "schema_version": "1.0.0"}

        mock_boto3, mock_client = mock_boto3_client
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(manifest_data).encode()
        mock_client.get_object.return_value = {"Body": mock_body}

        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            manifest, error = _load_manifest_from_s3(mock_config, "v1.2.0", "fr")

        assert error == ""
        assert manifest is not None
        assert manifest["dataset"] == "esco"

    def test_load_invalid_json(
        self, mock_config: PlatformSettings, mock_boto3_client: tuple
    ) -> None:
        """Returns error for invalid JSON."""
        mock_boto3, mock_client = mock_boto3_client
        mock_body = MagicMock()
        mock_body.read.return_value = b"not valid json {"
        mock_client.get_object.return_value = {"Body": mock_body}

        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            manifest, error = _load_manifest_from_s3(mock_config, "v1.2.0", "fr")

        assert manifest is None
        assert "Invalid JSON" in error


class TestCheckLandingManifestParseable:
    """Tests for check_landing_manifest_parseable."""

    def test_valid_json_passes(self, mock_config: PlatformSettings) -> None:
        """Returns PASS for valid JSON."""
        with patch("skill_radar.platform.validate.checks.esco._load_manifest_from_s3") as mock_load:
            mock_load.return_value = ({"dataset": "esco"}, "")

            result = check_landing_manifest_parseable(mock_config, "v1.2.0", "fr")

            assert result.status == CheckStatus.PASS

    def test_invalid_json_fails(self, mock_config: PlatformSettings) -> None:
        """Returns FAIL for invalid JSON."""
        with patch("skill_radar.platform.validate.checks.esco._load_manifest_from_s3") as mock_load:
            mock_load.return_value = (None, "Invalid JSON: error")

            result = check_landing_manifest_parseable(mock_config, "v1.2.0", "fr")

            assert result.status == CheckStatus.FAIL


class TestCheckLandingManifestSchemaVersion:
    """Tests for check_landing_manifest_schema_version."""

    def test_supported_version_passes(self, mock_config: PlatformSettings) -> None:
        """Returns PASS for supported schema version."""
        with patch("skill_radar.platform.validate.checks.esco._load_manifest_from_s3") as mock_load:
            mock_load.return_value = ({"schema_version": "1.0.0"}, "")

            result = check_landing_manifest_schema_version(mock_config, "v1.2.0", "fr")

            assert result.status == CheckStatus.PASS

    def test_unsupported_version_fails(self, mock_config: PlatformSettings) -> None:
        """Returns FAIL for unsupported schema version."""
        with patch("skill_radar.platform.validate.checks.esco._load_manifest_from_s3") as mock_load:
            mock_load.return_value = ({"schema_version": "99.0.0"}, "")

            result = check_landing_manifest_schema_version(mock_config, "v1.2.0", "fr")

            assert result.status == CheckStatus.FAIL
            assert "Unsupported" in result.detail

    def test_missing_version_fails(self, mock_config: PlatformSettings) -> None:
        """Returns FAIL when schema_version is missing."""
        with patch("skill_radar.platform.validate.checks.esco._load_manifest_from_s3") as mock_load:
            mock_load.return_value = ({"dataset": "esco"}, "")  # No schema_version

            result = check_landing_manifest_schema_version(mock_config, "v1.2.0", "fr")

            assert result.status == CheckStatus.FAIL
            assert "missing" in result.detail


class TestCheckLandingContractMatch:
    """Tests for check_landing_contract_match."""

    def test_matching_contract_passes(self, mock_config: PlatformSettings) -> None:
        """Returns PASS when manifest matches contract."""
        with patch("skill_radar.platform.validate.checks.esco._load_manifest_from_s3") as mock_load:
            mock_load.return_value = ({"dataset": "esco"}, "")

            result = check_landing_contract_match(mock_config, "v1.2.0", "fr")

            assert result.status == CheckStatus.PASS

    def test_unsupported_language_fails(self, mock_config: PlatformSettings) -> None:
        """Returns FAIL for unsupported language."""
        result = check_landing_contract_match(mock_config, "v1.2.0", "zz")

        assert result.status == CheckStatus.FAIL
        assert "not in supported" in result.detail

    def test_dataset_mismatch_fails(self, mock_config: PlatformSettings) -> None:
        """Returns FAIL when dataset doesn't match."""
        with patch("skill_radar.platform.validate.checks.esco._load_manifest_from_s3") as mock_load:
            mock_load.return_value = ({"dataset": "other_dataset"}, "")

            result = check_landing_contract_match(mock_config, "v1.2.0", "fr")

            assert result.status == CheckStatus.FAIL
            assert "mismatch" in result.detail
