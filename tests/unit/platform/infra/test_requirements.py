"""Unit tests for platform requirements module."""

from __future__ import annotations

import pytest

from skill_radar.config.models import (
    IcebergConfig,
    LoggingConfig,
    PlatformSettings,
    S3Config,
    StorageConfig,
)
from skill_radar.platform.infra.requirements import (
    IcebergRequirements,
    PlatformRequirements,
    get_platform_requirements,
)


class TestIcebergRequirements:
    """Tests for IcebergRequirements dataclass."""

    def test_namespace_fqn(self) -> None:
        """Test fully-qualified namespace name generation."""
        reqs = IcebergRequirements(
            catalog="sr",
            namespaces=("sr_bronze", "sr_silver"),
        )
        assert reqs.namespace_fqn("sr_bronze") == "sr.sr_bronze"
        assert reqs.namespace_fqn("sr_silver") == "sr.sr_silver"

    def test_all_fqns(self) -> None:
        """Test getting all fully-qualified namespace names."""
        reqs = IcebergRequirements(
            catalog="sr",
            namespaces=("sr_bronze", "sr_silver", "sr_gold"),
        )
        assert reqs.all_fqns() == ["sr.sr_bronze", "sr.sr_silver", "sr.sr_gold"]

    def test_frozen(self) -> None:
        """Test that dataclass is immutable."""
        reqs = IcebergRequirements(catalog="sr", namespaces=("sr_bronze",))
        with pytest.raises(AttributeError):
            reqs.catalog = "other"  # type: ignore[misc]


class TestPlatformRequirements:
    """Tests for PlatformRequirements dataclass."""

    def test_shortcuts(self) -> None:
        """Test shortcut properties."""
        iceberg = IcebergRequirements(catalog="sr", namespaces=("sr_bronze",))
        reqs = PlatformRequirements(
            buckets=("skillradar-lake", "skillradar-logs"),
            iceberg=iceberg,
        )
        assert reqs.catalog == "sr"
        assert reqs.namespaces == ("sr_bronze",)


class TestGetPlatformRequirements:
    """Tests for get_platform_requirements function."""

    def test_default_config_buckets(self) -> None:
        """Test that default config returns expected buckets."""
        config = PlatformSettings()
        reqs = get_platform_requirements(config)

        assert "skillradar-lake" in reqs.buckets
        assert "skillradar-logs" in reqs.buckets
        assert len(reqs.buckets) == 2

    def test_default_config_namespaces(self) -> None:
        """Test that default config returns expected namespaces."""
        config = PlatformSettings()
        reqs = get_platform_requirements(config)

        assert reqs.catalog == "sr"
        assert "sr_bronze" in reqs.namespaces
        assert "sr_silver" in reqs.namespaces
        assert "sr_gold" in reqs.namespaces
        assert len(reqs.namespaces) == 3

    def test_custom_buckets(self) -> None:
        """Test that custom bucket names are used."""
        config = PlatformSettings(
            storage=StorageConfig(
                s3=S3Config(bucket="my-lake"),
            ),
            logging=LoggingConfig(logs_bucket="my-logs"),
        )
        reqs = get_platform_requirements(config)

        assert "my-lake" in reqs.buckets
        assert "my-logs" in reqs.buckets

    def test_additional_buckets(self) -> None:
        """Test that additional buckets are included."""
        config = PlatformSettings(
            storage=StorageConfig(
                s3=S3Config(
                    bucket="my-lake",
                    additional_buckets=["extra-bucket", "another-bucket"],
                ),
            ),
        )
        reqs = get_platform_requirements(config)

        assert "my-lake" in reqs.buckets
        assert "extra-bucket" in reqs.buckets
        assert "another-bucket" in reqs.buckets

    def test_additional_buckets_deduplicated(self) -> None:
        """Test that duplicate bucket names are removed."""
        config = PlatformSettings(
            storage=StorageConfig(
                s3=S3Config(
                    bucket="my-lake",
                    additional_buckets=["my-lake", "extra"],  # duplicate
                ),
            ),
        )
        reqs = get_platform_requirements(config)

        # Should not have duplicates
        assert len([b for b in reqs.buckets if b == "my-lake"]) == 1

    def test_custom_namespaces(self) -> None:
        """Test that custom namespace list is used."""
        config = PlatformSettings(
            storage=StorageConfig(
                iceberg=IcebergConfig(
                    catalog_name="custom_catalog",
                    required_namespaces=["ns_bronze", "ns_silver"],
                ),
            ),
        )
        reqs = get_platform_requirements(config)

        assert reqs.catalog == "custom_catalog"
        assert reqs.namespaces == ("ns_bronze", "ns_silver")

    def test_returns_frozen_dataclass(self) -> None:
        """Test that returned requirements are immutable."""
        config = PlatformSettings()
        reqs = get_platform_requirements(config)

        with pytest.raises((AttributeError, TypeError)):
            reqs.buckets = ("hacked",)  # type: ignore[misc]

    def test_buckets_tuple_not_list(self) -> None:
        """Test that buckets are returned as tuple for immutability."""
        config = PlatformSettings()
        reqs = get_platform_requirements(config)

        assert isinstance(reqs.buckets, tuple)
        assert isinstance(reqs.namespaces, tuple)
