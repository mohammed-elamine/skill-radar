"""Tests for platform configuration loader."""

from __future__ import annotations

from skill_radar.config.loader import load_platform_config
from skill_radar.config.models import PlatformSettings


class TestConfigLoader:
    """Verify defaults.yaml loading and env-var overrides."""

    def test_loads_defaults(self):
        config = load_platform_config()
        assert isinstance(config, PlatformSettings)
        assert config.platform.environment == "local"
        assert config.storage.s3.bucket == "skillradar-lake"
        assert config.storage.s3.endpoint_host == "http://localhost:9000"
        assert config.storage.s3.endpoint_docker == "http://minio:9000"
        assert config.lake.root_prefix == "data"
        assert config.lake.layers.landing == "landing"
        assert config.manifest.schema_version == "1.0.0"
        assert config.logging.level == "INFO"

    def test_env_override_bucket(self, monkeypatch):
        monkeypatch.setenv("SKILLRADAR_S3_BUCKET", "test-bucket")
        config = load_platform_config()
        assert config.storage.s3.bucket == "test-bucket"

    def test_env_override_endpoint_host(self, monkeypatch):
        monkeypatch.setenv("SKILLRADAR_S3_ENDPOINT_HOST", "http://custom-host:9999")
        config = load_platform_config()
        assert config.storage.s3.endpoint_host == "http://custom-host:9999"

    def test_env_override_endpoint_docker(self, monkeypatch):
        monkeypatch.setenv("SKILLRADAR_S3_ENDPOINT_DOCKER", "http://custom-docker:9999")
        config = load_platform_config()
        assert config.storage.s3.endpoint_docker == "http://custom-docker:9999"

    def test_env_override_environment(self, monkeypatch):
        monkeypatch.setenv("SKILLRADAR_ENV", "production")
        config = load_platform_config()
        assert config.platform.environment == "production"

    def test_loads_validation_defaults(self):
        config = load_platform_config()
        assert config.validation.esco.silver.min_relation_fk_coverage == 0.99

    def test_env_override_validation_fk_coverage(self, monkeypatch):
        monkeypatch.setenv("SKILLRADAR_VALIDATION_ESCO_SILVER_MIN_FK_COVERAGE", "0.95")
        config = load_platform_config()
        assert config.validation.esco.silver.min_relation_fk_coverage == 0.95
