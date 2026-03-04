"""Pydantic models for platform configuration."""

from __future__ import annotations

from pydantic import BaseModel, Field


class S3Config(BaseModel):
    """S3-compatible storage configuration."""

    bucket: str = "skillradar-lake"
    endpoint: str = "http://localhost:9000"
    region: str = "eu-west-3"
    secure: bool = False


class StorageConfig(BaseModel):
    """Storage backend configuration."""

    s3: S3Config = Field(default_factory=S3Config)


class LakeLayersConfig(BaseModel):
    """Lake layer name mapping."""

    landing: str = "landing"
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"


class LakeConfig(BaseModel):
    """Data-lake layout configuration."""

    root_prefix: str = "data"
    layers: LakeLayersConfig = Field(default_factory=LakeLayersConfig)


class ManifestConfig(BaseModel):
    """Manifest versioning configuration."""

    schema_version: str = "1.0.0"


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    log_dir: str = "logs"
    console_format: str = "text"
    file_format: str = "json"
    upload: bool = False
    logs_bucket: str = "skillradar-logs"


class PlatformConfig(BaseModel):
    """Top-level platform section."""

    environment: str = "local"


class PlatformSettings(BaseModel):
    """Root configuration model for the Skill Radar platform.

    Mirrors the structure of ``config/defaults.yaml`` and can be overridden
    by environment variables via :func:`skill_radar.config.loader.load_platform_config`.
    """

    platform: PlatformConfig = Field(default_factory=PlatformConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    lake: LakeConfig = Field(default_factory=LakeConfig)
    manifest: ManifestConfig = Field(default_factory=ManifestConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
