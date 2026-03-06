"""Pydantic models for platform configuration."""

from __future__ import annotations

from pydantic import BaseModel, Field


class S3Config(BaseModel):
    """S3-compatible storage configuration.

    Attributes
    ----------
    bucket:
        Primary lake bucket name.
    endpoint_host:
        S3 endpoint when running on host (outside Docker).
    endpoint_docker:
        S3 endpoint when running inside Docker network.
    region:
        AWS region for S3 operations.
    secure:
        Whether to use HTTPS.
    additional_buckets:
        Extra buckets beyond lake + logs.
    """

    bucket: str = "skillradar-lake"
    endpoint_host: str = "http://localhost:9000"
    endpoint_docker: str = "http://minio:9000"
    region: str = "eu-west-3"
    secure: bool = False
    additional_buckets: list[str] = Field(default_factory=list)


class IcebergConfig(BaseModel):
    """Iceberg catalog configuration."""

    catalog_name: str = "sr"
    namespace_prefix: str = "sr"
    warehouse: str = "s3a://skillradar-lake/warehouse"
    required_namespaces: list[str] = Field(
        default_factory=lambda: ["sr_bronze", "sr_silver", "sr_gold"]
    )


class StorageConfig(BaseModel):
    """Storage backend configuration."""

    s3: S3Config = Field(default_factory=S3Config)
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)


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


class EscoSilverValidationConfig(BaseModel):
    """ESCO Silver validation configuration."""

    min_relation_fk_coverage: float = 0.99


class EscoValidationConfig(BaseModel):
    """ESCO dataset validation configuration."""

    silver: EscoSilverValidationConfig = Field(default_factory=EscoSilverValidationConfig)


class ValidationConfig(BaseModel):
    """Validation configuration."""

    esco: EscoValidationConfig = Field(default_factory=EscoValidationConfig)


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
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
