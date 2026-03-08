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


class AdzunaConfig(BaseModel):
    """Adzuna API and extraction configuration.

    Credentials (``ADZUNA_APP_ID``, ``ADZUNA_APP_KEY``) are resolved
    from environment variables at runtime — never stored here.
    """

    base_url: str = "https://api.adzuna.com/v1/api"
    default_country: str = "fr"
    results_per_page: int = 50
    max_pages_per_run: int = 20
    request_timeout_seconds: int = 30
    max_retries: int = 3
    backoff_seconds: int = 2
    default_preset: str = "default_fr"


class SearchIndicesConfig(BaseModel):
    """Logical index name suffixes for each served dataset.

    Combined with ``SearchConfig.index_prefix`` at runtime to form
    the full Elasticsearch index/alias name.
    """

    skill_demand_daily: str = "skill-demand-daily"
    salary_by_skill_daily: str = "salary-by-skill-daily"
    occupation_skill_graph: str = "occupation-skill-graph"
    job_skill_matches: str = "job-skill-matches"
    job_occupation_matches: str = "job-occupation-matches"
    skill_emerging_daily: str = "skill-emerging-daily"
    occupation_market_daily: str = "occupation-market-daily"
    skill_demand_segments_daily: str = "skill-demand-segments-daily"


class SearchConfig(BaseModel):
    """Elasticsearch / Kibana serving-layer configuration.

    All fields are environment-driven via ``SKILLRADAR_SEARCH_*`` env vars.
    Elasticsearch is a serving/indexing layer only — Gold/Iceberg remains
    the source of truth.
    """

    enabled: bool = True
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_url_docker: str = "http://elasticsearch:9200"
    kibana_url: str = "http://localhost:5601"
    kibana_url_docker: str = "http://kibana:5601"
    index_prefix: str = "skillradar"
    index_replicas: int = 0
    index_shards: int = 1
    request_timeout_seconds: int = 30
    bulk_chunk_size: int = 500
    bulk_max_retries: int = 3
    bulk_retry_backoff_seconds: int = 2
    country_default: str = "fr"
    dashboard_bootstrap_enabled: bool = False
    create_index_if_missing: bool = True
    use_tls: bool = False
    indices: SearchIndicesConfig = Field(default_factory=SearchIndicesConfig)


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


class EmergingScoreConfig(BaseModel):
    """Weights for the composite emerging-skill score.

    ``composite = w_momentum * momentum + w_acceleration * acceleration + w_novelty * novelty``

    All weights must sum to 1.0.
    """

    w_momentum: float = 0.4
    w_acceleration: float = 0.3
    w_novelty: float = 0.3


class MLSegmentsConfig(BaseModel):
    """KMeans clustering parameters for skill demand segmentation."""

    k: int = 4
    min_rows: int = 20
    seed: int = 42
    max_iter: int = 20
    features: list[str] = Field(
        default_factory=lambda: [
            "jobs_count",
            "unique_companies_count",
            "unique_locations_count",
        ]
    )
    segment_names: dict[str, str] = Field(
        default_factory=lambda: {
            "0": "niche",
            "1": "growing",
            "2": "established",
            "3": "dominant",
        }
    )


class GoldAnalyticsConfig(BaseModel):
    """Configuration for Gold analytics computations."""

    emerging: EmergingScoreConfig = Field(default_factory=EmergingScoreConfig)
    ml_segments: MLSegmentsConfig = Field(default_factory=MLSegmentsConfig)


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
    adzuna: AdzunaConfig = Field(default_factory=AdzunaConfig)
    search: SearchConfig = Field(default_factory=SearchConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    gold_analytics: GoldAnalyticsConfig = Field(default_factory=GoldAnalyticsConfig)
