"""Configuration loader: YAML defaults + environment-variable overrides."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from .models import PlatformSettings

_DEFAULTS_PATH = Path(__file__).parent / "defaults.yaml"

# Mapping of environment variables to their nested config key paths.
_ENV_OVERRIDES: dict[str, tuple[str, ...]] = {
    "SKILLRADAR_ENV": ("platform", "environment"),
    "SKILLRADAR_S3_BUCKET": ("storage", "s3", "bucket"),
    "SKILLRADAR_S3_ENDPOINT_HOST": ("storage", "s3", "endpoint_host"),
    "SKILLRADAR_S3_ENDPOINT_DOCKER": ("storage", "s3", "endpoint_docker"),
    "LOG_LEVEL": ("logging", "level"),
    "LOG_DIR": ("logging", "log_dir"),
    "LOG_FORMAT": ("logging", "console_format"),
    "SKILLRADAR_S3_LOGS_BUCKET": ("logging", "logs_bucket"),
    "SKILLRADAR_VALIDATION_ESCO_SILVER_MIN_FK_COVERAGE": (
        "validation",
        "esco",
        "silver",
        "min_relation_fk_coverage",
    ),
}


def _deep_set(data: dict, keys: tuple[str, ...], value: str) -> None:
    """Set a deeply-nested dict value given a key path."""
    node = data
    for key in keys[:-1]:
        node = node.setdefault(key, {})
    node[keys[-1]] = value


def load_platform_config(defaults_path: Path | None = None) -> PlatformSettings:
    """Load platform configuration.

    1. Read ``defaults.yaml``.
    2. Override values from environment variables.
    3. Parse into a strongly-typed Pydantic model.

    Parameters
    ----------
    defaults_path:
        Optional override for the YAML defaults file (useful in tests).
    """
    path = defaults_path or _DEFAULTS_PATH

    with Path.open(path) as fh:
        raw: dict = yaml.safe_load(fh) or {}

    for env_var, key_path in _ENV_OVERRIDES.items():
        value = os.environ.get(env_var)
        if value is not None:
            _deep_set(raw, key_path, value)

    return PlatformSettings(**raw)
