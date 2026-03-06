"""Centralized Airflow orchestration configuration for Skill Radar.

All tuneable knobs for DAG scheduling, Docker task execution, and
pipeline defaults are loaded from environment variables with safe
fallbacks.  DAG files should **never** hardcode project-specific
values — they read them from this module instead.

Environment variables
---------------------
SKILLRADAR_SPARK_IMAGE
    Docker image used by DockerOperator tasks (default: ``skillradar-spark:3.5.7-uv``).
SKILLRADAR_DOCKER_NETWORK
    Docker network shared with the compose stack (default: ``skillradar_default``).
SKILLRADAR_S3_ENDPOINT_DOCKER
    S3/MinIO endpoint reachable from inside containers (default: ``http://minio:9000``).
SKILLRADAR_S3_BUCKET
    Primary lake bucket (default: ``skillradar-lake``).
SKILLRADAR_S3_LOGS_BUCKET
    Logs/validation bucket (default: ``skillradar-logs``).
SKILLRADAR_ADZUNA_SCHEDULE
    Cron expression for Adzuna daily DAG (default: ``0 6 * * *``).
SKILLRADAR_ADZUNA_COUNTRY
    Default Adzuna country code (default: ``fr``).
SKILLRADAR_ADZUNA_PRESET
    Default Adzuna extraction preset (default: ``default_fr``).
SKILLRADAR_ESCO_VERSION
    Default ESCO version (default: ``v1.2.1``).
SKILLRADAR_ESCO_LANG
    Default ESCO language (default: ``fr``).
SKILLRADAR_ESCO_RUN_GOLD_AFTER
    Whether ESCO manual DAG triggers Gold by default (default: ``false``).
SKILLRADAR_TASK_RETRIES
    Default task retry count (default: ``2``).
SKILLRADAR_TASK_RETRY_DELAY_SECONDS
    Seconds between retries (default: ``120``).
SKILLRADAR_MAX_ACTIVE_RUNS
    Max concurrent DAG runs (default: ``1``).
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _env(key: str, default: str) -> str:
    """Read an env var with a fallback default."""
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    """Read an integer env var with a fallback."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(key: str, default: bool) -> bool:
    """Read a boolean env var (true/1/yes → True)."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


# ---------------------------------------------------------------------------
# Docker / Spark image
# ---------------------------------------------------------------------------

SPARK_IMAGE: str = _env("SKILLRADAR_SPARK_IMAGE", "skillradar-spark:3.5.7-uv")
DOCKER_NETWORK: str = _env("SKILLRADAR_DOCKER_NETWORK", "skillradar_default")
DOCKER_URL: str = _env("SKILLRADAR_DOCKER_URL", "unix://var/run/docker.sock")

# ---------------------------------------------------------------------------
# S3 / MinIO
# ---------------------------------------------------------------------------

S3_ENDPOINT_DOCKER: str = _env("SKILLRADAR_S3_ENDPOINT_DOCKER", "http://minio:9000")
S3_BUCKET: str = _env("SKILLRADAR_S3_BUCKET", "skillradar-lake")
S3_LOGS_BUCKET: str = _env("SKILLRADAR_S3_LOGS_BUCKET", "skillradar-logs")

# ---------------------------------------------------------------------------
# Credentials (forwarded into task containers)
# ---------------------------------------------------------------------------

AWS_ACCESS_KEY_ID: str = _env("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY: str = _env("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_DEFAULT_REGION: str = _env("AWS_DEFAULT_REGION", "eu-west-3")
ADZUNA_APP_ID: str = _env("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY: str = _env("ADZUNA_APP_KEY", "")

# ---------------------------------------------------------------------------
# Adzuna pipeline defaults
# ---------------------------------------------------------------------------

ADZUNA_SCHEDULE: str = _env("SKILLRADAR_ADZUNA_SCHEDULE", "0 6 * * *")
ADZUNA_COUNTRY: str = _env("SKILLRADAR_ADZUNA_COUNTRY", "fr")
ADZUNA_PRESET: str = _env("SKILLRADAR_ADZUNA_PRESET", "default_fr")

# ---------------------------------------------------------------------------
# ESCO pipeline defaults
# ---------------------------------------------------------------------------

ESCO_VERSION: str = _env("SKILLRADAR_ESCO_VERSION", "v1.2.1")
ESCO_LANG: str = _env("SKILLRADAR_ESCO_LANG", "fr")
ESCO_RUN_GOLD_AFTER: bool = _env_bool("SKILLRADAR_ESCO_RUN_GOLD_AFTER", False)

# ---------------------------------------------------------------------------
# Task execution defaults
# ---------------------------------------------------------------------------

TASK_RETRIES: int = _env_int("SKILLRADAR_TASK_RETRIES", 2)
TASK_RETRY_DELAY_SECONDS: int = _env_int("SKILLRADAR_TASK_RETRY_DELAY_SECONDS", 120)
MAX_ACTIVE_RUNS: int = _env_int("SKILLRADAR_MAX_ACTIVE_RUNS", 1)
TASK_EXECUTION_TIMEOUT_SECONDS: int = _env_int(
    "SKILLRADAR_TASK_EXECUTION_TIMEOUT_SECONDS",
    3600,
)

# ---------------------------------------------------------------------------
# Container mounts (host paths → container paths)
# ---------------------------------------------------------------------------

# These reproduce the same mounts the ``spark`` service uses in
# docker-compose.yml so that ephemeral DockerOperator containers have
# identical access to code, config, and logs.
CONTAINER_WORKING_DIR: str = "/opt/skillradar"

CONTAINER_MOUNTS: list[dict[str, str]] = [
    # Source code (read-only for safety)
    {"source": "src", "target": "/opt/skillradar/src", "type": "bind", "read_only": True},
    # Project descriptor (read-only)
    {
        "source": "pyproject.toml",
        "target": "/opt/skillradar/pyproject.toml",
        "type": "bind",
        "read_only": True,
    },
    {"source": "uv.lock", "target": "/opt/skillradar/uv.lock", "type": "bind", "read_only": True},
    # Spark config
    {"source": "configs", "target": "/opt/skillradar/configs", "type": "bind", "read_only": True},
    {
        "source": "configs/spark-defaults.conf",
        "target": "/opt/spark/conf/spark-defaults.conf",
        "type": "bind",
        "read_only": True,
    },
    {
        "source": "configs/log4j2.properties",
        "target": "/opt/spark/conf/log4j2.properties",
        "type": "bind",
        "read_only": True,
    },
    # Logs (writable)
    {"source": "logs", "target": "/opt/skillradar/logs", "type": "bind", "read_only": False},
    # Jobs
    {"source": "jobs", "target": "/opt/skillradar/jobs", "type": "bind", "read_only": True},
    # Dropzone (read-only)
    {
        "source": "data/incoming",
        "target": "/opt/skillradar/incoming",
        "type": "bind",
        "read_only": True,
    },
]

# ---------------------------------------------------------------------------
# Container environment (forwarded into every DockerOperator task)
# ---------------------------------------------------------------------------


def get_task_environment() -> dict[str, str]:
    """Build the environment dict injected into every Spark task container.

    Mirrors the ``spark`` service environment from docker-compose.yml so that
    CLI commands behave identically whether run via ``docker compose exec`` or
    via Airflow's ``DockerOperator``.
    """
    return {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
        "ADZUNA_APP_ID": ADZUNA_APP_ID,
        "ADZUNA_APP_KEY": ADZUNA_APP_KEY,
        "JAVA_HOME": "/usr/lib/jvm/temurin-17-jdk",
        "PATH": (
            "/usr/lib/jvm/temurin-17-jdk/bin:"
            "/opt/skillradar/.venv/bin:"
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        ),
        "PYTHONPATH": "/opt/skillradar/src",
        "PYSPARK_PYTHON": "/opt/skillradar/.venv/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/opt/skillradar/.venv/bin/python",
        "SKILLRADAR_RUNTIME_CONTEXT": "docker",
    }
