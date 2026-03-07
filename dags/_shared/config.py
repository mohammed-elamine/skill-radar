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
SKILLRADAR_SPARK_POOL
    Airflow pool name for Spark containers (default: ``spark_containers``).
SKILLRADAR_SPARK_POOL_SLOTS
    Max concurrent Spark container tasks across all DAGs (default: ``2``).
SKILLRADAR_MOUNT_MODE
    ``dev`` (default) bind-mounts source code; ``prod`` relies on baked image.
SKILLRADAR_DOCKER_PLATFORM
    Optional Docker platform override (e.g. ``linux/amd64``, ``linux/arm64``).
    When set, every DockerOperator container is pinned to that platform.
    Omit to let Docker select the native platform.
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
DOCKER_PLATFORM: str | None = os.environ.get("SKILLRADAR_DOCKER_PLATFORM") or None

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
# Search pipeline defaults
# ---------------------------------------------------------------------------

SEARCH_ENABLED: bool = _env_bool("SKILLRADAR_SEARCH_ENABLED", True)
SEARCH_ES_URL_DOCKER: str = _env(
    "SKILLRADAR_SEARCH_ELASTICSEARCH_URL_DOCKER", "http://elasticsearch:9200"
)

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
# Resource discipline (pools / concurrency)
# ---------------------------------------------------------------------------

# Limits how many *Spark containers* run concurrently across all DAGs.
# Set via an Airflow pool named ``spark_containers`` (create it in the UI
# or via ``airflow pools set spark_containers <slots> "…"``).
SPARK_POOL: str = _env("SKILLRADAR_SPARK_POOL", "spark_containers")
SPARK_POOL_SLOTS: int = _env_int("SKILLRADAR_SPARK_POOL_SLOTS", 2)

# Task-level priority weight (higher = scheduled first when pool is
# contended).  Individual DAGs can override per task.
DEFAULT_PRIORITY_WEIGHT: int = _env_int("SKILLRADAR_DEFAULT_PRIORITY_WEIGHT", 1)

# ---------------------------------------------------------------------------
# Mount mode (dev vs production)
# ---------------------------------------------------------------------------

# In *dev* mode the source tree, pyproject.toml and uv.lock are bind-mounted
# into every container so code changes are reflected immediately.
# In *prod* mode these are baked into the image and only config, logs and
# the incoming dropzone are mounted.
MOUNT_MODE: str = _env("SKILLRADAR_MOUNT_MODE", "dev")  # "dev" | "prod"

# ---------------------------------------------------------------------------
# Container mounts (host paths → container paths)
# ---------------------------------------------------------------------------

# These reproduce the same mounts the ``spark`` service uses in
# docker-compose.yml so that ephemeral DockerOperator containers have
# identical access to code, config, and logs.
CONTAINER_WORKING_DIR: str = "/opt/skillradar"

# Mounts required only during **development** (code is baked into the
# image for production builds).
_DEV_MOUNTS: list[dict[str, str]] = [
    {"source": "src", "target": "/opt/skillradar/src", "type": "bind", "read_only": True},
    {
        "source": "pyproject.toml",
        "target": "/opt/skillradar/pyproject.toml",
        "type": "bind",
        "read_only": True,
    },
    {"source": "uv.lock", "target": "/opt/skillradar/uv.lock", "type": "bind", "read_only": True},
]

# Mounts shared by **all** modes (config, logs, jobs, dropzone).
_COMMON_MOUNTS: list[dict[str, str]] = [
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
    {"source": "logs", "target": "/opt/skillradar/logs", "type": "bind", "read_only": False},
    {"source": "jobs", "target": "/opt/skillradar/jobs", "type": "bind", "read_only": True},
    {
        "source": "data/incoming",
        "target": "/opt/skillradar/incoming",
        "type": "bind",
        "read_only": True,
    },
]

# Pre-built list for backward compatibility — used by the task factory.
CONTAINER_MOUNTS: list[dict[str, str]] = (
    _DEV_MOUNTS + _COMMON_MOUNTS if MOUNT_MODE == "dev" else list(_COMMON_MOUNTS)
)

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
        "JAVA_HOME": "/opt/java/openjdk",
        "PATH": (
            "/opt/java/openjdk/bin:"
            "/opt/skillradar/.venv/bin:"
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        ),
        "PYTHONPATH": "/opt/skillradar/src",
        "PYSPARK_PYTHON": "/opt/skillradar/.venv/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/opt/skillradar/.venv/bin/python",
        "SKILLRADAR_RUNTIME_CONTEXT": "docker",
    }
