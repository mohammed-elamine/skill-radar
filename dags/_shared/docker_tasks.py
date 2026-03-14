"""DockerOperator factory for Skill Radar Airflow tasks."""

from __future__ import annotations

import os
from pathlib import Path

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from _shared.config import (
    CONTAINER_MOUNTS,
    CONTAINER_WORKING_DIR,
    DEFAULT_PRIORITY_WEIGHT,
    DOCKER_NETWORK,
    DOCKER_PLATFORM,
    DOCKER_URL,
    SPARK_IMAGE,
    SPARK_POOL,
    get_task_environment,
)

# Resolve project root relative to the host.  When the scheduler runs
# inside a Docker container the host project root must be provided
# explicitly via SKILLRADAR_HOST_PROJECT_DIR so that bind-mounts
# resolve correctly on the Docker daemon's filesystem.
_HOST_PROJECT_DIR: str = os.environ.get(
    "SKILLRADAR_HOST_PROJECT_DIR",
    # Fallback: assume CWD of the scheduler is the project root.
    # Works for native (non-containerised) Airflow installs.
    str(Path.cwd()),
)


def _build_mounts() -> list[Mount]:
    """Convert the declarative mount spec into ``docker.types.Mount`` objects.

    Source paths are resolved relative to ``_HOST_PROJECT_DIR`` so that
    the Docker daemon sees absolute host paths regardless of where the
    Airflow scheduler itself runs.
    """
    mounts: list[Mount] = []
    for m in CONTAINER_MOUNTS:
        source = str(Path(_HOST_PROJECT_DIR) / m["source"])
        mounts.append(
            Mount(
                target=m["target"],
                source=source,
                type=m.get("type", "bind"),
                read_only=m.get("read_only", True),
            )
        )
    return mounts


def make_skill_radar_task(
    *,
    task_id: str,
    command: str,
    dag: object,
    extra_env: dict[str, str] | None = None,
    task_group: object | None = None,
    retries: int | None = None,
    pool: str | None = None,
    priority_weight: int | None = None,
) -> DockerOperator:
    """Create a ``DockerOperator`` task pre-configured for the Spark image.

    Parameters
    ----------
    task_id:
        Stable, human-readable Airflow task identifier.
    command:
        Shell command to run inside the container.  Typically a
        ``skill-radar run <stage-unit>`` CLI invocation that bundles
        processing + validation in one Spark session.
    dag:
        The parent DAG instance.
    extra_env:
        Additional environment variables merged on top of the base
        task environment (e.g. per-task params).
    task_group:
        Optional ``TaskGroup`` to assign the task to.
    retries:
        Override the default retry count for this task.
    pool:
        Airflow pool name.  Defaults to ``SPARK_POOL`` from config so
        that Spark container concurrency is bounded globally.
    priority_weight:
        Override the default priority weight for pool scheduling.

    Returns
    -------
    DockerOperator
        A fully configured task ready to be wired into a DAG graph.
    """
    env = get_task_environment()
    if extra_env:
        env.update(extra_env)

    kwargs: dict = {
        "task_id": task_id,
        "image": SPARK_IMAGE,
        "command": f'bash -lc "{command}"',
        "docker_url": DOCKER_URL,
        "network_mode": DOCKER_NETWORK,
        "environment": env,
        "mounts": _build_mounts(),
        "working_dir": CONTAINER_WORKING_DIR,
        "auto_remove": "success",
        # Disable the internal tmp-dir mount.  When the scheduler runs
        # inside a container (Docker-in-Docker), the temp path exists only
        # in the scheduler's filesystem and is unreachable by the Docker
        # daemon on the host, causing a "bind source path does not exist"
        # error on macOS/Docker Desktop.
        "mount_tmp_dir": False,
        "tty": False,
        "do_xcom_push": False,
        "dag": dag,
        "pool": pool or SPARK_POOL,
        "priority_weight": priority_weight or DEFAULT_PRIORITY_WEIGHT,
    }

    if retries is not None:
        kwargs["retries"] = retries

    if task_group is not None:
        kwargs["task_group"] = task_group

    # Optional platform override (e.g. linux/amd64 on Apple Silicon).
    if DOCKER_PLATFORM:
        kwargs["platform"] = DOCKER_PLATFORM

    return DockerOperator(**kwargs)
