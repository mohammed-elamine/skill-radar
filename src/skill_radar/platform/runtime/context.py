"""Runtime context detection for host vs docker environments.

This module provides runtime context detection to determine whether
the application is running on the host machine or inside a Docker container.
This affects which S3/MinIO endpoint should be used.
"""

from __future__ import annotations

import os
from enum import StrEnum
from pathlib import Path


class RuntimeContext(StrEnum):
    """Runtime environment context.

    HOST: Running on host machine (use localhost endpoints)
    DOCKER: Running inside Docker container (use container network endpoints)
    """

    HOST = "host"
    DOCKER = "docker"


# Environment variable for explicit context override
ENV_RUNTIME_CONTEXT = "SKILLRADAR_RUNTIME_CONTEXT"


def detect_runtime_context() -> RuntimeContext:
    """Detect runtime context by inspecting system indicators.

    Detection strategy:
    1. Check for /.dockerenv file (Docker creates this)
    2. Check /proc/1/cgroup for docker/containerd indicators
    3. Default to HOST if no indicators found

    Returns:
        RuntimeContext.DOCKER if inside container, RuntimeContext.HOST otherwise.
    """
    # Check for .dockerenv file (most reliable indicator)
    if Path("/.dockerenv").exists():
        return RuntimeContext.DOCKER

    # Check cgroup for container indicators
    cgroup_path = Path("/proc/1/cgroup")
    if cgroup_path.exists():
        try:
            content = cgroup_path.read_text()
            # Docker and containerd both leave traces in cgroup
            if "docker" in content or "containerd" in content or "/lxc/" in content:
                return RuntimeContext.DOCKER
        except (OSError, PermissionError):
            # Can't read cgroup, assume host
            pass

    # Default to host
    return RuntimeContext.HOST


def get_runtime_context(override: RuntimeContext | str | None = None) -> RuntimeContext:
    """Get runtime context with optional override.

    Resolution order:
    1. Explicit override parameter (if provided)
    2. Environment variable SKILLRADAR_RUNTIME_CONTEXT
    3. Auto-detection via detect_runtime_context()

    Args:
        override: Explicit context override. Accepts RuntimeContext enum or string.

    Returns:
        Resolved RuntimeContext.

    Raises:
        ValueError: If override or env var contains invalid value.
    """
    # Handle explicit override
    if override is not None:
        if isinstance(override, RuntimeContext):
            return override
        if isinstance(override, str):
            return _parse_context(override, source="override")

    # Check environment variable
    env_value = os.environ.get(ENV_RUNTIME_CONTEXT)
    if env_value:
        return _parse_context(env_value, source=f"env:{ENV_RUNTIME_CONTEXT}")

    # Auto-detect
    return detect_runtime_context()


def _parse_context(value: str, source: str = "input") -> RuntimeContext:
    """Parse string value to RuntimeContext enum.

    Args:
        value: String to parse (case-insensitive).
        source: Description of where value came from (for error messages).

    Returns:
        Parsed RuntimeContext.

    Raises:
        ValueError: If value doesn't match any RuntimeContext.
    """
    normalized = value.strip().lower()
    try:
        return RuntimeContext(normalized)
    except ValueError:
        valid = [ctx.value for ctx in RuntimeContext]
        raise ValueError(
            f"Invalid runtime context '{value}' from {source}. Valid values: {valid}"
        ) from None
