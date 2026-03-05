"""Endpoint resolution based on runtime context.

This module maps runtime context to appropriate service endpoints,
enabling seamless operation in both host and containerized environments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from skill_radar.platform.runtime.context import RuntimeContext

if TYPE_CHECKING:
    from skill_radar.config.models import S3Config


def resolve_s3_endpoint(s3_config: S3Config, context: RuntimeContext) -> str:
    """Resolve S3/MinIO endpoint based on runtime context.

    Args:
        s3_config: S3 configuration containing endpoint_host and endpoint_docker.
        context: Runtime context determining which endpoint to use.

    Returns:
        Appropriate endpoint URL for the given context.

    Example:
        >>> from skill_radar.config import load_config
        >>> config = load_config()
        >>> endpoint = resolve_s3_endpoint(config.platform.storage.s3, RuntimeContext.HOST)
        >>> # Returns "http://localhost:9000" for host context
    """
    if context == RuntimeContext.HOST:
        return s3_config.endpoint_host
    elif context == RuntimeContext.DOCKER:
        return s3_config.endpoint_docker
    else:
        # Defensive: should not reach here with proper enum usage
        raise ValueError(f"Unknown runtime context: {context}")


def format_endpoint_info(endpoint: str, context: RuntimeContext) -> str:
    """Format endpoint and context for error/log messages.

    Args:
        endpoint: The endpoint URL being used.
        context: The runtime context.

    Returns:
        Formatted string like "http://localhost:9000 (context=host)"
    """
    return f"{endpoint} (context={context.value})"
