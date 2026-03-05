"""Tests for endpoint resolution based on runtime context.

Tests cover:
- resolve_s3_endpoint() for host and docker contexts
- format_endpoint_info() formatting
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from skill_radar.platform.runtime import RuntimeContext
from skill_radar.platform.runtime.endpoints import (
    format_endpoint_info,
    resolve_s3_endpoint,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_s3_config() -> Mock:
    """Create a mock S3Config with dual endpoints."""
    config = Mock()
    config.endpoint_host = "http://localhost:9000"
    config.endpoint_docker = "http://minio:9000"
    return config


# ---------------------------------------------------------------------------
# resolve_s3_endpoint() tests
# ---------------------------------------------------------------------------


class TestResolveS3Endpoint:
    """Tests for resolve_s3_endpoint()."""

    def test_returns_host_endpoint_for_host_context(self, mock_s3_config: Mock) -> None:
        """Returns endpoint_host when context is HOST."""
        result = resolve_s3_endpoint(mock_s3_config, RuntimeContext.HOST)
        assert result == "http://localhost:9000"

    def test_returns_docker_endpoint_for_docker_context(self, mock_s3_config: Mock) -> None:
        """Returns endpoint_docker when context is DOCKER."""
        result = resolve_s3_endpoint(mock_s3_config, RuntimeContext.DOCKER)
        assert result == "http://minio:9000"

    def test_uses_config_values(self) -> None:
        """Uses actual config values, not hardcoded."""
        config = Mock()
        config.endpoint_host = "http://custom-host:8000"
        config.endpoint_docker = "http://custom-docker:8000"

        assert resolve_s3_endpoint(config, RuntimeContext.HOST) == "http://custom-host:8000"
        assert resolve_s3_endpoint(config, RuntimeContext.DOCKER) == "http://custom-docker:8000"


# ---------------------------------------------------------------------------
# format_endpoint_info() tests
# ---------------------------------------------------------------------------


class TestFormatEndpointInfo:
    """Tests for format_endpoint_info()."""

    def test_formats_host_context(self) -> None:
        """Formats endpoint with host context."""
        result = format_endpoint_info("http://localhost:9000", RuntimeContext.HOST)
        assert result == "http://localhost:9000 (context=host)"

    def test_formats_docker_context(self) -> None:
        """Formats endpoint with docker context."""
        result = format_endpoint_info("http://minio:9000", RuntimeContext.DOCKER)
        assert result == "http://minio:9000 (context=docker)"

    def test_includes_full_endpoint(self) -> None:
        """Full endpoint URL is included."""
        result = format_endpoint_info("https://custom.endpoint:443/path", RuntimeContext.HOST)
        assert "https://custom.endpoint:443/path" in result
        assert "context=host" in result
