"""Tests for runtime context detection and resolution.

Tests cover:
- RuntimeContext enum values
- detect_runtime_context() auto-detection
- get_runtime_context() with overrides and env vars
- _parse_context() validation
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from skill_radar.platform.runtime.context import (
    ENV_RUNTIME_CONTEXT,
    RuntimeContext,
    _parse_context,
    detect_runtime_context,
    get_runtime_context,
)

# ---------------------------------------------------------------------------
# RuntimeContext enum tests
# ---------------------------------------------------------------------------


class TestRuntimeContextEnum:
    """Tests for RuntimeContext enum."""

    def test_host_value(self) -> None:
        """HOST value is 'host'."""
        assert RuntimeContext.HOST.value == "host"

    def test_docker_value(self) -> None:
        """DOCKER value is 'docker'."""
        assert RuntimeContext.DOCKER.value == "docker"

    def test_enum_is_string(self) -> None:
        """RuntimeContext is str-based enum."""
        assert isinstance(RuntimeContext.HOST, str)
        assert RuntimeContext.HOST == "host"

    def test_all_values(self) -> None:
        """All expected values exist."""
        values = [ctx.value for ctx in RuntimeContext]
        assert set(values) == {"host", "docker"}


# ---------------------------------------------------------------------------
# detect_runtime_context() tests
# ---------------------------------------------------------------------------


class TestDetectRuntimeContext:
    """Tests for detect_runtime_context()."""

    def test_returns_host_by_default(self, tmp_path: Path) -> None:  # noqa: ARG002
        """Returns HOST when no container indicators present."""
        # Mock non-existent paths
        with patch.object(Path, "exists", return_value=False):
            result = detect_runtime_context()
            assert result == RuntimeContext.HOST

    def test_returns_docker_when_dockerenv_exists(self) -> None:
        """Returns DOCKER when /.dockerenv exists."""
        with patch.object(Path, "exists") as mock_exists:
            # /.dockerenv exists
            mock_exists.return_value = True
            result = detect_runtime_context()
            assert result == RuntimeContext.DOCKER

    def test_returns_docker_when_cgroup_has_docker(self, tmp_path: Path) -> None:
        """Returns DOCKER when /proc/1/cgroup contains 'docker'."""
        # Create mock cgroup file
        cgroup = tmp_path / "cgroup"
        cgroup.write_text("12:memory:/docker/abc123\n")

        def path_exists_mock(self: Path) -> bool:
            if str(self) == "/.dockerenv":
                return False
            return str(self) == "/proc/1/cgroup"

        def path_read_text_mock(self: Path) -> str:
            if str(self) == "/proc/1/cgroup":
                return "12:memory:/docker/abc123\n"
            raise FileNotFoundError()

        with (
            patch.object(Path, "exists", path_exists_mock),
            patch.object(Path, "read_text", path_read_text_mock),
        ):
            result = detect_runtime_context()
            assert result == RuntimeContext.DOCKER

    def test_returns_docker_when_cgroup_has_containerd(self, tmp_path: Path) -> None:  # noqa: ARG002
        """Returns DOCKER when /proc/1/cgroup contains 'containerd'."""

        def path_exists_mock(self: Path) -> bool:
            if str(self) == "/.dockerenv":
                return False
            return str(self) == "/proc/1/cgroup"

        def path_read_text_mock(self: Path) -> str:
            if str(self) == "/proc/1/cgroup":
                return "0::/system.slice/containerd.service\n"
            raise FileNotFoundError()

        with (
            patch.object(Path, "exists", path_exists_mock),
            patch.object(Path, "read_text", path_read_text_mock),
        ):
            result = detect_runtime_context()
            assert result == RuntimeContext.DOCKER


# ---------------------------------------------------------------------------
# get_runtime_context() tests
# ---------------------------------------------------------------------------


class TestGetRuntimeContext:
    """Tests for get_runtime_context()."""

    def test_returns_override_when_enum_provided(self) -> None:
        """Explicit enum override is returned."""
        result = get_runtime_context(override=RuntimeContext.DOCKER)
        assert result == RuntimeContext.DOCKER

        result = get_runtime_context(override=RuntimeContext.HOST)
        assert result == RuntimeContext.HOST

    def test_returns_override_when_string_provided(self) -> None:
        """Explicit string override is parsed and returned."""
        result = get_runtime_context(override="docker")
        assert result == RuntimeContext.DOCKER

        result = get_runtime_context(override="host")
        assert result == RuntimeContext.HOST

    def test_string_override_case_insensitive(self) -> None:
        """String override is case-insensitive."""
        assert get_runtime_context(override="DOCKER") == RuntimeContext.DOCKER
        assert get_runtime_context(override="Host") == RuntimeContext.HOST
        assert get_runtime_context(override="DOCKER") == RuntimeContext.DOCKER

    def test_env_var_overrides_detection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Environment variable overrides auto-detection."""
        monkeypatch.setenv(ENV_RUNTIME_CONTEXT, "docker")

        # Mock detection to return HOST
        with patch(
            "skill_radar.platform.runtime.context.detect_runtime_context",
            return_value=RuntimeContext.HOST,
        ):
            result = get_runtime_context()
            assert result == RuntimeContext.DOCKER

    def test_explicit_override_beats_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Explicit override takes precedence over env var."""
        monkeypatch.setenv(ENV_RUNTIME_CONTEXT, "docker")

        result = get_runtime_context(override=RuntimeContext.HOST)
        assert result == RuntimeContext.HOST

    def test_auto_detects_when_no_override_or_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Falls back to auto-detection when no override."""
        monkeypatch.delenv(ENV_RUNTIME_CONTEXT, raising=False)

        with patch(
            "skill_radar.platform.runtime.context.detect_runtime_context",
            return_value=RuntimeContext.HOST,
        ) as mock_detect:
            result = get_runtime_context()
            assert result == RuntimeContext.HOST
            mock_detect.assert_called_once()

    def test_invalid_override_raises_value_error(self) -> None:
        """Invalid string override raises ValueError."""
        with pytest.raises(ValueError, match="Invalid runtime context 'invalid'"):
            get_runtime_context(override="invalid")

    def test_invalid_env_var_raises_value_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid env var value raises ValueError."""
        monkeypatch.setenv(ENV_RUNTIME_CONTEXT, "invalid")

        with pytest.raises(ValueError, match="Invalid runtime context 'invalid'"):
            get_runtime_context()


# ---------------------------------------------------------------------------
# _parse_context() tests
# ---------------------------------------------------------------------------


class TestParseContext:
    """Tests for _parse_context()."""

    def test_parses_host(self) -> None:
        """Parses 'host' correctly."""
        assert _parse_context("host", "test") == RuntimeContext.HOST

    def test_parses_docker(self) -> None:
        """Parses 'docker' correctly."""
        assert _parse_context("docker", "test") == RuntimeContext.DOCKER

    def test_strips_whitespace(self) -> None:
        """Strips whitespace from value."""
        assert _parse_context("  host  ", "test") == RuntimeContext.HOST
        assert _parse_context("\tdocker\n", "test") == RuntimeContext.DOCKER

    def test_case_insensitive(self) -> None:
        """Parsing is case-insensitive."""
        assert _parse_context("HOST", "test") == RuntimeContext.HOST
        assert _parse_context("Docker", "test") == RuntimeContext.DOCKER

    def test_invalid_value_raises_with_source(self) -> None:
        """Invalid value raises with source in error message."""
        with pytest.raises(ValueError, match="Invalid runtime context 'bad' from env:FOO"):
            _parse_context("bad", "env:FOO")
