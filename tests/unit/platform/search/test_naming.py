"""Unit tests for search naming utilities."""

from __future__ import annotations

import pytest

from skill_radar.config.models import SearchConfig
from skill_radar.platform.search.naming import (
    build_alias_name,
    build_index_base_name,
    build_index_name,
)


@pytest.fixture
def search_config() -> SearchConfig:
    """Default search config for tests."""
    return SearchConfig()


class TestBuildIndexBaseName:
    """Tests for build_index_base_name()."""

    def test_default_prefix(self, search_config: SearchConfig) -> None:
        result = build_index_base_name("skill_demand_daily", search_config)
        assert result == "skillradar-skill_demand_daily"

    def test_custom_prefix(self) -> None:
        cfg = SearchConfig(index_prefix="myapp")
        result = build_index_base_name("salary_by_skill_daily", cfg)
        assert result == "myapp-salary_by_skill_daily"

    def test_underscores_preserved(self, search_config: SearchConfig) -> None:
        result = build_index_base_name("occupation_skill_graph", search_config)
        assert "occupation_skill_graph" in result


class TestBuildAliasName:
    """Tests for build_alias_name()."""

    def test_default_alias(self, search_config: SearchConfig) -> None:
        result = build_alias_name("skill_demand_daily", "fr", search_config)
        assert result == "skillradar-skill_demand_daily-fr"

    def test_alias_includes_country(self, search_config: SearchConfig) -> None:
        result = build_alias_name("salary_by_skill_daily", "gb", search_config)
        assert result.endswith("-gb")

    def test_custom_prefix_alias(self) -> None:
        cfg = SearchConfig(index_prefix="custom")
        result = build_alias_name("skill_demand_daily", "fr", cfg)
        assert result.startswith("custom-")


class TestBuildIndexName:
    """Tests for build_index_name()."""

    def test_includes_date(self, search_config: SearchConfig) -> None:
        result = build_index_name("skill_demand_daily", "2025-01-15", "fr", search_config)
        assert "2025.01.15" in result

    def test_date_format_dots(self, search_config: SearchConfig) -> None:
        """Date separators should be dots (Elasticsearch convention)."""
        result = build_index_name("skill_demand_daily", "2025-01-15", "fr", search_config)
        assert "-2025.01.15" in result

    def test_includes_country(self, search_config: SearchConfig) -> None:
        result = build_index_name("salary_by_skill_daily", "2025-06-01", "gb", search_config)
        assert "-gb-" in result

    def test_full_format(self, search_config: SearchConfig) -> None:
        result = build_index_name("skill_demand_daily", "2025-01-15", "fr", search_config)
        assert result == "skillradar-skill_demand_daily-fr-2025.01.15"
