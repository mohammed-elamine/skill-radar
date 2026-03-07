"""Unit tests for search configuration loading."""

from __future__ import annotations

from skill_radar.config.models import PlatformSettings, SearchConfig


class TestSearchConfigDefaults:
    """Tests for SearchConfig default values."""

    def test_defaults_from_constructor(self) -> None:
        cfg = SearchConfig()
        assert cfg.enabled is True
        assert cfg.elasticsearch_url == "http://localhost:9200"
        assert cfg.index_prefix == "skillradar"
        assert cfg.index_shards == 1
        assert cfg.index_replicas == 0
        assert cfg.bulk_chunk_size == 500
        assert cfg.country_default == "fr"

    def test_custom_values(self) -> None:
        cfg = SearchConfig(
            elasticsearch_url="http://es:9200",
            index_prefix="myproject",
            index_shards=3,
            index_replicas=2,
            bulk_chunk_size=1000,
        )
        assert cfg.elasticsearch_url == "http://es:9200"
        assert cfg.index_prefix == "myproject"
        assert cfg.index_shards == 3
        assert cfg.index_replicas == 2
        assert cfg.bulk_chunk_size == 1000


class TestSearchConfigInPlatformSettings:
    """Tests for SearchConfig integration with PlatformSettings."""

    def test_search_field_exists(self) -> None:
        settings = PlatformSettings()
        assert hasattr(settings, "search")
        assert isinstance(settings.search, SearchConfig)

    def test_search_defaults_loaded(self) -> None:
        settings = PlatformSettings()
        assert settings.search.enabled is True
        assert settings.search.elasticsearch_url == "http://localhost:9200"

    def test_indices_config(self) -> None:
        """Verify indices config is accessible."""
        from skill_radar.config.models import SearchIndicesConfig

        settings = PlatformSettings()
        assert isinstance(settings.search.indices, SearchIndicesConfig)
