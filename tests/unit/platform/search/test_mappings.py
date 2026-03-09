"""Unit tests for search ES mappings."""

from __future__ import annotations

from typing import ClassVar

import pytest

from skill_radar.platform.search.mappings import MAPPINGS, get_index_body, get_mapping


class TestMappingsRegistry:
    """Tests for the MAPPINGS dict."""

    EXPECTED_KEYS: ClassVar[list[str]] = [
        "skill-demand-daily",
        "salary-by-skill-daily",
        "occupation-skill-graph",
        "job-skill-matches",
        "job-occupation-matches",
        "skill-emerging-daily",
        "occupation-market-daily",
        "skill-demand-segments-daily",
        "occupation-profile-daily",
        "skill-profile-daily",
        "occupation-similarity-daily",
        "occupation-transition-daily",
    ]

    def test_all_twelve_datasets_registered(self) -> None:
        for key in self.EXPECTED_KEYS:
            assert key in MAPPINGS, f"Missing mapping key: {key}"

    def test_no_extra_keys(self) -> None:
        assert set(MAPPINGS.keys()) == set(self.EXPECTED_KEYS)

    def test_each_mapping_has_properties(self) -> None:
        for key, mapping in MAPPINGS.items():
            assert "properties" in mapping, f"No properties in mapping for {key}"

    def test_each_mapping_has_doc_id(self) -> None:
        for key, mapping in MAPPINGS.items():
            props = mapping["properties"]
            assert "doc_id" in props, f"No doc_id in {key} mapping"
            assert props["doc_id"]["type"] == "keyword"

    def test_each_mapping_has_ingestion_date(self) -> None:
        for key, mapping in MAPPINGS.items():
            props = mapping["properties"]
            assert "ingestion_date" in props, f"No ingestion_date in {key} mapping"
            assert props["ingestion_date"]["type"] == "date"

    def test_each_mapping_has_country(self) -> None:
        for key, mapping in MAPPINGS.items():
            props = mapping["properties"]
            assert "country" in props, f"No country in {key} mapping"
            assert props["country"]["type"] == "keyword"


class TestGetMapping:
    """Tests for get_mapping()."""

    def test_returns_mapping_for_valid_key(self) -> None:
        m = get_mapping("skill-demand-daily")
        assert "properties" in m

    def test_raises_for_unknown_key(self) -> None:
        with pytest.raises(KeyError):
            get_mapping("nonexistent_dataset")


class TestGetIndexBody:
    """Tests for get_index_body()."""

    def test_contains_settings_and_mappings(self) -> None:
        from skill_radar.config.models import SearchConfig

        cfg = SearchConfig()
        body = get_index_body("skill-demand-daily", cfg)
        assert "settings" in body
        assert "mappings" in body

    def test_settings_have_shards_and_replicas(self) -> None:
        from skill_radar.config.models import SearchConfig

        cfg = SearchConfig(index_shards=2, index_replicas=1)
        body = get_index_body("skill-demand-daily", cfg)
        settings = body["settings"]
        assert settings["number_of_shards"] == 2
        assert settings["number_of_replicas"] == 1

    def test_mappings_have_properties(self) -> None:
        from skill_radar.config.models import SearchConfig

        cfg = SearchConfig()
        body = get_index_body("salary-by-skill-daily", cfg)
        assert "properties" in body["mappings"]
