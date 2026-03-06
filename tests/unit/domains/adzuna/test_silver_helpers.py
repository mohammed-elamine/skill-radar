"""Tests for Silver normalization helpers (pure Python, no Spark dependency)."""

from __future__ import annotations

from skill_radar.domains.adzuna.silver.format import (
    _location_hierarchy,
    _safe_parse_json_array,
)


class TestSafeParseJsonArray:
    """_safe_parse_json_array handles various JSON inputs."""

    def test_valid_array(self) -> None:
        result = _safe_parse_json_array('["France", "Île-de-France", "Paris"]')
        assert result == ["France", "Île-de-France", "Paris"]

    def test_empty_string(self) -> None:
        assert _safe_parse_json_array("") == []

    def test_none(self) -> None:
        assert _safe_parse_json_array(None) == []

    def test_invalid_json(self) -> None:
        assert _safe_parse_json_array("not json") == []

    def test_json_object_returns_empty(self) -> None:
        assert _safe_parse_json_array('{"key": "value"}') == []

    def test_numeric_array_coerced_to_strings(self) -> None:
        result = _safe_parse_json_array("[1, 2, 3]")
        assert result == ["1", "2", "3"]


class TestLocationHierarchy:
    """_location_hierarchy derives (country, region, subregion) from area array."""

    def test_full_hierarchy(self) -> None:
        country, region, sub = _location_hierarchy(["France", "Île-de-France", "Paris"])
        assert country == "France"
        assert region == "Île-de-France"
        assert sub == "Paris"

    def test_two_levels(self) -> None:
        country, region, sub = _location_hierarchy(["France", "Bretagne"])
        assert country == "France"
        assert region == "Bretagne"
        assert sub == ""

    def test_one_level(self) -> None:
        country, region, sub = _location_hierarchy(["France"])
        assert country == "France"
        assert region == ""
        assert sub == ""

    def test_empty_array(self) -> None:
        country, region, sub = _location_hierarchy([])
        assert country == ""
        assert region == ""
        assert sub == ""

    def test_four_levels(self) -> None:
        """Extra levels beyond the third are ignored."""
        country, region, sub = _location_hierarchy(["France", "Île-de-France", "Paris", "75001"])
        assert country == "France"
        assert region == "Île-de-France"
        assert sub == "Paris"
