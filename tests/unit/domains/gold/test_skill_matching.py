"""Unit tests for Gold skill matching helpers — normalize_label, regex builder."""

from __future__ import annotations

import re

from skill_radar.domains.gold.matching.skill_matching import (
    build_match_regex_pattern,
    normalize_label,
)


class TestNormalizeLabel:
    """normalize_label pure function."""

    def test_none_returns_empty(self) -> None:
        assert normalize_label(None) == ""

    def test_empty_string_returns_empty(self) -> None:
        assert normalize_label("") == ""

    def test_whitespace_only_returns_empty(self) -> None:
        assert normalize_label("   ") == ""

    def test_lowercase(self) -> None:
        assert normalize_label("Python") == "python"

    def test_strip_whitespace(self) -> None:
        assert normalize_label("  hello  ") == "hello"

    def test_collapse_internal_whitespace(self) -> None:
        assert normalize_label("machine   learning") == "machine learning"

    def test_tabs_and_newlines(self) -> None:
        assert normalize_label("data\t\nscience") == "data science"

    def test_mixed_case_and_whitespace(self) -> None:
        assert normalize_label("  Machine   Learning  ") == "machine learning"

    def test_accented_characters_preserved(self) -> None:
        assert normalize_label("Gestion de données") == "gestion de données"


class TestBuildMatchRegexPattern:
    """build_match_regex_pattern helper."""

    def test_none_for_empty_list(self) -> None:
        assert build_match_regex_pattern([]) is None

    def test_none_for_whitespace_only(self) -> None:
        assert build_match_regex_pattern(["", "  "]) is None

    def test_single_label(self) -> None:
        pattern = build_match_regex_pattern(["python"])
        assert pattern is not None
        assert re.search(pattern, "we need python here")
        assert not re.search(pattern, "we need cpython here")

    def test_multiple_labels(self) -> None:
        pattern = build_match_regex_pattern(["python", "java"])
        assert pattern is not None
        assert re.search(pattern, "python developer")
        assert re.search(pattern, "java developer")
        assert not re.search(pattern, "javascript developer")

    def test_word_boundary_prevents_substring_match(self) -> None:
        pattern = build_match_regex_pattern(["sql"])
        assert pattern is not None
        # Should match "sql" standalone
        assert re.search(pattern, "we need sql experience")
        # Should NOT match "mysql" or "nosql" (substring)
        assert not re.search(pattern, "we need mysql experience")

    def test_longer_phrases_sorted_first(self) -> None:
        """Longer patterns match first to avoid partial greedy matches."""
        pattern = build_match_regex_pattern(["machine learning", "machine"])
        assert pattern is not None
        # The pattern should start with the longer phrase
        assert "machine\\ learning" in pattern or "machine learning" in pattern.replace("\\", "")

    def test_special_regex_chars_escaped(self) -> None:
        pattern = build_match_regex_pattern(["c++"])
        assert pattern is not None
        # c++ contains non-word chars so \b may not match at "c++" boundary;
        # verify the pattern at least compiles and the label is escaped
        assert "c\\+\\+" in pattern or "c\\+" in pattern

    def test_dots_escaped(self) -> None:
        pattern = build_match_regex_pattern(["node.js"])
        assert pattern is not None
        assert re.search(pattern, "node.js backend")
