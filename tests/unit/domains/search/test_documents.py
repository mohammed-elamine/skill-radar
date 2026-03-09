"""Unit tests for search document builders."""

from __future__ import annotations

import hashlib
from typing import ClassVar

from skill_radar.domains.search.documents import (
    DOCUMENT_BUILDERS,
    _deterministic_id,
)


class TestDeterministicId:
    """Tests for _deterministic_id()."""

    def test_returns_20_hex_chars(self) -> None:
        result = _deterministic_id("a", "b", "c")
        assert len(result) == 20
        # Must be valid hex
        int(result, 16)

    def test_deterministic(self) -> None:
        """Same inputs always produce the same ID."""
        id1 = _deterministic_id("fr", "2025-01-15", "http://data.europa.eu/esco/skill/123")
        id2 = _deterministic_id("fr", "2025-01-15", "http://data.europa.eu/esco/skill/123")
        assert id1 == id2

    def test_different_inputs_different_ids(self) -> None:
        id1 = _deterministic_id("fr", "2025-01-15", "skill_a")
        id2 = _deterministic_id("fr", "2025-01-15", "skill_b")
        assert id1 != id2

    def test_order_matters(self) -> None:
        id1 = _deterministic_id("a", "b")
        id2 = _deterministic_id("b", "a")
        assert id1 != id2

    def test_matches_manual_sha256(self) -> None:
        """Verify the algorithm matches manual SHA-256 computation."""
        parts = ("fr", "2025-01-15", "test_uri")
        raw = "|".join(parts)
        expected = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
        assert _deterministic_id(*parts) == expected

    def test_single_part(self) -> None:
        result = _deterministic_id("only_one")
        assert len(result) == 20

    def test_empty_parts(self) -> None:
        """Empty strings are valid — produces a valid hash."""
        result = _deterministic_id("", "")
        assert len(result) == 20


class TestDocumentBuildersRegistry:
    """Tests for the DOCUMENT_BUILDERS dict."""

    EXPECTED_KEYS: ClassVar[list[str]] = [
        "build_skill_demand_documents",
        "build_salary_by_skill_documents",
        "build_occupation_skill_graph_documents",
        "build_job_skill_matches_documents",
        "build_job_occupation_matches_documents",
        "build_skill_emerging_documents",
        "build_occupation_market_documents",
        "build_skill_demand_segments_documents",
        "build_occupation_profile_documents",
        "build_skill_profile_documents",
        "build_occupation_similarity_documents",
        "build_occupation_transition_documents",
    ]

    def test_all_twelve_registered(self) -> None:
        assert set(DOCUMENT_BUILDERS.keys()) == set(self.EXPECTED_KEYS)

    def test_all_are_callable(self) -> None:
        for key, fn in DOCUMENT_BUILDERS.items():
            assert callable(fn), f"{key} builder is not callable"
