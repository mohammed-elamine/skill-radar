"""Unit tests for generic lakehouse validation checks (layer-aware naming).

Verifies that check-name prefixes are inferred from the table FQN rather
than hardcoded to ``bronze``.
"""

from __future__ import annotations

import pytest

from skill_radar.platform.validate.checks.lakehouse import _infer_layer


class TestInferLayer:
    """_infer_layer derives the correct prefix from a table FQN."""

    @pytest.mark.parametrize(
        ("fqn", "expected"),
        [
            ("sr.sr_bronze.esco_skills_raw", "bronze"),
            ("sr.sr_silver.adzuna_jobs", "silver"),
            ("sr.sr_gold.skill_matches", "gold"),
            ("sr.sr_misc.some_table", "lake"),
            ("random_table", "lake"),
        ],
    )
    def test_known_layers(self, fqn: str, expected: str) -> None:
        assert _infer_layer(fqn) == expected

    def test_case_insensitive(self) -> None:
        assert _infer_layer("SR.SR_SILVER.ADZUNA_JOBS") == "silver"

    def test_namespace_only(self) -> None:
        """Used by check_namespace_exists with just a namespace string."""
        assert _infer_layer("sr_bronze") == "bronze"
        assert _infer_layer("sr_silver") == "silver"
        assert _infer_layer("sr_gold") == "gold"
