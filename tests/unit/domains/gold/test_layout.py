"""Unit tests for LakeLayout Gold convenience methods."""

from __future__ import annotations

import pytest

from skill_radar.config.models import PlatformSettings
from skill_radar.platform.lake.layout import LakeLayout


class TestLakeLayoutGoldHelpers:
    """LakeLayout.gold_* convenience methods produce correct FQNs."""

    @pytest.fixture
    def layout(self) -> LakeLayout:
        return LakeLayout(PlatformSettings())

    def test_gold_namespace(self, layout: LakeLayout) -> None:
        ns = layout.iceberg_namespace_name("gold")
        assert "gold" in ns.lower()
        # Default prefix is "sr" → sr_gold
        assert ns == "sr_gold"

    def test_job_skill_matches_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_job_skill_matches_fqn()
        assert fqn == "sr.sr_gold.gold_job_skill_matches"

    def test_job_occupation_matches_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_job_occupation_matches_fqn()
        assert fqn == "sr.sr_gold.gold_job_occupation_matches"

    def test_skill_demand_daily_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_skill_demand_daily_fqn()
        assert fqn == "sr.sr_gold.gold_skill_demand_daily"

    def test_salary_by_skill_daily_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_salary_by_skill_daily_fqn()
        assert fqn == "sr.sr_gold.gold_salary_by_skill_daily"

    def test_occupation_skill_graph_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_occupation_skill_graph_fqn()
        assert fqn == "sr.sr_gold.gold_occupation_skill_graph"

    def test_skill_emerging_daily_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_skill_emerging_daily_fqn()
        assert fqn == "sr.sr_gold.gold_skill_emerging_daily"

    def test_occupation_market_daily_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_occupation_market_daily_fqn()
        assert fqn == "sr.sr_gold.gold_occupation_market_daily"

    def test_skill_demand_segments_daily_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.gold_skill_demand_segments_daily_fqn()
        assert fqn == "sr.sr_gold.gold_skill_demand_segments_daily"

    def test_all_gold_fqns_are_unique(self, layout: LakeLayout) -> None:
        fqns = {
            layout.gold_job_skill_matches_fqn(),
            layout.gold_job_occupation_matches_fqn(),
            layout.gold_skill_demand_daily_fqn(),
            layout.gold_salary_by_skill_daily_fqn(),
            layout.gold_occupation_skill_graph_fqn(),
            layout.gold_skill_emerging_daily_fqn(),
            layout.gold_occupation_market_daily_fqn(),
            layout.gold_skill_demand_segments_daily_fqn(),
        }
        assert len(fqns) == 8

    def test_all_gold_fqns_use_gold_namespace(self, layout: LakeLayout) -> None:
        for method_name in (
            "gold_job_skill_matches_fqn",
            "gold_job_occupation_matches_fqn",
            "gold_skill_demand_daily_fqn",
            "gold_salary_by_skill_daily_fqn",
            "gold_occupation_skill_graph_fqn",
            "gold_skill_emerging_daily_fqn",
            "gold_occupation_market_daily_fqn",
            "gold_skill_demand_segments_daily_fqn",
        ):
            fqn = getattr(layout, method_name)()
            parts = fqn.split(".")
            assert len(parts) == 3, f"FQN should have 3 parts: {fqn}"
            assert parts[0] == "sr", f"Catalog should be 'sr': {fqn}"
            assert "gold" in parts[1], f"Namespace should contain 'gold': {fqn}"
            assert parts[2].startswith("gold_"), f"Table should start with 'gold_': {fqn}"

    def test_esco_silver_convenience_fqns(self, layout: LakeLayout) -> None:
        """ESCO Silver convenience helpers exist and return Silver FQNs."""
        skills = layout.esco_silver_skills_fqn()
        occs = layout.esco_silver_occupations_fqn()
        rels = layout.esco_silver_relations_fqn()
        assert "silver" in skills.lower()
        assert "silver" in occs.lower()
        assert "silver" in rels.lower()
        assert skills != occs != rels
