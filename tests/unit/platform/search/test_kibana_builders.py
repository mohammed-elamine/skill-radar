"""Unit tests for Kibana Lens visualization and dashboard builders."""

from __future__ import annotations

import json

import pytest

from skill_radar.config.models import SearchConfig
from skill_radar.domains.search.kibana_metadata import (
    DASHBOARD_DATASETS,
    PRIMARY_DASHBOARD_DATASETS,
    get_dashboard_meta,
)
from skill_radar.platform.search.kibana_builders import (
    build_all_dashboards,
    build_dashboard_suite,
    build_data_views,
    build_saved_searches,
)


@pytest.fixture
def search_config() -> SearchConfig:
    """Default search configuration for tests."""
    return SearchConfig()


# ─────────────────────────────────────────────────────────────────────────
# Data view builders
# ─────────────────────────────────────────────────────────────────────────


class TestBuildDataViews:
    """Tests for build_data_views()."""

    def test_builds_one_per_dataset(self, search_config: SearchConfig) -> None:
        views = build_data_views(search_config)
        assert len(views) == len(DASHBOARD_DATASETS)

    def test_view_ids_are_deterministic(self, search_config: SearchConfig) -> None:
        a = build_data_views(search_config)
        b = build_data_views(search_config)
        assert [v.id for v in a] == [v.id for v in b]

    def test_view_type_is_index_pattern(self, search_config: SearchConfig) -> None:
        views = build_data_views(search_config)
        for v in views:
            assert v.type == "index-pattern"

    def test_view_pattern_uses_config_prefix(self) -> None:
        cfg = SearchConfig(index_prefix="myapp")
        views = build_data_views(cfg)
        for v in views:
            assert v.attributes["title"].startswith("myapp-")

    def test_view_includes_time_field(self, search_config: SearchConfig) -> None:
        views = build_data_views(search_config)
        for v in views:
            assert "timeFieldName" in v.attributes


# ─────────────────────────────────────────────────────────────────────────
# Saved search builders
# ─────────────────────────────────────────────────────────────────────────


class TestBuildSavedSearches:
    """Tests for build_saved_searches()."""

    def test_builds_one_per_dataset(self, search_config: SearchConfig) -> None:
        searches = build_saved_searches(search_config)
        assert len(searches) == len(DASHBOARD_DATASETS)

    def test_search_type(self, search_config: SearchConfig) -> None:
        searches = build_saved_searches(search_config)
        for s in searches:
            assert s.type == "search"

    def test_search_has_columns(self, search_config: SearchConfig) -> None:
        searches = build_saved_searches(search_config)
        for s in searches:
            assert len(s.attributes["columns"]) > 0

    def test_search_ids_are_deterministic(self, search_config: SearchConfig) -> None:
        a = build_saved_searches(search_config)
        b = build_saved_searches(search_config)
        assert [s.id for s in a] == [s.id for s in b]


# ─────────────────────────────────────────────────────────────────────────
# Dashboard suite builders
# ─────────────────────────────────────────────────────────────────────────


class TestBuildDashboardSuite:
    """Tests for build_dashboard_suite()."""

    @pytest.mark.parametrize("ds_key", PRIMARY_DASHBOARD_DATASETS)
    def test_returns_visualizations_and_dashboard(
        self, search_config: SearchConfig, ds_key: str
    ) -> None:
        vises, dashboard = build_dashboard_suite(search_config, ds_key)
        assert len(vises) > 0
        assert dashboard.type == "dashboard"

    @pytest.mark.parametrize("ds_key", PRIMARY_DASHBOARD_DATASETS)
    def test_all_visualizations_are_lens(self, search_config: SearchConfig, ds_key: str) -> None:
        vises, _ = build_dashboard_suite(search_config, ds_key)
        for vis in vises:
            assert vis.type == "lens"

    @pytest.mark.parametrize("ds_key", PRIMARY_DASHBOARD_DATASETS)
    def test_dashboard_panels_match_visualizations(
        self, search_config: SearchConfig, ds_key: str
    ) -> None:
        vises, dashboard = build_dashboard_suite(search_config, ds_key)
        panels_json = json.loads(dashboard.attributes["panelsJSON"])
        assert len(panels_json) == len(vises)

    @pytest.mark.parametrize("ds_key", PRIMARY_DASHBOARD_DATASETS)
    def test_dashboard_references_match_panels(
        self, search_config: SearchConfig, ds_key: str
    ) -> None:
        vises, dashboard = build_dashboard_suite(search_config, ds_key)
        ref_ids = {r["id"] for r in dashboard.references}
        vis_ids = {v.id for v in vises}
        assert ref_ids == vis_ids

    @pytest.mark.parametrize("ds_key", PRIMARY_DASHBOARD_DATASETS)
    def test_dashboard_id_is_deterministic(self, search_config: SearchConfig, ds_key: str) -> None:
        _, d1 = build_dashboard_suite(search_config, ds_key)
        _, d2 = build_dashboard_suite(search_config, ds_key)
        assert d1.id == d2.id

    @pytest.mark.parametrize("ds_key", PRIMARY_DASHBOARD_DATASETS)
    def test_visualization_state_is_valid_dict(
        self, search_config: SearchConfig, ds_key: str
    ) -> None:
        vises, _ = build_dashboard_suite(search_config, ds_key)
        for vis in vises:
            state = vis.attributes["state"]
            assert isinstance(state, dict)
            assert "datasourceStates" in state
            assert "visualization" in state

    @pytest.mark.parametrize("ds_key", PRIMARY_DASHBOARD_DATASETS)
    def test_visualization_references_data_view(
        self, search_config: SearchConfig, ds_key: str
    ) -> None:
        vises, _ = build_dashboard_suite(search_config, ds_key)
        meta = get_dashboard_meta(ds_key)
        expected_dv_id = meta.resolve_data_view_id(search_config)
        for vis in vises:
            ref_ids = [r["id"] for r in vis.references]
            assert expected_dv_id in ref_ids

    def test_unknown_dataset_raises(self, search_config: SearchConfig) -> None:
        with pytest.raises(ValueError, match="No dashboard builder"):
            build_dashboard_suite(search_config, "nonexistent")


# ─────────────────────────────────────────────────────────────────────────
# Market Overview specifics
# ─────────────────────────────────────────────────────────────────────────


class TestMarketOverviewDashboard:
    """Tests for the Market Overview dashboard suite."""

    def test_five_visualizations(self, search_config: SearchConfig) -> None:
        vises, _ = build_dashboard_suite(search_config, "skill_demand_daily")
        assert len(vises) == 5

    def test_includes_kpi_metric(self, search_config: SearchConfig) -> None:
        vises, _ = build_dashboard_suite(search_config, "skill_demand_daily")
        metric_vises = [v for v in vises if v.attributes["visualizationType"] == "lnsMetric"]
        assert len(metric_vises) >= 1

    def test_includes_datatable(self, search_config: SearchConfig) -> None:
        vises, _ = build_dashboard_suite(search_config, "skill_demand_daily")
        table_vises = [v for v in vises if v.attributes["visualizationType"] == "lnsDatatable"]
        assert len(table_vises) >= 1

    def test_dashboard_title(self, search_config: SearchConfig) -> None:
        _, dashboard = build_dashboard_suite(search_config, "skill_demand_daily")
        assert "Market Overview" in dashboard.attributes["title"]


# ─────────────────────────────────────────────────────────────────────────
# Salary Intelligence specifics
# ─────────────────────────────────────────────────────────────────────────


class TestSalaryIntelligenceDashboard:
    """Tests for the Salary Intelligence dashboard suite."""

    def test_five_visualizations(self, search_config: SearchConfig) -> None:
        vises, _ = build_dashboard_suite(search_config, "salary_by_skill_daily")
        assert len(vises) == 5

    def test_dashboard_title(self, search_config: SearchConfig) -> None:
        _, dashboard = build_dashboard_suite(search_config, "salary_by_skill_daily")
        assert "Salary" in dashboard.attributes["title"]


# ─────────────────────────────────────────────────────────────────────────
# Occupation-Skill Graph specifics
# ─────────────────────────────────────────────────────────────────────────


class TestOccSkillGraphDashboard:
    """Tests for the Occupation-Skill Graph Explorer dashboard suite."""

    def test_six_visualizations(self, search_config: SearchConfig) -> None:
        vises, _ = build_dashboard_suite(search_config, "occupation_skill_graph")
        assert len(vises) == 6

    def test_three_kpi_metrics(self, search_config: SearchConfig) -> None:
        vises, _ = build_dashboard_suite(search_config, "occupation_skill_graph")
        metric_vises = [v for v in vises if v.attributes["visualizationType"] == "lnsMetric"]
        assert len(metric_vises) == 3

    def test_dashboard_title(self, search_config: SearchConfig) -> None:
        _, dashboard = build_dashboard_suite(search_config, "occupation_skill_graph")
        assert "Occupation" in dashboard.attributes["title"]


# ─────────────────────────────────────────────────────────────────────────
# Full build
# ─────────────────────────────────────────────────────────────────────────


class TestBuildAllDashboards:
    """Tests for build_all_dashboards()."""

    def test_returns_four_tuples(self, search_config: SearchConfig) -> None:
        dvs, vises, dashes, searches = build_all_dashboards(search_config)
        assert len(dvs) == 3
        assert len(dashes) == 3
        assert len(searches) == 3
        assert len(vises) == 5 + 5 + 6  # market + salary + occ-skill

    def test_all_ids_unique(self, search_config: SearchConfig) -> None:
        dvs, vises, dashes, searches = build_all_dashboards(search_config)
        all_ids = (
            [o.id for o in dvs]
            + [o.id for o in vises]
            + [o.id for o in dashes]
            + [o.id for o in searches]
        )
        assert len(all_ids) == len(set(all_ids)), "Duplicate IDs found"

    def test_deterministic_output(self, search_config: SearchConfig) -> None:
        a = build_all_dashboards(search_config)
        b = build_all_dashboards(search_config)
        a_ids = [o.id for category in a for o in category]
        b_ids = [o.id for category in b for o in category]
        assert a_ids == b_ids
