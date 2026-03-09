"""Unit tests for dashboard dataset metadata registry."""

from __future__ import annotations

import pytest

from skill_radar.domains.search.kibana_metadata import (
    DASHBOARD_DATASETS,
    PRIMARY_DASHBOARD_DATASETS,
    DashboardDatasetMeta,
    FieldMeta,
    get_dashboard_meta,
    validate_all_metadata,
)


class TestFieldMeta:
    """Tests for FieldMeta dataclass."""

    def test_aggregation_field_default(self) -> None:
        fm = FieldMeta("country", "Country", "dimension")
        assert fm.aggregation_field == "country"

    def test_aggregation_field_custom(self) -> None:
        fm = FieldMeta("skill", "Skill", "dimension", agg_field="skill.raw")
        assert fm.aggregation_field == "skill.raw"

    def test_frozen(self) -> None:
        fm = FieldMeta("x", "X", "metric")
        with pytest.raises(AttributeError):
            fm.name = "y"  # type: ignore[misc]


class TestDashboardDatasetMeta:
    """Tests for DashboardDatasetMeta dataclass."""

    def test_get_field_existing(self) -> None:
        meta = get_dashboard_meta("skill_demand_daily")
        field = meta.get_field("jobs_count")
        assert field.label == "Jobs Count"
        assert field.role == "metric"

    def test_get_field_missing_raises(self) -> None:
        meta = get_dashboard_meta("skill_demand_daily")
        with pytest.raises(KeyError, match="nonexistent"):
            meta.get_field("nonexistent")

    def test_dimension_fields(self) -> None:
        meta = get_dashboard_meta("skill_demand_daily")
        dims = meta.dimension_fields()
        dim_names = [f.name for f in dims]
        assert "country" in dim_names
        assert "esco_skill_preferred_label" in dim_names

    def test_metric_fields(self) -> None:
        meta = get_dashboard_meta("skill_demand_daily")
        metrics = meta.metric_fields()
        metric_names = [f.name for f in metrics]
        assert "jobs_count" in metric_names
        assert "unique_companies_count" in metric_names

    def test_required_field_names(self) -> None:
        meta = get_dashboard_meta("skill_demand_daily")
        required = meta.required_field_names()
        assert "ingestion_date" in required
        assert "jobs_count" in required
        # Meta fields should NOT be in required
        assert "gold_run_id" not in required

    def test_validate_against_mapping_passes(self) -> None:
        meta = get_dashboard_meta("skill_demand_daily")
        errors = meta.validate_against_mapping()
        assert errors == []

    def test_validate_against_mapping_bad_suffix(self) -> None:
        meta = DashboardDatasetMeta(
            key="test",
            index_suffix="nonexistent-index",
            data_view_title="Test",
            time_field="t",
            label="Test",
            description="Test",
            fields=(FieldMeta("x", "X", "metric"),),
        )
        errors = meta.validate_against_mapping()
        assert len(errors) >= 1
        assert "No mapping" in errors[0]

    def test_resolve_data_view_pattern(self) -> None:
        from skill_radar.config.models import SearchConfig

        config = SearchConfig(index_prefix="sr")
        meta = get_dashboard_meta("skill_demand_daily")
        assert meta.resolve_data_view_pattern(config) == "sr-skill-demand-daily-*"

    def test_resolve_data_view_id(self) -> None:
        from skill_radar.config.models import SearchConfig

        config = SearchConfig(index_prefix="sr")
        meta = get_dashboard_meta("skill_demand_daily")
        assert meta.resolve_data_view_id(config) == "sr-dv-skill-demand-daily"


class TestDashboardDatasetsRegistry:
    """Tests for the metadata registry."""

    def test_all_primary_datasets_registered(self) -> None:
        for key in PRIMARY_DASHBOARD_DATASETS:
            assert key in DASHBOARD_DATASETS

    def test_registry_has_ten_entries(self) -> None:
        assert len(DASHBOARD_DATASETS) == 10

    def test_all_entries_dashboard_eligible(self) -> None:
        for meta in DASHBOARD_DATASETS.values():
            assert meta.dashboard_eligible is True

    def test_all_entries_have_time_field(self) -> None:
        for meta in DASHBOARD_DATASETS.values():
            assert meta.time_field == "ingestion_date"

    def test_get_dashboard_meta_valid(self) -> None:
        meta = get_dashboard_meta("salary_by_skill_daily")
        assert meta.key == "salary_by_skill_daily"

    def test_get_dashboard_meta_invalid_raises(self) -> None:
        with pytest.raises(KeyError, match="nonexistent"):
            get_dashboard_meta("nonexistent")


class TestValidateAllMetadata:
    """Tests for validate_all_metadata."""

    def test_validation_passes(self) -> None:
        errors = validate_all_metadata()
        assert errors == [], f"Metadata validation errors: {errors}"

    def test_all_datasets_have_fields(self) -> None:
        for meta in DASHBOARD_DATASETS.values():
            assert len(meta.fields) > 0, f"No fields for {meta.key}"

    def test_all_datasets_have_metric_fields(self) -> None:
        for meta in DASHBOARD_DATASETS.values():
            metrics = meta.metric_fields()
            assert len(metrics) > 0, f"No metric fields for {meta.key}"

    def test_skill_field_uses_raw_agg(self) -> None:
        """Verify text+keyword fields use .raw for aggregation."""
        meta = get_dashboard_meta("skill_demand_daily")
        skill = meta.get_field("esco_skill_preferred_label")
        assert skill.aggregation_field == "esco_skill_preferred_label.raw"

    def test_occupation_field_uses_raw_agg(self) -> None:
        meta = get_dashboard_meta("occupation_skill_graph")
        occ = meta.get_field("esco_occupation_preferred_label")
        assert occ.aggregation_field == "esco_occupation_preferred_label.raw"
