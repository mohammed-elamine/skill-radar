"""Unit tests for search dataset registry."""

from __future__ import annotations

from typing import ClassVar

import pytest

from skill_radar.domains.search.datasets import (
    ALL_DATASET_NAMES,
    PRIMARY_DATASETS,
    SERVED_DATASETS,
    get_served_dataset,
    resolve_index_suffix,
)


class TestServedDatasetsRegistry:
    """Tests for the SERVED_DATASETS list."""

    EXPECTED_NAMES: ClassVar[list[str]] = [
        "skill_demand_daily",
        "salary_by_skill_daily",
        "occupation_skill_graph",
        "job_skill_matches",
        "job_occupation_matches",
    ]

    def test_all_five_registered(self) -> None:
        names = [ds.name for ds in SERVED_DATASETS]
        assert set(names) == set(self.EXPECTED_NAMES)

    def test_each_has_required_fields(self) -> None:
        for ds in SERVED_DATASETS:
            assert ds.name in self.EXPECTED_NAMES
            assert ds.fqn_method is not None
            assert isinstance(ds.mapping_key, str)

    def test_primary_datasets_subset(self) -> None:
        assert set(PRIMARY_DATASETS).issubset(set(ALL_DATASET_NAMES))

    def test_primary_has_three(self) -> None:
        assert len(PRIMARY_DATASETS) == 3

    def test_all_dataset_names_has_five(self) -> None:
        assert len(ALL_DATASET_NAMES) == 5


class TestGetServedDataset:
    """Tests for get_served_dataset()."""

    def test_returns_dataset(self) -> None:
        ds = get_served_dataset("skill_demand_daily")
        assert ds.name == "skill_demand_daily"

    def test_raises_for_unknown(self) -> None:
        with pytest.raises(KeyError):
            get_served_dataset("nonexistent")


class TestResolveIndexSuffix:
    """Tests for resolve_index_suffix()."""

    def test_returns_configured_suffix_if_present(self) -> None:
        from skill_radar.config.models import SearchConfig

        cfg = SearchConfig()
        ds = get_served_dataset("skill_demand_daily")
        suffix = resolve_index_suffix(ds, cfg)
        # Should use the indices config override or fall back to dataset name
        assert isinstance(suffix, str)
        assert len(suffix) > 0

    def test_returns_default_suffix_with_hyphens(self) -> None:
        from skill_radar.config.models import SearchConfig

        cfg = SearchConfig()
        ds = get_served_dataset("skill_demand_daily")
        suffix = resolve_index_suffix(ds, cfg)
        assert suffix == "skill-demand-daily"
