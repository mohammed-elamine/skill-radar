"""Unit tests for Kibana NDJSON asset generation and inventory."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from skill_radar.config.models import SearchConfig
from skill_radar.platform.search.kibana_assets import (
    EXPECTED_DASHBOARDS,
    EXPECTED_DATA_VIEW_SUFFIXES,
    KibanaAssetResult,
    generate_all_assets,
    get_expected_dashboard_ids,
    get_expected_data_view_ids,
    objects_to_ndjson,
    write_ndjson_artifact,
)
from skill_radar.platform.search.kibana_models import KibanaSavedObject


@pytest.fixture
def search_config() -> SearchConfig:
    """Default search configuration for tests."""
    return SearchConfig()


# ─────────────────────────────────────────────────────────────────────────
# KibanaAssetResult
# ─────────────────────────────────────────────────────────────────────────


class TestKibanaAssetResult:
    """Tests for the KibanaAssetResult dataclass."""

    def test_success_when_no_errors(self) -> None:
        result = KibanaAssetResult(total_objects=5)
        assert result.success is True

    def test_not_success_with_errors(self) -> None:
        result = KibanaAssetResult(errors=["something went wrong"])
        assert result.success is False

    def test_default_values(self) -> None:
        result = KibanaAssetResult()
        assert result.data_views_count == 0
        assert result.visualizations_count == 0
        assert result.dashboards_count == 0
        assert result.saved_searches_count == 0
        assert result.applied is False
        assert result.artifact_path == ""


# ─────────────────────────────────────────────────────────────────────────
# NDJSON generation
# ─────────────────────────────────────────────────────────────────────────


class TestGenerateAllAssets:
    """Tests for generate_all_assets()."""

    def test_returns_nonempty_list(self, search_config: SearchConfig) -> None:
        objects = generate_all_assets(search_config)
        assert len(objects) > 0

    def test_all_objects_have_type_and_id(self, search_config: SearchConfig) -> None:
        objects = generate_all_assets(search_config)
        for obj in objects:
            assert obj.type
            assert obj.id

    def test_dependency_order(self, search_config: SearchConfig) -> None:
        """Data views must come before objects that reference them."""
        objects = generate_all_assets(search_config)
        types_in_order = [o.type for o in objects]

        # All index-patterns should appear before any lens/dashboard
        last_dv_idx = max(i for i, t in enumerate(types_in_order) if t == "index-pattern")
        first_lens_idx = min(
            (i for i, t in enumerate(types_in_order) if t == "lens"),
            default=len(types_in_order),
        )
        first_dash_idx = min(
            (i for i, t in enumerate(types_in_order) if t == "dashboard"),
            default=len(types_in_order),
        )
        assert last_dv_idx < first_lens_idx
        assert last_dv_idx < first_dash_idx

    def test_includes_all_object_types(self, search_config: SearchConfig) -> None:
        objects = generate_all_assets(search_config)
        types = {o.type for o in objects}
        assert "index-pattern" in types
        assert "lens" in types
        assert "dashboard" in types
        assert "search" in types

    def test_object_count_matches_expected(self, search_config: SearchConfig) -> None:
        objects = generate_all_assets(search_config)
        # 10 data views + 10 saved searches + 24 visualizations + 5 dashboards = 49
        assert len(objects) == 49


class TestObjectsToNdjson:
    """Tests for objects_to_ndjson()."""

    def test_empty_list(self) -> None:
        result = objects_to_ndjson([])
        assert result == "\n"

    def test_valid_ndjson_lines(self) -> None:
        objects = [
            KibanaSavedObject(type="test", id="t1", attributes={"a": 1}),
            KibanaSavedObject(type="test", id="t2", attributes={"b": 2}),
        ]
        result = objects_to_ndjson(objects)
        lines = result.strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert "type" in parsed

    def test_full_asset_set_is_valid_ndjson(self, search_config: SearchConfig) -> None:
        objects = generate_all_assets(search_config)
        ndjson = objects_to_ndjson(objects)
        lines = [line_ for line_ in ndjson.strip().split("\n") if line_]
        for line in lines:
            json.loads(line)  # must not raise


class TestWriteNdjsonArtifact:
    """Tests for write_ndjson_artifact()."""

    def test_writes_file(self, search_config: SearchConfig) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = write_ndjson_artifact(
                search_config,
                output_dir=Path(tmpdir),
                filename="test_output.ndjson",
            )
            assert result.success
            assert result.total_objects > 0
            path = Path(result.artifact_path)
            assert path.exists()
            assert path.name == "test_output.ndjson"

    def test_file_is_valid_ndjson(self, search_config: SearchConfig) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = write_ndjson_artifact(search_config, output_dir=Path(tmpdir))
            content = Path(result.artifact_path).read_text(encoding="utf-8")
            lines = [line_ for line_ in content.strip().split("\n") if line_]
            for line in lines:
                json.loads(line)

    def test_result_counts(self, search_config: SearchConfig) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = write_ndjson_artifact(search_config, output_dir=Path(tmpdir))
            assert result.data_views_count == 10
            assert result.dashboards_count == 5
            assert result.saved_searches_count == 10
            assert result.visualizations_count == 24
            assert result.total_objects == 49


# ─────────────────────────────────────────────────────────────────────────
# Expected asset inventory
# ─────────────────────────────────────────────────────────────────────────


class TestExpectedAssetInventory:
    """Tests for expected asset inventory constants and helpers."""

    def test_expected_dashboards_count(self) -> None:
        assert len(EXPECTED_DASHBOARDS) == 5

    def test_expected_dashboard_ids(self) -> None:
        ids = get_expected_dashboard_ids()
        assert len(ids) == 5
        for did in ids:
            assert did.startswith("skillradar-dash-")

    def test_expected_data_view_suffixes(self) -> None:
        assert len(EXPECTED_DATA_VIEW_SUFFIXES) == 10

    def test_expected_data_view_ids_use_config_prefix(self) -> None:
        cfg = SearchConfig(index_prefix="myapp")
        ids = get_expected_data_view_ids(cfg)
        assert len(ids) == 10
        for dvid in ids:
            assert dvid.startswith("myapp-dv-")

    def test_expected_data_view_ids_default_prefix(self) -> None:
        cfg = SearchConfig()
        ids = get_expected_data_view_ids(cfg)
        for dvid in ids:
            assert dvid.startswith("skillradar-dv-")
