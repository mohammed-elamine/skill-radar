"""Unit tests for Kibana saved object models."""

from __future__ import annotations

import json

from skill_radar.platform.search.kibana_models import (
    DashboardObject,
    DashboardPanel,
    DataViewObject,
    KibanaSavedObject,
    LensVisualizationObject,
    SavedSearchObject,
)


class TestKibanaSavedObject:
    """Tests for the base saved object."""

    def test_to_ndjson_dict(self) -> None:
        obj = KibanaSavedObject(
            type="test",
            id="test-id",
            attributes={"title": "Test"},
        )
        d = obj.to_ndjson_dict()
        assert d["type"] == "test"
        assert d["id"] == "test-id"
        assert d["attributes"]["title"] == "Test"
        assert "references" not in d  # empty refs omitted

    def test_to_ndjson_dict_with_references(self) -> None:
        obj = KibanaSavedObject(
            type="test",
            id="test-id",
            attributes={"title": "Test"},
            references=[{"type": "index-pattern", "id": "dv-1", "name": "ref"}],
        )
        d = obj.to_ndjson_dict()
        assert len(d["references"]) == 1

    def test_to_ndjson_line_is_valid_json(self) -> None:
        obj = KibanaSavedObject(type="test", id="test-id", attributes={"key": "value"})
        line = obj.to_ndjson_line()
        parsed = json.loads(line)
        assert parsed["type"] == "test"


class TestDataViewObject:
    """Tests for DataViewObject."""

    def test_basic_data_view(self) -> None:
        dv = DataViewObject(
            id="dv-test",
            title="Test Data View",
            index_pattern="test-*",
            time_field="timestamp",
        )
        assert dv.type == "index-pattern"
        assert dv.id == "dv-test"
        assert dv.attributes["title"] == "test-*"
        assert dv.attributes["timeFieldName"] == "timestamp"
        assert dv.attributes["name"] == "Test Data View"

    def test_default_time_field(self) -> None:
        dv = DataViewObject(
            id="dv-test",
            title="Test",
            index_pattern="test-*",
        )
        assert dv.attributes["timeFieldName"] == "ingestion_date"

    def test_ndjson_roundtrip(self) -> None:
        dv = DataViewObject(
            id="dv-1",
            title="Title",
            index_pattern="idx-*",
        )
        line = dv.to_ndjson_line()
        parsed = json.loads(line)
        assert parsed["type"] == "index-pattern"
        assert parsed["id"] == "dv-1"


class TestLensVisualizationObject:
    """Tests for LensVisualizationObject."""

    def _sample_state(self) -> dict:
        return {
            "datasourceStates": {"formBased": {"layers": {}}},
            "visualization": {},
            "filters": [],
            "query": {"query": "", "language": "kuery"},
        }

    def test_basic_lens_vis(self) -> None:
        vis = LensVisualizationObject(
            id="vis-test",
            title="Test Vis",
            description="A test",
            visualization_type="lnsMetric",
            state=self._sample_state(),
            data_view_id="dv-1",
        )
        assert vis.type == "lens"
        assert vis.id == "vis-test"
        assert vis.attributes["visualizationType"] == "lnsMetric"
        # State should be a dict in attributes (Kibana 8.13 format)
        state_obj = vis.attributes["state"]
        assert isinstance(state_obj, dict)
        assert "datasourceStates" in state_obj

    def test_references_linked_to_data_view(self) -> None:
        vis = LensVisualizationObject(
            id="vis-test",
            title="Test",
            description="",
            visualization_type="lnsXY",
            state=self._sample_state(),
            data_view_id="dv-1",
            layer_id="layer1",
        )
        assert len(vis.references) == 1
        ref = vis.references[0]
        assert ref["type"] == "index-pattern"
        assert ref["id"] == "dv-1"
        assert ref["name"] == "indexpattern-datasource-layer-layer1"


class TestDashboardPanel:
    """Tests for DashboardPanel."""

    def test_to_panel_dict(self) -> None:
        panel = DashboardPanel(
            panel_id="p0",
            vis_id="vis-1",
            vis_type="lens",
            x=0,
            y=0,
            w=24,
            h=12,
        )
        d = panel.to_panel_dict()
        assert d["type"] == "lens"
        assert d["gridData"]["x"] == 0
        assert d["gridData"]["w"] == 24
        assert d["panelRefName"] == "panel_p0"

    def test_to_reference(self) -> None:
        panel = DashboardPanel(
            panel_id="p1",
            vis_id="vis-abc",
            vis_type="lens",
            x=0,
            y=0,
            w=12,
            h=8,
        )
        ref = panel.to_reference()
        assert ref["name"] == "panel_p1"
        assert ref["type"] == "lens"
        assert ref["id"] == "vis-abc"

    def test_panel_with_title(self) -> None:
        panel = DashboardPanel(
            panel_id="p0",
            vis_id="v1",
            vis_type="lens",
            x=0,
            y=0,
            w=24,
            h=12,
            title="My Panel",
        )
        d = panel.to_panel_dict()
        assert d["title"] == "My Panel"


class TestDashboardObject:
    """Tests for DashboardObject."""

    def test_basic_dashboard(self) -> None:
        panels = [
            DashboardPanel("p0", "vis-1", "lens", 0, 0, 24, 12),
            DashboardPanel("p1", "vis-2", "lens", 24, 0, 24, 12),
        ]
        dash = DashboardObject(
            id="dash-test",
            title="Test Dashboard",
            description="A test dashboard",
            panels=panels,
        )
        assert dash.type == "dashboard"
        assert dash.id == "dash-test"
        assert len(dash.references) == 2

        # panelsJSON should be valid JSON
        panels_json = json.loads(dash.attributes["panelsJSON"])
        assert len(panels_json) == 2

    def test_dashboard_options(self) -> None:
        dash = DashboardObject(
            id="d1",
            title="T",
            description="",
            panels=[],
        )
        opts = json.loads(dash.attributes["optionsJSON"])
        assert opts["useMargins"] is True
        assert opts["hidePanelTitles"] is False

    def test_time_restore(self) -> None:
        dash = DashboardObject(
            id="d1",
            title="T",
            description="",
            panels=[],
            time_restore=True,
        )
        assert dash.attributes["timeRestore"] is True


class TestSavedSearchObject:
    """Tests for SavedSearchObject."""

    def test_basic_saved_search(self) -> None:
        ss = SavedSearchObject(
            id="ss-test",
            title="Test Search",
            description="A test",
            data_view_id="dv-1",
            columns=["field_a", "field_b"],
            sort=[["field_a", "desc"]],
        )
        assert ss.type == "search"
        assert ss.id == "ss-test"
        assert ss.attributes["columns"] == ["field_a", "field_b"]
        assert len(ss.references) == 1
        assert ss.references[0]["id"] == "dv-1"

    def test_search_source_json(self) -> None:
        ss = SavedSearchObject(
            id="ss-1",
            title="T",
            description="",
            data_view_id="dv-1",
            columns=["a"],
        )
        src = json.loads(ss.attributes["kibanaSavedObjectMeta"]["searchSourceJSON"])
        assert "query" in src
        assert "indexRefName" in src
