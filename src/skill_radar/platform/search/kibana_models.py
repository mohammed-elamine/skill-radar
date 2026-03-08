"""Typed models for Kibana saved objects.

Provides dataclass-based representations for NDJSON serialization:

- :class:`DataViewObject` — Kibana data view (``index-pattern``)
- :class:`LensVisualizationObject` — Kibana Lens visualization
- :class:`DashboardObject` — Kibana dashboard with panel layout
- :class:`SavedSearchObject` — Kibana Discover saved search

Each model serializes to the NDJSON line format expected by
``POST /api/saved_objects/_import``.  All IDs are deterministic
so that repeated generation produces identical artifacts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

# ═══════════════════════════════════════════════════════════════════════════
# Base saved object
# ═══════════════════════════════════════════════════════════════════════════


# Kibana 8.13 migration versions — prevents the saved-objects service
# from running legacy transformations that expect an older schema.
_CORE_MIGRATION_VERSION = "8.8.0"
_TYPE_MIGRATION_VERSION = "8.9.0"


@dataclass
class KibanaSavedObject:
    """Base saved object for NDJSON serialization.

    Attributes
    ----------
    type:
        Kibana saved object type (e.g. ``index-pattern``, ``lens``).
    id:
        Deterministic saved object ID.
    attributes:
        Saved object attributes dict.
    references:
        List of reference dicts linking to other saved objects.
    """

    type: str
    id: str
    attributes: dict[str, Any]
    references: list[dict[str, str]] = field(default_factory=list)
    core_migration_version: str = ""
    type_migration_version: str = ""

    def to_ndjson_dict(self) -> dict[str, Any]:
        """Return the dict representation for NDJSON serialization."""
        d: dict[str, Any] = {
            "type": self.type,
            "id": self.id,
            "attributes": self.attributes,
        }
        if self.references:
            d["references"] = self.references
        if self.core_migration_version:
            d["coreMigrationVersion"] = self.core_migration_version
        if self.type_migration_version:
            d["typeMigrationVersion"] = self.type_migration_version
        return d

    def to_ndjson_line(self) -> str:
        """Return a single NDJSON line."""
        return json.dumps(self.to_ndjson_dict(), ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════════════════
# Data view (index-pattern)
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class DataViewObject(KibanaSavedObject):
    """Kibana data view (index-pattern) saved object.

    Parameters
    ----------
    id:
        Deterministic object ID (e.g. ``skillradar-dv-skill-demand-daily``).
    title:
        Human-readable title shown in Kibana.
    index_pattern:
        Elasticsearch index pattern (e.g. ``skillradar-skill-demand-daily-*``).
    time_field:
        Default time field for time-based filtering.
    """

    def __init__(
        self,
        *,
        id: str,
        title: str,
        index_pattern: str,
        time_field: str = "ingestion_date",
    ) -> None:
        super().__init__(
            type="index-pattern",
            id=id,
            attributes={
                "title": index_pattern,
                "timeFieldName": time_field,
                "name": title,
            },
        )


# ═══════════════════════════════════════════════════════════════════════════
# Lens visualization
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class LensVisualizationObject(KibanaSavedObject):
    """Kibana Lens visualization saved object.

    The ``state`` dict is JSON-serialized into the ``state`` attribute
    string, matching the Kibana 8.x Lens saved object format.

    Parameters
    ----------
    id:
        Deterministic object ID.
    title:
        Visualization title shown in the panel header.
    description:
        Human-readable description.
    visualization_type:
        Lens type: ``lnsXY``, ``lnsMetric``, ``lnsDatatable``, ``lnsPie``.
    state:
        Lens state dict (datasourceStates, visualization, filters, query).
    data_view_id:
        ID of the referenced data view saved object.
    layer_id:
        Internal layer ID (default ``layer1``).
    """

    def __init__(
        self,
        *,
        id: str,
        title: str,
        description: str,
        visualization_type: str,
        state: dict[str, Any],
        data_view_id: str,
        layer_id: str = "layer1",
    ) -> None:
        super().__init__(
            type="lens",
            id=id,
            attributes={
                "title": title,
                "description": description,
                "visualizationType": visualization_type,
                # Kibana 8.13 expects ``state`` as a nested object,
                # NOT a JSON-encoded string.
                "state": state,
            },
            references=[
                {
                    "type": "index-pattern",
                    "id": data_view_id,
                    "name": f"indexpattern-datasource-layer-{layer_id}",
                }
            ],
            core_migration_version=_CORE_MIGRATION_VERSION,
            type_migration_version=_TYPE_MIGRATION_VERSION,
        )


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard panel
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class DashboardPanel:
    """A panel definition within a Kibana dashboard.

    Maps to one entry in the dashboard's ``panelsJSON`` array and
    one corresponding reference linking to a saved visualization.
    """

    panel_id: str
    vis_id: str
    vis_type: str  # "lens", "search"
    x: int
    y: int
    w: int
    h: int
    title: str = ""

    def to_panel_dict(self) -> dict[str, Any]:
        """Serialize to the panelsJSON entry format."""
        panel: dict[str, Any] = {
            "version": "8.13.4",
            "type": self.vis_type,
            "gridData": {
                "x": self.x,
                "y": self.y,
                "w": self.w,
                "h": self.h,
                "i": self.panel_id,
            },
            "panelIndex": self.panel_id,
            "embeddableConfig": {"enhancements": {}},
            "panelRefName": f"panel_{self.panel_id}",
        }
        if self.title:
            panel["title"] = self.title
        return panel

    def to_reference(self) -> dict[str, str]:
        """Serialize to the corresponding references entry."""
        return {
            "name": f"panel_{self.panel_id}",
            "type": self.vis_type,
            "id": self.vis_id,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class DashboardObject(KibanaSavedObject):
    """Kibana dashboard saved object.

    Composes a set of panels (by-reference to saved visualizations)
    with layout, filtering, and display options.

    Parameters
    ----------
    id:
        Deterministic dashboard ID.
    title:
        Dashboard title.
    description:
        Human-readable description.
    panels:
        Ordered list of panel definitions with grid positions.
    time_restore:
        Whether to restore the saved time range on load.
    time_from:
        Kibana time-range start expression persisted with the dashboard
        (e.g. ``"now-30d"``).  Only written when non-empty.
    time_to:
        Kibana time-range end expression persisted with the dashboard
        (e.g. ``"now"``).  Only written when non-empty.
    """

    def __init__(
        self,
        *,
        id: str,
        title: str,
        description: str,
        panels: list[DashboardPanel],
        time_restore: bool = False,
        time_from: str = "",
        time_to: str = "",
    ) -> None:
        panels_json = json.dumps(
            [p.to_panel_dict() for p in panels],
            ensure_ascii=False,
        )
        options_json = json.dumps(
            {
                "useMargins": True,
                "syncColors": False,
                "syncCursor": True,
                "syncTooltips": False,
                "hidePanelTitles": False,
            }
        )
        search_source_json = json.dumps(
            {
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            }
        )

        attrs: dict[str, Any] = {
            "title": title,
            "description": description,
            "panelsJSON": panels_json,
            "optionsJSON": options_json,
            "timeRestore": time_restore,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": search_source_json,
            },
        }
        if time_from:
            attrs["timeFrom"] = time_from
        if time_to:
            attrs["timeTo"] = time_to

        super().__init__(
            type="dashboard",
            id=id,
            attributes=attrs,
            references=[p.to_reference() for p in panels],
            core_migration_version=_CORE_MIGRATION_VERSION,
            type_migration_version=_TYPE_MIGRATION_VERSION,
        )


# ═══════════════════════════════════════════════════════════════════════════
# Saved search (Discover)
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class SavedSearchObject(KibanaSavedObject):
    """Kibana saved search (Discover) object.

    Creates a Discover-oriented search preset with column selection
    and default sort for analyst inspection.

    Parameters
    ----------
    id:
        Deterministic saved search ID.
    title:
        Saved search title.
    description:
        Human-readable description.
    data_view_id:
        ID of the referenced data view.
    columns:
        Ordered list of column field names to display.
    sort:
        Default sort specification as ``[[field, direction], ...]``.
    """

    def __init__(
        self,
        *,
        id: str,
        title: str,
        description: str,
        data_view_id: str,
        columns: list[str],
        sort: list[list[str]] | None = None,
    ) -> None:
        search_source = {
            "query": {"query": "", "language": "kuery"},
            "filter": [],
            "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
        }

        super().__init__(
            type="search",
            id=id,
            attributes={
                "title": title,
                "description": description,
                "columns": columns,
                "sort": sort or [],
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source, ensure_ascii=False),
                },
            },
            references=[
                {
                    "type": "index-pattern",
                    "id": data_view_id,
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                }
            ],
            core_migration_version=_CORE_MIGRATION_VERSION,
            type_migration_version=_TYPE_MIGRATION_VERSION,
        )
