"""Kibana asset generation — NDJSON artifact writer and import orchestrator.

Composes all saved objects (data views, visualizations, dashboards,
saved searches) into a complete NDJSON artifact that can be:

1. Written to disk for version control or manual import
2. Pushed to Kibana via the Saved Objects import API

This module acts as the bridge between the builder layer
(:mod:`~skill_radar.platform.search.kibana_builders`) and the
client layer (:mod:`~skill_radar.platform.search.kibana`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from skill_radar.platform.search.kibana_builders import build_all_dashboards

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig
    from skill_radar.platform.search.kibana_models import KibanaSavedObject

logger = logging.getLogger(__name__)

# Default output directory for generated NDJSON artifacts
DEFAULT_OUTPUT_DIR = Path("configs/kibana")


# ═══════════════════════════════════════════════════════════════════════════
# Result model
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class KibanaAssetResult:
    """Result of a Kibana asset generation or apply operation.

    Attributes
    ----------
    data_views_count:
        Number of data views generated.
    visualizations_count:
        Number of visualizations generated.
    dashboards_count:
        Number of dashboards generated.
    saved_searches_count:
        Number of saved searches generated.
    total_objects:
        Total number of saved objects.
    artifact_path:
        Path to the generated NDJSON file (if written).
    applied:
        Whether the assets were applied to Kibana via API.
    errors:
        List of error messages from the apply operation.
    """

    data_views_count: int = 0
    visualizations_count: int = 0
    dashboards_count: int = 0
    saved_searches_count: int = 0
    total_objects: int = 0
    artifact_path: str = ""
    applied: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


# ═══════════════════════════════════════════════════════════════════════════
# NDJSON generation
# ═══════════════════════════════════════════════════════════════════════════


def generate_all_assets(config: SearchConfig) -> list[KibanaSavedObject]:
    """Generate the complete set of Kibana saved objects.

    Returns saved objects in dependency order:
    data views → saved searches → visualizations → dashboards.
    """
    data_views, visualizations, dashboards, saved_searches = build_all_dashboards(config)

    # Order matters: data views first (referenced by others)
    objects: list[KibanaSavedObject] = []
    objects.extend(data_views)
    objects.extend(saved_searches)
    objects.extend(visualizations)
    objects.extend(dashboards)

    return objects


def objects_to_ndjson(objects: list[KibanaSavedObject]) -> str:
    """Serialize saved objects to NDJSON string."""
    lines = [obj.to_ndjson_line() for obj in objects]
    return "\n".join(lines) + "\n"


def write_ndjson_artifact(
    config: SearchConfig,
    output_dir: Path | None = None,
    filename: str = "skill_radar_dashboards.ndjson",
) -> KibanaAssetResult:
    """Generate all Kibana assets and write to an NDJSON file.

    Parameters
    ----------
    config:
        Search configuration.
    output_dir:
        Target directory.  Defaults to ``configs/kibana``.
    filename:
        Output filename.

    Returns
    -------
    KibanaAssetResult
        Result with artifact path and object counts.
    """
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / filename

    objects = generate_all_assets(config)
    ndjson = objects_to_ndjson(objects)

    artifact_path.write_text(ndjson, encoding="utf-8")

    # Count by type
    dv_count = sum(1 for o in objects if o.type == "index-pattern")
    vis_count = sum(1 for o in objects if o.type == "lens")
    dash_count = sum(1 for o in objects if o.type == "dashboard")
    ss_count = sum(1 for o in objects if o.type == "search")

    result = KibanaAssetResult(
        data_views_count=dv_count,
        visualizations_count=vis_count,
        dashboards_count=dash_count,
        saved_searches_count=ss_count,
        total_objects=len(objects),
        artifact_path=str(artifact_path),
    )

    logger.info(
        "Kibana NDJSON artifact written to %s "
        "(%d data views, %d visualizations, %d dashboards, %d saved searches)",
        artifact_path,
        dv_count,
        vis_count,
        dash_count,
        ss_count,
    )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Expected asset inventory
# ═══════════════════════════════════════════════════════════════════════════

# Dashboard IDs and titles for validation
EXPECTED_DASHBOARDS: list[dict[str, str]] = [
    {
        "id": "skillradar-dash-market-overview",
        "title": "Skill Radar / Market Overview",
    },
    {
        "id": "skillradar-dash-salary-intelligence",
        "title": "Skill Radar / Salary Intelligence",
    },
    {
        "id": "skillradar-dash-occ-skill-graph",
        "title": "Skill Radar / Occupation\u2013Skill Graph Explorer",
    },
]

# Data view IDs for validation (prefix-dependent, built at runtime)
EXPECTED_DATA_VIEW_SUFFIXES: list[str] = [
    "skill-demand-daily",
    "salary-by-skill-daily",
    "occupation-skill-graph",
]


def get_expected_data_view_ids(config: SearchConfig) -> list[str]:
    """Return expected data view IDs for validation."""
    return [f"{config.index_prefix}-dv-{s}" for s in EXPECTED_DATA_VIEW_SUFFIXES]


def get_expected_dashboard_ids() -> list[str]:
    """Return expected dashboard IDs for validation."""
    return [d["id"] for d in EXPECTED_DASHBOARDS]
