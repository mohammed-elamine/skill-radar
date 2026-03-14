"""Kibana Lens visualization and dashboard builders.

Constructs saved object payloads from dashboard dataset metadata.
Field bindings come exclusively from
:mod:`skill_radar.domains.search.kibana_metadata`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from skill_radar.domains.search.kibana_metadata import (
    DashboardDatasetMeta,
    get_dashboard_meta,
)
from skill_radar.platform.search.kibana_models import (
    DashboardObject,
    DashboardPanel,
    DataViewObject,
    LensVisualizationObject,
    SavedSearchObject,
)

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig

logger = logging.getLogger(__name__)

# ── Naming conventions ────────────────────────────────────────────────────

_LAYER = "layer1"
_ID_PREFIX = "skillradar"


def _vis_id(dashboard_slug: str, viz_slug: str) -> str:
    """Deterministic visualization ID."""
    return f"{_ID_PREFIX}-vis-{dashboard_slug}-{viz_slug}"


def _dash_id(slug: str) -> str:
    """Deterministic dashboard ID."""
    return f"{_ID_PREFIX}-dash-{slug}"


def _ss_id(suffix: str) -> str:
    """Deterministic saved search ID."""
    return f"{_ID_PREFIX}-ss-{suffix}"


# ═══════════════════════════════════════════════════════════════════════════
# Lens column builders (low-level)
# ═══════════════════════════════════════════════════════════════════════════


def _col_count(col_id: str, label: str = "Count") -> dict[str, Any]:
    """Build a count metric column."""
    return {
        col_id: {
            "label": label,
            "dataType": "number",
            "operationType": "count",
            "isBucketed": False,
            "scale": "ratio",
            "sourceField": "___records___",
        }
    }


def _col_sum(col_id: str, source_field: str, label: str) -> dict[str, Any]:
    """Build a sum metric column."""
    return {
        col_id: {
            "label": label,
            "dataType": "number",
            "operationType": "sum",
            "isBucketed": False,
            "scale": "ratio",
            "sourceField": source_field,
        }
    }


def _col_average(col_id: str, source_field: str, label: str) -> dict[str, Any]:
    """Build an average metric column."""
    return {
        col_id: {
            "label": label,
            "dataType": "number",
            "operationType": "average",
            "isBucketed": False,
            "scale": "ratio",
            "sourceField": source_field,
        }
    }


def _col_max(col_id: str, source_field: str, label: str) -> dict[str, Any]:
    """Build a max metric column."""
    return {
        col_id: {
            "label": label,
            "dataType": "number",
            "operationType": "max",
            "isBucketed": False,
            "scale": "ratio",
            "sourceField": source_field,
        }
    }


def _col_unique_count(col_id: str, source_field: str, label: str) -> dict[str, Any]:
    """Build a unique count (cardinality) metric column."""
    return {
        col_id: {
            "label": label,
            "dataType": "number",
            "operationType": "unique_count",
            "isBucketed": False,
            "scale": "ratio",
            "sourceField": source_field,
        }
    }


def _col_terms(
    col_id: str,
    source_field: str,
    label: str,
    *,
    size: int = 20,
    order_by_col: str = "",
    order_direction: str = "desc",
) -> dict[str, Any]:
    """Build a terms bucket column."""
    order_by: dict[str, Any] = (
        {"type": "column", "columnId": order_by_col} if order_by_col else {"type": "alphabetical"}
    )
    return {
        col_id: {
            "label": label,
            "dataType": "string",
            "operationType": "terms",
            "isBucketed": True,
            "scale": "ordinal",
            "sourceField": source_field,
            "params": {
                "size": size,
                "orderBy": order_by,
                "orderDirection": order_direction,
                "missingBucket": False,
            },
        }
    }


def _col_date_histogram(
    col_id: str,
    source_field: str,
    label: str = "Date",
    *,
    interval: str = "auto",
) -> dict[str, Any]:
    """Build a date histogram bucket column."""
    return {
        col_id: {
            "label": label,
            "dataType": "date",
            "operationType": "date_histogram",
            "isBucketed": True,
            "scale": "interval",
            "sourceField": source_field,
            "params": {"interval": interval},
        }
    }


# ═══════════════════════════════════════════════════════════════════════════
# Lens state composers (mid-level)
# ═══════════════════════════════════════════════════════════════════════════


def _build_lens_state(
    layer_id: str,
    columns: dict[str, Any],
    visualization: dict[str, Any],
    *,
    data_view_id: str = "",
) -> dict[str, Any]:
    """Compose a complete Lens state dict from columns + visualization.

    Parameters
    ----------
    data_view_id:
        When provided the layer includes an explicit ``indexPatternId``
        so the formBased datasource can resolve the data view without
        relying solely on the saved-object reference indirection.
    """
    layer: dict[str, Any] = {
        "columns": columns,
        "columnOrder": list(columns.keys()),
        "incompleteColumns": {},
    }
    if data_view_id:
        layer["indexPatternId"] = data_view_id
    return {
        "datasourceStates": {
            "formBased": {
                "layers": {
                    layer_id: layer,
                }
            }
        },
        "visualization": visualization,
        "filters": [],
        "query": {"query": "", "language": "kuery"},
        "adHocDataViews": {},
    }


def _metric_vis(layer_id: str, metric_col_id: str) -> dict[str, Any]:
    """Metric visualization config."""
    return {
        "layerId": layer_id,
        "layerType": "data",
        "metricAccessor": metric_col_id,
    }


def _xy_vis(
    layer_id: str,
    x_accessor: str,
    y_accessors: list[str],
    series_type: str = "bar_horizontal",
) -> dict[str, Any]:
    """XY (bar, line, area) visualization config."""
    return {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "preferredSeriesType": series_type,
        "layers": [
            {
                "layerId": layer_id,
                "layerType": "data",
                "seriesType": series_type,
                "xAccessor": x_accessor,
                "accessors": y_accessors,
            }
        ],
    }


def _datatable_vis(layer_id: str, column_ids: list[str]) -> dict[str, Any]:
    """Data table visualization config."""
    return {
        "layerId": layer_id,
        "layerType": "data",
        "columns": [{"columnId": cid} for cid in column_ids],
    }


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard A — Market Overview (skill_demand_daily)
# ═══════════════════════════════════════════════════════════════════════════

_MARKET_SLUG = "market-overview"


def _build_market_overview(
    meta: DashboardDatasetMeta,
    data_view_id: str,
) -> tuple[list[LensVisualizationObject], DashboardObject]:
    """Build the Market Overview dashboard suite.

    Visualizations:
    0. KPI: total records
    1. Bar: top 20 skills by jobs_count
    2. Line: demand trend over ingestion_date
    3. Table: ranked skills with metrics
    4. Bar: title vs description match distribution
    """
    slug = _MARKET_SLUG
    skill_agg = meta.get_field("esco_skill_preferred_label").aggregation_field
    vises: list[LensVisualizationObject] = []
    panels: list[DashboardPanel] = []

    # ── 0. KPI: Total Records ─────────────────────────────────────────
    cols = {**_col_count("c1", "Total Records")}
    state = _build_lens_state(_LAYER, cols, _metric_vis(_LAYER, "c1"), data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "kpi-total-records"),
        title="Skill Demand — Total Records",
        description="Total skill-demand records in current filtered scope",
        visualization_type="lnsMetric",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p0", vis.id, "lens", x=0, y=0, w=16, h=8))

    # ── 1. Bar: Top 20 Demanded Skills ────────────────────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=20, order_by_col="c2"),
        **_col_sum("c2", "jobs_count", "Jobs Count"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-skills-bar"),
        title="Skill Demand — Top Skills by Jobs Count",
        description="Top 20 demanded skills ranked by total job postings",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p1", vis.id, "lens", x=0, y=8, w=24, h=14))

    # ── 2. Line: Demand Trend Over Time ───────────────────────────────
    cols = {
        **_col_date_histogram("c1", "ingestion_date", "Date"),
        **_col_sum("c2", "jobs_count", "Jobs Count"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "line"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "demand-trend-line"),
        title="Skill Demand — Trend Over Time",
        description="Skill demand trend over ingestion dates",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p2", vis.id, "lens", x=24, y=8, w=24, h=14))

    # ── 3. Table: Ranked Skills ───────────────────────────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=50, order_by_col="c2"),
        **_col_sum("c2", "jobs_count", "Jobs Count"),
        **_col_sum("c3", "unique_companies_count", "Unique Companies"),
        **_col_sum("c4", "unique_locations_count", "Unique Locations"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _datatable_vis(_LAYER, ["c1", "c2", "c3", "c4"]),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "skills-table"),
        title="Skill Demand — Ranked Skills Table",
        description="Skills ranked by demand with company and location counts",
        visualization_type="lnsDatatable",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p3", vis.id, "lens", x=0, y=22, w=48, h=16))

    # ── 4. Bar: Title vs Description Match Distribution ───────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=15, order_by_col="c2"),
        **_col_sum("c2", "title_match_jobs_count", "Title Matches"),
        **_col_sum("c3", "description_match_jobs_count", "Description Matches"),
    }
    xy_config = {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "preferredSeriesType": "bar_horizontal_stacked",
        "layers": [
            {
                "layerId": _LAYER,
                "layerType": "data",
                "seriesType": "bar_horizontal_stacked",
                "xAccessor": "c1",
                "accessors": ["c2", "c3"],
            }
        ],
    }
    state = _build_lens_state(_LAYER, cols, xy_config, data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "match-distribution"),
        title="Skill Demand — Match Source Distribution",
        description="Title vs description match distribution for top skills",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p4", vis.id, "lens", x=0, y=38, w=48, h=14))

    # ── Dashboard composition ─────────────────────────────────────────
    dashboard = DashboardObject(
        id=_dash_id(slug),
        title="Skill Radar / Market Overview",
        description="Top-level monitoring of labor market skill demand",
        panels=panels,
        time_restore=True,
        time_from="now-30d",
        time_to="now",
    )

    return vises, dashboard


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard B — Salary Intelligence (salary_by_skill_daily)
# ═══════════════════════════════════════════════════════════════════════════

_SALARY_SLUG = "salary-intelligence"


def _build_salary_intelligence(
    meta: DashboardDatasetMeta,
    data_view_id: str,
) -> tuple[list[LensVisualizationObject], DashboardObject]:
    """Build the Salary Intelligence dashboard suite.

    Visualizations:
    0. KPI: skills with salary evidence
    1. Bar: top 20 skills by average salary
    2. Bar: skills with highest salary job volume
    3. Line: salary trend over time
    4. Table: skill salary details
    """
    slug = _SALARY_SLUG
    skill_agg = meta.get_field("esco_skill_preferred_label").aggregation_field
    vises: list[LensVisualizationObject] = []
    panels: list[DashboardPanel] = []

    # ── 0. KPI: Skills with Salary Data ───────────────────────────────
    cols = {**_col_count("c1", "Skills with Salary Data")}
    state = _build_lens_state(_LAYER, cols, _metric_vis(_LAYER, "c1"), data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "kpi-salary-count"),
        title="Salary — Skills with Salary Evidence",
        description="Number of skill records with salary data",
        visualization_type="lnsMetric",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p0", vis.id, "lens", x=0, y=0, w=16, h=8))

    # ── 1. Bar: Top Skills by Average Salary ──────────────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=20, order_by_col="c2"),
        **_col_average("c2", "avg_salary_mean", "Avg Salary"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-salary-bar"),
        title="Salary — Top Skills by Average Salary",
        description="Top 20 skills ranked by average salary",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p1", vis.id, "lens", x=0, y=8, w=24, h=14))

    # ── 2. Bar: Skills with Highest Salary Job Volume ─────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=20, order_by_col="c2"),
        **_col_sum("c2", "salary_jobs_count", "Salary Jobs Count"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-demand-salary-bar"),
        title="Salary — Top Skills by Salary Job Volume",
        description="Skills with the most salary-bearing job postings",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p2", vis.id, "lens", x=24, y=8, w=24, h=14))

    # ── 3. Line: Salary Trend Over Time ───────────────────────────────
    cols = {
        **_col_date_histogram("c1", "ingestion_date", "Date"),
        **_col_average("c2", "avg_salary_mean", "Avg Salary"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "line"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "salary-trend-line"),
        title="Salary — Average Salary Trend Over Time",
        description="Average salary trend across ingestion dates",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p3", vis.id, "lens", x=0, y=22, w=48, h=12))

    # ── 4. Table: Skill Salary Details ────────────────────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=50, order_by_col="c2"),
        **_col_average("c2", "avg_salary_mean", "Avg Salary"),
        **_col_max("c3", "max_salary_max", "Max Salary"),
        **_col_sum("c4", "salary_jobs_count", "Salary Jobs"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _datatable_vis(_LAYER, ["c1", "c2", "c3", "c4"]),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "salary-table"),
        title="Salary — Skill Salary Details Table",
        description="Salary metrics per skill: average, max, job count",
        visualization_type="lnsDatatable",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p4", vis.id, "lens", x=0, y=34, w=48, h=16))

    # ── Dashboard composition ─────────────────────────────────────────
    dashboard = DashboardObject(
        id=_dash_id(slug),
        title="Skill Radar / Salary Intelligence",
        description="Salary insights by skill from job postings",
        panels=panels,
        time_restore=True,
        time_from="now-30d",
        time_to="now",
    )

    return vises, dashboard


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard C - Occupation-Skill Graph Explorer (occupation_skill_graph)
# ═══════════════════════════════════════════════════════════════════════════

_OCC_SKILL_SLUG = "occ-skill-graph"


def _build_occ_skill_graph(
    meta: DashboardDatasetMeta,
    data_view_id: str,
) -> tuple[list[LensVisualizationObject], DashboardObject]:
    """Build the Occupation-Skill Graph Explorer dashboard suite.

    Visualizations:
    0. KPI: total relationship rows
    1. KPI: unique occupations
    2. KPI: unique skills
    3. Bar: top skills by matched jobs
    4. Bar: top occupations by matched jobs
    5. Table: occupation ↔ skill relationship rows
    """
    slug = _OCC_SKILL_SLUG
    occ_agg = meta.get_field("esco_occupation_preferred_label").aggregation_field
    skill_agg = meta.get_field("esco_skill_preferred_label").aggregation_field
    vises: list[LensVisualizationObject] = []
    panels: list[DashboardPanel] = []

    # ── 0. KPI: Total Relationships ───────────────────────────────────
    cols = {**_col_count("c1", "Total Relationships")}
    state = _build_lens_state(_LAYER, cols, _metric_vis(_LAYER, "c1"), data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "kpi-total-relations"),
        title="Occ-Skill — Total Relationships",
        description="Total occupation-skill relationship records",
        visualization_type="lnsMetric",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p0", vis.id, "lens", x=0, y=0, w=16, h=8))

    # ── 1. KPI: Unique Occupations ────────────────────────────────────
    cols = {**_col_unique_count("c1", "esco_occupation_concept_uri", "Unique Occupations")}
    state = _build_lens_state(_LAYER, cols, _metric_vis(_LAYER, "c1"), data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "kpi-unique-occupations"),
        title="Occ-Skill — Unique Occupations",
        description="Number of distinct occupations in scope",
        visualization_type="lnsMetric",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p1", vis.id, "lens", x=16, y=0, w=16, h=8))

    # ── 2. KPI: Unique Skills ─────────────────────────────────────────
    cols = {**_col_unique_count("c1", "esco_skill_concept_uri", "Unique Skills")}
    state = _build_lens_state(_LAYER, cols, _metric_vis(_LAYER, "c1"), data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "kpi-unique-skills"),
        title="Occ-Skill — Unique Skills",
        description="Number of distinct skills in scope",
        visualization_type="lnsMetric",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p2", vis.id, "lens", x=32, y=0, w=16, h=8))

    # ── 3. Bar: Top Skills by Matched Jobs ────────────────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=20, order_by_col="c2"),
        **_col_sum("c2", "matched_jobs_count", "Matched Jobs"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-skills-bar"),
        title="Occ-Skill — Top Skills by Matched Jobs",
        description="Skills with the most occupation-skill relationship evidence",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p3", vis.id, "lens", x=0, y=8, w=24, h=14))

    # ── 4. Bar: Top Occupations by Matched Jobs ──────────────────────
    cols = {
        **_col_terms("c1", occ_agg, "Occupation", size=20, order_by_col="c2"),
        **_col_sum("c2", "matched_jobs_count", "Matched Jobs"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-occupations-bar"),
        title="Occ-Skill — Top Occupations by Matched Jobs",
        description="Occupations with the most skill-relationship evidence",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p4", vis.id, "lens", x=24, y=8, w=24, h=14))

    # ── 5. Table: Occupation ↔ Skill Relationship Rows ────────────────
    cols = {
        **_col_terms("c1", occ_agg, "Occupation", size=50, order_by_col="c4"),
        **_col_terms("c2", skill_agg, "Skill", size=50, order_by_col="c4"),
        **_col_terms("c3", "relation_type", "Relation Type", size=10, order_by_col="c4"),
        **_col_sum("c4", "matched_jobs_count", "Matched Jobs"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _datatable_vis(_LAYER, ["c1", "c2", "c3", "c4"]),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "relations-table"),
        title="Occ-Skill — Relationship Table",
        description="Occupation-skill relationships with evidence and type",
        visualization_type="lnsDatatable",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p5", vis.id, "lens", x=0, y=22, w=48, h=16))

    # ── Dashboard composition ─────────────────────────────────────────
    dashboard = DashboardObject(
        id=_dash_id(slug),
        title="Skill Radar / Occupation\u2013Skill Graph Explorer",
        description="Explore occupation-skill relationships and evidence",
        panels=panels,
        time_restore=True,
        time_from="now-30d",
        time_to="now",
    )

    return vises, dashboard


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard D — Emerging Skills & Market Signals (skill_emerging_daily)
# ═══════════════════════════════════════════════════════════════════════════

_EMERGING_SLUG = "emerging-signals"


def _build_emerging_signals(
    meta: DashboardDatasetMeta,
    data_view_id: str,
) -> tuple[list[LensVisualizationObject], DashboardObject]:
    """Build the Emerging Skills & Market Signals dashboard suite.

    Visualizations:
    0. KPI: total emerging skill records
    1. Bar: top 20 skills by emerging composite score
    2. Line: emerging composite score trend over time
    3. Table: skill details with all signal scores
    """
    slug = _EMERGING_SLUG
    skill_agg = meta.get_field("esco_skill_preferred_label").aggregation_field
    vises: list[LensVisualizationObject] = []
    panels: list[DashboardPanel] = []

    # ── 0. KPI: Total Emerging Records ────────────────────────────────
    cols = {**_col_count("c1", "Total Emerging Records")}
    state = _build_lens_state(_LAYER, cols, _metric_vis(_LAYER, "c1"), data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "kpi-total-emerging"),
        title="Emerging Skills \u2014 Total Records",
        description="Total emerging-skill signal records",
        visualization_type="lnsMetric",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p0", vis.id, "lens", x=0, y=0, w=16, h=8))

    # ── 1. Bar: Top 20 by Emerging Score ──────────────────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=20, order_by_col="c2"),
        **_col_average("c2", "emerging_composite_score", "Emerging Score"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-emerging-bar"),
        title="Emerging Skills \u2014 Top by Composite Score",
        description="Top 20 skills ranked by emerging composite score",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p1", vis.id, "lens", x=0, y=8, w=24, h=14))

    # ── 2. Line: Composite Score Trend ────────────────────────────────
    cols = {
        **_col_date_histogram("c1", "ingestion_date", "Date"),
        **_col_average("c2", "emerging_composite_score", "Avg Emerging Score"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "line"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "emerging-trend-line"),
        title="Emerging Skills \u2014 Score Trend Over Time",
        description="Average emerging composite score across ingestion dates",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p2", vis.id, "lens", x=24, y=8, w=24, h=14))

    # ── 3. Table: Skill Signal Details ────────────────────────────────
    cols = {
        **_col_terms("c1", skill_agg, "Skill", size=50, order_by_col="c5"),
        **_col_average("c2", "momentum_score", "Momentum"),
        **_col_average("c3", "acceleration_score", "Acceleration"),
        **_col_average("c4", "novelty_score", "Novelty"),
        **_col_average("c5", "emerging_composite_score", "Emerging Score"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _datatable_vis(_LAYER, ["c1", "c2", "c3", "c4", "c5"]),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "signals-table"),
        title="Emerging Skills \u2014 Signal Details Table",
        description="Skill-level momentum, acceleration, novelty, and composite scores",
        visualization_type="lnsDatatable",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p3", vis.id, "lens", x=0, y=22, w=48, h=16))

    # ── Dashboard composition ─────────────────────────────────────────
    dashboard = DashboardObject(
        id=_dash_id(slug),
        title="Skill Radar / Emerging Skills & Market Signals",
        description="Emerging skill signals: momentum, acceleration, novelty",
        panels=panels,
        time_restore=True,
        time_from="now-30d",
        time_to="now",
    )

    return vises, dashboard


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard E — Career Navigation Explorer
# ═══════════════════════════════════════════════════════════════════════════

_CAREER_NAV_SLUG = "career-navigation"


def _build_career_navigation(
    meta: DashboardDatasetMeta,
    data_view_id: str,
) -> tuple[list[LensVisualizationObject], DashboardObject]:
    """Build the Career Navigation Explorer dashboard suite.

    Uses occupation_profile_daily as primary data view. Panels:
    0. KPI: total occupation profiles
    1. Bar: top 20 occupations by matched jobs
    2. Bar: top 20 occupations by average salary
    3. Table: occupation profile details
    """
    slug = _CAREER_NAV_SLUG
    occ_agg = meta.get_field("esco_occupation_preferred_label").aggregation_field
    vises: list[LensVisualizationObject] = []
    panels: list[DashboardPanel] = []

    # ── 0. KPI: Total Occupation Profiles ─────────────────────────────
    cols = {**_col_count("c1", "Total Occupation Profiles")}
    state = _build_lens_state(_LAYER, cols, _metric_vis(_LAYER, "c1"), data_view_id=data_view_id)
    vis = LensVisualizationObject(
        id=_vis_id(slug, "kpi-total-profiles"),
        title="Career Navigation \u2014 Total Occupation Profiles",
        description="Total canonical occupation profiles available",
        visualization_type="lnsMetric",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p0", vis.id, "lens", x=0, y=0, w=16, h=8))

    # ── 1. Bar: Top 20 Occupations by Jobs ────────────────────────────
    cols = {
        **_col_terms("c1", occ_agg, "Occupation", size=20, order_by_col="c2"),
        **_col_sum("c2", "matched_jobs_count", "Matched Jobs"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-occ-jobs-bar"),
        title="Career Navigation \u2014 Top Occupations by Demand",
        description="Top 20 occupations ranked by matched job postings",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p1", vis.id, "lens", x=0, y=8, w=24, h=14))

    # ── 2. Bar: Top 20 Occupations by Salary ──────────────────────────
    cols = {
        **_col_terms("c1", occ_agg, "Occupation", size=20, order_by_col="c2"),
        **_col_average("c2", "avg_salary_mean", "Avg Salary"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _xy_vis(_LAYER, "c1", ["c2"], "bar_horizontal"),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "top-occ-salary-bar"),
        title="Career Navigation \u2014 Top Occupations by Salary",
        description="Top 20 occupations ranked by average salary",
        visualization_type="lnsXY",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p2", vis.id, "lens", x=24, y=8, w=24, h=14))

    # ── 3. Table: Occupation Profile Details ──────────────────────────
    cols = {
        **_col_terms("c1", occ_agg, "Occupation", size=50, order_by_col="c2"),
        **_col_sum("c2", "matched_jobs_count", "Jobs"),
        **_col_sum("c3", "distinct_companies_count", "Companies"),
        **_col_average("c4", "avg_salary_mean", "Avg Salary"),
        **_col_sum("c5", "related_skills_count", "Related Skills"),
    }
    state = _build_lens_state(
        _LAYER,
        cols,
        _datatable_vis(_LAYER, ["c1", "c2", "c3", "c4", "c5"]),
        data_view_id=data_view_id,
    )
    vis = LensVisualizationObject(
        id=_vis_id(slug, "occ-profile-table"),
        title="Career Navigation \u2014 Occupation Profile Details",
        description="Occupation profiles with demand, salary, and skill metrics",
        visualization_type="lnsDatatable",
        state=state,
        data_view_id=data_view_id,
    )
    vises.append(vis)
    panels.append(DashboardPanel(f"{slug}-p3", vis.id, "lens", x=0, y=22, w=48, h=16))

    # ── Dashboard composition ─────────────────────────────────────────
    dashboard = DashboardObject(
        id=_dash_id(slug),
        title="Skill Radar / Career Navigation Explorer",
        description="Occupation profiles, similarity, and transition guidance",
        panels=panels,
        time_restore=True,
        time_from="now-30d",
        time_to="now",
    )

    return vises, dashboard


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard builder registry
# ═══════════════════════════════════════════════════════════════════════════

_DASHBOARD_BUILDERS: dict[str, tuple[str, Any]] = {
    "skill_demand_daily": (_MARKET_SLUG, _build_market_overview),
    "salary_by_skill_daily": (_SALARY_SLUG, _build_salary_intelligence),
    "occupation_skill_graph": (_OCC_SKILL_SLUG, _build_occ_skill_graph),
    "skill_emerging_daily": (_EMERGING_SLUG, _build_emerging_signals),
    "occupation_profile_daily": (_CAREER_NAV_SLUG, _build_career_navigation),
}


# ═══════════════════════════════════════════════════════════════════════════
# Public builder API
# ═══════════════════════════════════════════════════════════════════════════


def build_data_views(
    config: SearchConfig,
    datasets: list[DashboardDatasetMeta] | None = None,
) -> list[DataViewObject]:
    """Build data view saved objects from metadata.

    Parameters
    ----------
    config:
        Search configuration for index prefix resolution.
    datasets:
        Specific datasets to build views for.  Defaults to all
        dashboard-eligible datasets.
    """
    from skill_radar.domains.search.kibana_metadata import DASHBOARD_DATASETS

    targets = datasets or list(DASHBOARD_DATASETS.values())
    views: list[DataViewObject] = []

    for meta in targets:
        views.append(
            DataViewObject(
                id=meta.resolve_data_view_id(config),
                title=meta.data_view_title,
                index_pattern=meta.resolve_data_view_pattern(config),
                time_field=meta.time_field,
            )
        )

    return views


def build_saved_searches(
    config: SearchConfig,
    datasets: list[DashboardDatasetMeta] | None = None,
) -> list[SavedSearchObject]:
    """Build saved search objects for Discover.

    Creates one saved search per dataset with appropriate column
    selection and default sort for analyst inspection.
    """
    from skill_radar.domains.search.kibana_metadata import DASHBOARD_DATASETS

    targets = datasets or list(DASHBOARD_DATASETS.values())
    searches: list[SavedSearchObject] = []

    for meta in targets:
        data_view_id = meta.resolve_data_view_id(config)
        columns = [f.name for f in meta.fields if f.role in ("dimension", "metric", "time")]

        sort_field = meta.default_sort_field or "ingestion_date"
        sort_order = meta.default_sort_order or "desc"

        searches.append(
            SavedSearchObject(
                id=_ss_id(meta.index_suffix),
                title=f"Skill Radar \u2014 {meta.label} / Records",
                description=f"All records for {meta.description}",
                data_view_id=data_view_id,
                columns=columns,
                sort=[[sort_field, sort_order]],
            )
        )

    return searches


def build_dashboard_suite(
    config: SearchConfig,
    dataset_key: str,
) -> tuple[list[LensVisualizationObject], DashboardObject]:
    """Build a complete dashboard suite for a dataset.

    Returns a tuple of (visualizations, dashboard) where the dashboard
    references the visualizations by ID via panels.

    Parameters
    ----------
    config:
        Search configuration for index prefix resolution.
    dataset_key:
        Dataset key from the metadata registry.

    Raises
    ------
    ValueError
        If no builder exists for the dataset or field validation fails.
    """
    if dataset_key not in _DASHBOARD_BUILDERS:
        raise ValueError(
            f"No dashboard builder for dataset '{dataset_key}'. "
            f"Available: {list(_DASHBOARD_BUILDERS.keys())}"
        )

    meta = get_dashboard_meta(dataset_key)

    # Validate field bindings before building
    errors = meta.validate_against_mapping()
    if errors:
        raise ValueError(f"Metadata validation failed for '{dataset_key}': {errors}")

    _slug, builder_fn = _DASHBOARD_BUILDERS[dataset_key]
    data_view_id = meta.resolve_data_view_id(config)

    vises, dashboard = builder_fn(meta, data_view_id)
    return vises, dashboard


def build_all_dashboards(
    config: SearchConfig,
) -> tuple[
    list[DataViewObject],
    list[LensVisualizationObject],
    list[DashboardObject],
    list[SavedSearchObject],
]:
    """Build the complete Kibana asset set for all primary datasets.

    Returns
    -------
    tuple
        (data_views, visualizations, dashboards, saved_searches)
    """
    from skill_radar.domains.search.kibana_metadata import PRIMARY_DASHBOARD_DATASETS

    data_views = build_data_views(config)
    saved_searches = build_saved_searches(config)

    all_visualizations: list[LensVisualizationObject] = []
    all_dashboards: list[DashboardObject] = []

    for ds_key in PRIMARY_DASHBOARD_DATASETS:
        vises, dashboard = build_dashboard_suite(config, ds_key)
        all_visualizations.extend(vises)
        all_dashboards.append(dashboard)

    logger.info(
        "Built %d data views, %d visualizations, %d dashboards, %d saved searches",
        len(data_views),
        len(all_visualizations),
        len(all_dashboards),
        len(saved_searches),
    )

    return data_views, all_visualizations, all_dashboards, saved_searches
