"""Dashboard dataset metadata — single source of truth for Kibana asset generation.

Extends the served dataset registry with Kibana-specific metadata:

- Data view configuration (index pattern, time field)
- Field roles (dimensions, metrics, filters, identifiers, metadata)
- Dashboard eligibility and default visualization parameters
- Human-readable labels and descriptions

This is the **only** place where dashboard field bindings are defined.
Adding a new dashboard or changing field assignments requires editing
this file — not the builders or CLI.

Design
------
Each :class:`DashboardDatasetMeta` maps one-to-one with a served dataset
from :mod:`skill_radar.domains.search.datasets` and enriches it with:

- ``fields`` — typed field metadata including aggregation-friendly names
  (e.g. ``.raw`` sub-fields for text+keyword mappings)
- ``validate_against_mapping()`` — fails early if fields are missing
- ``resolve_data_view_*`` — deterministic Kibana data view naming

The registry :data:`DASHBOARD_DATASETS` is consumed by the builder layer
in :mod:`skill_radar.platform.search.kibana_builders`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from skill_radar.platform.search.mappings import MAPPINGS

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig


# ═══════════════════════════════════════════════════════════════════════════
# Field metadata
# ═══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class FieldMeta:
    """Metadata for a single field in a served dataset.

    Attributes
    ----------
    name:
        Elasticsearch field name (e.g. ``esco_skill_preferred_label``).
    label:
        Human-readable display label for dashboards.
    role:
        Field role: ``dimension``, ``metric``, ``time``, ``identifier``, ``meta``.
    agg_field:
        Aggregation-friendly field name.  For ``text`` fields with a
        ``.raw`` keyword sub-field, set this to ``field.raw`` so that
        Lens terms aggregations use the keyword variant.
    """

    name: str
    label: str
    role: str  # "dimension" | "metric" | "time" | "identifier" | "meta"
    agg_field: str = ""

    @property
    def aggregation_field(self) -> str:
        """Return the field name suitable for aggregations."""
        return self.agg_field or self.name


# ═══════════════════════════════════════════════════════════════════════════
# Dataset metadata
# ═══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class DashboardDatasetMeta:
    """Kibana-specific metadata for a served dataset.

    Single source of truth for:

    - logical dataset key
    - Elasticsearch alias / data view pattern
    - Kibana data view name
    - time field
    - key business dimensions and metrics
    - dashboard eligibility
    - human-readable labels / descriptions

    Attributes
    ----------
    key:
        Logical dataset key matching :class:`ServedDataset.name`.
    index_suffix:
        Elasticsearch index suffix (e.g. ``skill-demand-daily``).
    data_view_title:
        Human-readable Kibana data view title.
    time_field:
        Default time field for time-based filtering.
    label:
        Short human-readable label for UI display.
    description:
        Business-level description of the dataset.
    fields:
        Ordered tuple of field metadata definitions.
    dashboard_eligible:
        Whether this dataset should have dashboards generated.
    default_sort_field:
        Default sort field for saved searches / tables.
    default_sort_order:
        Default sort order (``asc`` or ``desc``).
    """

    key: str
    index_suffix: str
    data_view_title: str
    time_field: str
    label: str
    description: str
    fields: tuple[FieldMeta, ...] = ()
    dashboard_eligible: bool = True
    default_sort_field: str = ""
    default_sort_order: str = "desc"

    def get_field(self, name: str) -> FieldMeta:
        """Look up a field by name.

        Raises
        ------
        KeyError
            If the field is not defined in this dataset's metadata.
        """
        for f in self.fields:
            if f.name == name:
                return f
        available = [f.name for f in self.fields]
        raise KeyError(
            f"Field '{name}' not found in metadata for '{self.key}'. Available: {available}"
        )

    def dimension_fields(self) -> list[FieldMeta]:
        """Return fields with role ``dimension``."""
        return [f for f in self.fields if f.role == "dimension"]

    def metric_fields(self) -> list[FieldMeta]:
        """Return fields with role ``metric``."""
        return [f for f in self.fields if f.role == "metric"]

    def required_field_names(self) -> list[str]:
        """Return field names required for dashboard generation."""
        return [f.name for f in self.fields if f.role in ("dimension", "metric", "time")]

    def validate_against_mapping(self) -> list[str]:
        """Validate that all referenced fields exist in the ES mapping.

        Returns
        -------
        list[str]
            List of error messages.  Empty if validation passes.
        """
        mapping = MAPPINGS.get(self.index_suffix)
        if mapping is None:
            return [f"No mapping found for index suffix '{self.index_suffix}'"]
        mapping_fields = set(mapping.get("properties", {}).keys())
        errors: list[str] = []
        for fm in self.fields:
            # Handle .raw sub-fields: check the base field name
            base_field = fm.name.split(".")[0]
            if base_field not in mapping_fields:
                errors.append(f"Field '{fm.name}' not in mapping for '{self.index_suffix}'")
        return errors

    def resolve_data_view_pattern(self, config: SearchConfig) -> str:
        """Return the Elasticsearch index pattern for the data view."""
        return f"{config.index_prefix}-{self.index_suffix}-*"

    def resolve_data_view_id(self, config: SearchConfig) -> str:
        """Return the deterministic Kibana saved object ID for the data view."""
        return f"{config.index_prefix}-dv-{self.index_suffix}"


# ═══════════════════════════════════════════════════════════════════════════
# Dataset metadata registry
# ═══════════════════════════════════════════════════════════════════════════

SKILL_DEMAND_DAILY_META = DashboardDatasetMeta(
    key="skill_demand_daily",
    index_suffix="skill-demand-daily",
    data_view_title="Skill Radar — Skill Demand Daily",
    time_field="ingestion_date",
    label="Skill Demand",
    description="Daily skill demand aggregates from job postings",
    default_sort_field="jobs_count",
    fields=(
        FieldMeta("ingestion_date", "Ingestion Date", "time"),
        FieldMeta("country", "Country", "dimension"),
        FieldMeta("esco_skill_concept_uri", "Skill URI", "identifier"),
        FieldMeta(
            "esco_skill_preferred_label",
            "Skill",
            "dimension",
            agg_field="esco_skill_preferred_label.raw",
        ),
        FieldMeta("jobs_count", "Jobs Count", "metric"),
        FieldMeta("unique_companies_count", "Unique Companies", "metric"),
        FieldMeta("unique_locations_count", "Unique Locations", "metric"),
        FieldMeta("title_match_jobs_count", "Title Match Jobs", "metric"),
        FieldMeta("description_match_jobs_count", "Description Match Jobs", "metric"),
        FieldMeta("gold_run_id", "Gold Run ID", "meta"),
        FieldMeta("gold_generated_at_utc", "Gold Generated At", "meta"),
        FieldMeta("esco_version", "ESCO Version", "meta"),
        FieldMeta("esco_lang", "ESCO Language", "meta"),
    ),
)

SALARY_BY_SKILL_DAILY_META = DashboardDatasetMeta(
    key="salary_by_skill_daily",
    index_suffix="salary-by-skill-daily",
    data_view_title="Skill Radar — Salary by Skill Daily",
    time_field="ingestion_date",
    label="Salary by Skill",
    description="Salary statistics per skill from job postings",
    default_sort_field="avg_salary_mean",
    fields=(
        FieldMeta("ingestion_date", "Ingestion Date", "time"),
        FieldMeta("country", "Country", "dimension"),
        FieldMeta("esco_skill_concept_uri", "Skill URI", "identifier"),
        FieldMeta(
            "esco_skill_preferred_label",
            "Skill",
            "dimension",
            agg_field="esco_skill_preferred_label.raw",
        ),
        FieldMeta("salary_jobs_count", "Salary Jobs Count", "metric"),
        FieldMeta("avg_salary_mean", "Avg Salary", "metric"),
        FieldMeta("min_salary_min", "Min Salary", "metric"),
        FieldMeta("max_salary_max", "Max Salary", "metric"),
        FieldMeta("gold_run_id", "Gold Run ID", "meta"),
        FieldMeta("gold_generated_at_utc", "Gold Generated At", "meta"),
        FieldMeta("esco_version", "ESCO Version", "meta"),
        FieldMeta("esco_lang", "ESCO Language", "meta"),
    ),
)

OCCUPATION_SKILL_GRAPH_META = DashboardDatasetMeta(
    key="occupation_skill_graph",
    index_suffix="occupation-skill-graph",
    data_view_title="Skill Radar — Occupation-Skill Graph",
    time_field="ingestion_date",
    label="Occupation-Skill Graph",
    description="Occupation-skill relationships with evidence metrics",
    default_sort_field="matched_jobs_count",
    fields=(
        FieldMeta("ingestion_date", "Ingestion Date", "time"),
        FieldMeta("country", "Country", "dimension"),
        FieldMeta("esco_occupation_concept_uri", "Occupation URI", "identifier"),
        FieldMeta(
            "esco_occupation_preferred_label",
            "Occupation",
            "dimension",
            agg_field="esco_occupation_preferred_label.raw",
        ),
        FieldMeta("esco_skill_concept_uri", "Skill URI", "identifier"),
        FieldMeta(
            "esco_skill_preferred_label",
            "Skill",
            "dimension",
            agg_field="esco_skill_preferred_label.raw",
        ),
        FieldMeta("relation_type", "Relation Type", "dimension"),
        FieldMeta("matched_jobs_count", "Matched Jobs", "metric"),
        FieldMeta("gold_run_id", "Gold Run ID", "meta"),
        FieldMeta("gold_generated_at_utc", "Gold Generated At", "meta"),
        FieldMeta("esco_version", "ESCO Version", "meta"),
        FieldMeta("esco_lang", "ESCO Language", "meta"),
    ),
)

SKILL_EMERGING_DAILY_META = DashboardDatasetMeta(
    key="skill_emerging_daily",
    index_suffix="skill-emerging-daily",
    data_view_title="Skill Radar \u2014 Emerging Skills Daily",
    time_field="ingestion_date",
    label="Emerging Skills",
    description="Emerging-skill signals with momentum, acceleration, and novelty scores",
    default_sort_field="emerging_composite_score",
    fields=(
        FieldMeta("ingestion_date", "Ingestion Date", "time"),
        FieldMeta("country", "Country", "dimension"),
        FieldMeta("esco_skill_concept_uri", "Skill URI", "identifier"),
        FieldMeta(
            "esco_skill_preferred_label",
            "Skill",
            "dimension",
            agg_field="esco_skill_preferred_label.raw",
        ),
        FieldMeta("jobs_count", "Jobs Count", "metric"),
        FieldMeta("momentum_score", "Momentum", "metric"),
        FieldMeta("acceleration_score", "Acceleration", "metric"),
        FieldMeta("novelty_score", "Novelty", "metric"),
        FieldMeta("emerging_composite_score", "Emerging Score", "metric"),
        FieldMeta("gold_run_id", "Gold Run ID", "meta"),
        FieldMeta("gold_generated_at_utc", "Gold Generated At", "meta"),
        FieldMeta("esco_version", "ESCO Version", "meta"),
        FieldMeta("esco_lang", "ESCO Language", "meta"),
    ),
)

OCCUPATION_MARKET_DAILY_META = DashboardDatasetMeta(
    key="occupation_market_daily",
    index_suffix="occupation-market-daily",
    data_view_title="Skill Radar \u2014 Occupation Market Daily",
    time_field="ingestion_date",
    label="Occupation Market",
    description="Occupation-level market analytics with demand and skill breadth",
    default_sort_field="total_jobs_count",
    fields=(
        FieldMeta("ingestion_date", "Ingestion Date", "time"),
        FieldMeta("country", "Country", "dimension"),
        FieldMeta("esco_occupation_concept_uri", "Occupation URI", "identifier"),
        FieldMeta(
            "esco_occupation_preferred_label",
            "Occupation",
            "dimension",
            agg_field="esco_occupation_preferred_label.raw",
        ),
        FieldMeta("total_jobs_count", "Total Jobs", "metric"),
        FieldMeta("unique_skills_count", "Unique Skills", "metric"),
        FieldMeta("avg_match_score", "Avg Match Score", "metric"),
        FieldMeta("gold_run_id", "Gold Run ID", "meta"),
        FieldMeta("gold_generated_at_utc", "Gold Generated At", "meta"),
        FieldMeta("esco_version", "ESCO Version", "meta"),
        FieldMeta("esco_lang", "ESCO Language", "meta"),
    ),
)

SKILL_DEMAND_SEGMENTS_DAILY_META = DashboardDatasetMeta(
    key="skill_demand_segments_daily",
    index_suffix="skill-demand-segments-daily",
    data_view_title="Skill Radar \u2014 Skill Demand Segments Daily",
    time_field="ingestion_date",
    label="Skill Segments",
    description="ML-derived skill demand segments (KMeans clustering)",
    default_sort_field="jobs_count",
    fields=(
        FieldMeta("ingestion_date", "Ingestion Date", "time"),
        FieldMeta("country", "Country", "dimension"),
        FieldMeta("esco_skill_concept_uri", "Skill URI", "identifier"),
        FieldMeta(
            "esco_skill_preferred_label",
            "Skill",
            "dimension",
            agg_field="esco_skill_preferred_label.raw",
        ),
        FieldMeta("segment_id", "Segment ID", "dimension"),
        FieldMeta("segment_label", "Segment", "dimension"),
        FieldMeta("jobs_count", "Jobs Count", "metric"),
        FieldMeta("unique_companies_count", "Unique Companies", "metric"),
        FieldMeta("unique_locations_count", "Unique Locations", "metric"),
        FieldMeta("gold_run_id", "Gold Run ID", "meta"),
        FieldMeta("gold_generated_at_utc", "Gold Generated At", "meta"),
        FieldMeta("esco_version", "ESCO Version", "meta"),
        FieldMeta("esco_lang", "ESCO Language", "meta"),
    ),
)


# ── Registries ────────────────────────────────────────────────────────────

DASHBOARD_DATASETS: dict[str, DashboardDatasetMeta] = {
    "skill_demand_daily": SKILL_DEMAND_DAILY_META,
    "salary_by_skill_daily": SALARY_BY_SKILL_DAILY_META,
    "occupation_skill_graph": OCCUPATION_SKILL_GRAPH_META,
    "skill_emerging_daily": SKILL_EMERGING_DAILY_META,
    "occupation_market_daily": OCCUPATION_MARKET_DAILY_META,
    "skill_demand_segments_daily": SKILL_DEMAND_SEGMENTS_DAILY_META,
}

PRIMARY_DASHBOARD_DATASETS: list[str] = [
    "skill_demand_daily",
    "salary_by_skill_daily",
    "occupation_skill_graph",
    "skill_emerging_daily",
]


def get_dashboard_meta(key: str) -> DashboardDatasetMeta:
    """Look up dashboard metadata by dataset key.

    Raises
    ------
    KeyError
        If the key is not in the registry.
    """
    if key not in DASHBOARD_DATASETS:
        raise KeyError(
            f"Unknown dashboard dataset: '{key}'. Available: {list(DASHBOARD_DATASETS.keys())}"
        )
    return DASHBOARD_DATASETS[key]


def validate_all_metadata() -> list[str]:
    """Validate all metadata entries against their ES mappings.

    Returns
    -------
    list[str]
        Aggregated error messages.  Empty if all validations pass.
    """
    errors: list[str] = []
    for meta in DASHBOARD_DATASETS.values():
        errors.extend(meta.validate_against_mapping())
    return errors
