"""Gold schema — single source of truth for Gold table naming conventions.

Defines naming conventions (callable namespaces) and table contracts
(required columns, composite keys) consumed by both data producers
and consumers.  Column prefixes prevent join ambiguity and provide
provenance::

    esco_skill("concept_uri")          # → "esco_skill_concept_uri"
    esco_occupation("preferred_label")  # → "esco_occupation_preferred_label"
"""

from __future__ import annotations

# Namespace helper


class _Namespace:
    """A callable column-naming convention with a fixed prefix.

    Examples
    --------
    >>> ns = _Namespace("esco_skill")
    >>> ns("concept_uri")
    'esco_skill_concept_uri'
    >>> ns.prefix
    'esco_skill'
    """

    __slots__ = ("_doc", "_prefix")

    def __init__(self, prefix: str, *, doc: str = "") -> None:
        self._prefix = prefix
        self._doc = doc

    def __call__(self, field: str) -> str:
        """Return ``{prefix}_{field}``."""
        return f"{self._prefix}_{field}"

    def __repr__(self) -> str:
        return f"_Namespace({self._prefix!r})"

    @property
    def prefix(self) -> str:
        """The raw prefix string."""
        return self._prefix


# Naming conventions

esco_skill = _Namespace(
    "esco_skill",
    doc="ESCO skill entity columns projected into Gold tables.",
)

esco_occupation = _Namespace(
    "esco_occupation",
    doc="ESCO occupation entity columns projected into Gold tables.",
)

matched = _Namespace(
    "matched",
    doc="Match-result fields produced by the matching stage.",
)

gold_meta = _Namespace(
    "gold",
    doc="Gold pipeline lineage / metadata columns.",
)


# Lineage / partition columns (shared across all Gold tables)

GOLD_LINEAGE_COLS: list[str] = [
    gold_meta("run_id"),  # gold_run_id
    gold_meta("generated_at_utc"),  # gold_generated_at_utc
    "esco_version",
    "esco_lang",
]


# Required columns per Gold table  (validation + documentation contracts)

SKILL_MATCHES_REQUIRED: list[str] = [
    "job_id",
    "country",
    "ingestion_date",
    esco_skill("concept_uri"),
    esco_skill("preferred_label"),
    matched("label"),
    matched("label_type"),
    "match_method",
    "title_hit",
    "description_hit",
    "match_score",
    *GOLD_LINEAGE_COLS,
]

OCCUPATION_MATCHES_REQUIRED: list[str] = [
    "job_id",
    "country",
    "ingestion_date",
    esco_occupation("concept_uri"),
    esco_occupation("preferred_label"),
    "match_method",
    "match_score",
    *GOLD_LINEAGE_COLS,
]

SKILL_DEMAND_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_skill("concept_uri"),
    esco_skill("preferred_label"),
    "jobs_count",
    "unique_companies_count",
    "unique_locations_count",
    "title_match_jobs_count",
    "description_match_jobs_count",
    *GOLD_LINEAGE_COLS,
]

SALARY_BY_SKILL_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_skill("concept_uri"),
    esco_skill("preferred_label"),
    "salary_jobs_count",
    "avg_salary_mean",
    "min_salary_min",
    "max_salary_max",
    *GOLD_LINEAGE_COLS,
]

OCCUPATION_SKILL_GRAPH_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_occupation("concept_uri"),
    esco_occupation("preferred_label"),
    esco_skill("concept_uri"),
    esco_skill("preferred_label"),
    matched("jobs_count"),
    *GOLD_LINEAGE_COLS,
]

SKILL_EMERGING_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_skill("concept_uri"),
    esco_skill("preferred_label"),
    "jobs_count",
    "momentum_score",
    "acceleration_score",
    "novelty_score",
    "emerging_composite_score",
    *GOLD_LINEAGE_COLS,
]

OCCUPATION_MARKET_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_occupation("concept_uri"),
    esco_occupation("preferred_label"),
    "total_jobs_count",
    "unique_skills_count",
    "avg_match_score",
    *GOLD_LINEAGE_COLS,
]

SKILL_DEMAND_SEGMENTS_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_skill("concept_uri"),
    esco_skill("preferred_label"),
    "segment_id",
    "segment_label",
    "jobs_count",
    *GOLD_LINEAGE_COLS,
]


# Composite key definitions  (deduplication + validation uniqueness checks)

SKILL_MATCHES_KEY: list[str] = [
    "job_id",
    esco_skill("concept_uri"),
    "country",
    "ingestion_date",
]

OCCUPATION_MATCHES_KEY: list[str] = [
    "job_id",
    esco_occupation("concept_uri"),
    "match_method",
    "country",
    "ingestion_date",
]

SKILL_DEMAND_KEY: list[str] = [
    esco_skill("concept_uri"),
    "country",
    "ingestion_date",
]

SALARY_BY_SKILL_KEY: list[str] = [
    esco_skill("concept_uri"),
    "country",
    "ingestion_date",
]

OCCUPATION_SKILL_GRAPH_KEY: list[str] = [
    esco_occupation("concept_uri"),
    esco_skill("concept_uri"),
    "country",
    "ingestion_date",
]

SKILL_EMERGING_KEY: list[str] = [
    esco_skill("concept_uri"),
    "country",
    "ingestion_date",
]

OCCUPATION_MARKET_KEY: list[str] = [
    esco_occupation("concept_uri"),
    "country",
    "ingestion_date",
]

SKILL_DEMAND_SEGMENTS_KEY: list[str] = [
    esco_skill("concept_uri"),
    "country",
    "ingestion_date",
]


# Count-column sets  (non-negativity validation)

DEMAND_COUNT_COLS: list[str] = [
    "jobs_count",
    "unique_companies_count",
    "unique_locations_count",
    "title_match_jobs_count",
    "description_match_jobs_count",
]

SALARY_COUNT_COLS: list[str] = ["salary_jobs_count"]

EMERGING_COUNT_COLS: list[str] = ["jobs_count"]

OCCUPATION_MARKET_COUNT_COLS: list[str] = [
    "total_jobs_count",
    "unique_skills_count",
]

SEGMENTS_COUNT_COLS: list[str] = ["jobs_count"]

EMERGING_SCORE_COLS: list[str] = [
    "momentum_score",
    "acceleration_score",
    "novelty_score",
    "emerging_composite_score",
]


# Career Navigation table contracts

OCCUPATION_PROFILE_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_occupation("concept_uri"),
    esco_occupation("concept_uri_uuid"),
    esco_occupation("preferred_label"),
    "occupation_search_text",
    "matched_jobs_count",
    "distinct_companies_count",
    "distinct_locations_count",
    "avg_salary_mean",
    "top_essential_skills_json",
    "top_optional_skills_json",
    "top_companies_json",
    "related_skills_count",
    *GOLD_LINEAGE_COLS,
]

SKILL_PROFILE_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    esco_skill("concept_uri"),
    esco_skill("concept_uri_uuid"),
    esco_skill("preferred_label"),
    "skill_type",
    "skill_search_text",
    "jobs_count",
    "companies_count",
    "locations_count",
    "avg_salary_mean",
    "top_occupations_json",
    "top_companies_json",
    *GOLD_LINEAGE_COLS,
]

OCCUPATION_SIMILARITY_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    "source_occupation_uri",
    "source_occupation_label",
    "target_occupation_uri",
    "target_occupation_label",
    "similarity_score",
    "shared_skill_count",
    "shared_essential_skill_count",
    "shared_optional_skill_count",
    "source_skill_count",
    "target_skill_count",
    *GOLD_LINEAGE_COLS,
]

OCCUPATION_TRANSITION_DAILY_REQUIRED: list[str] = [
    "ingestion_date",
    "country",
    "from_occupation_uri",
    "from_occupation_label",
    "to_occupation_uri",
    "to_occupation_label",
    "similarity_score",
    "shared_skills_json",
    "missing_skills_json",
    "missing_essential_skills_json",
    "missing_optional_skills_json",
    "missing_skill_count",
    "missing_essential_skill_count",
    "transition_difficulty_score",
    "from_avg_salary_mean",
    "to_avg_salary_mean",
    "salary_delta_mean",
    "from_jobs_count",
    "to_jobs_count",
    "jobs_delta",
    *GOLD_LINEAGE_COLS,
]


# ── Composite keys ────────────────────────────────────────────────────

OCCUPATION_PROFILE_KEY: list[str] = [
    esco_occupation("concept_uri"),
    "country",
    "ingestion_date",
]

SKILL_PROFILE_KEY: list[str] = [
    esco_skill("concept_uri"),
    "country",
    "ingestion_date",
]

OCCUPATION_SIMILARITY_KEY: list[str] = [
    "source_occupation_uri",
    "target_occupation_uri",
    "country",
    "ingestion_date",
]

OCCUPATION_TRANSITION_KEY: list[str] = [
    "from_occupation_uri",
    "to_occupation_uri",
    "country",
    "ingestion_date",
]


# ── Count columns ─────────────────────────────────────────────────────

OCCUPATION_PROFILE_COUNT_COLS: list[str] = [
    "matched_jobs_count",
    "distinct_companies_count",
    "distinct_locations_count",
    "related_skills_count",
]

SKILL_PROFILE_COUNT_COLS: list[str] = [
    "jobs_count",
    "companies_count",
    "locations_count",
]

SIMILARITY_COUNT_COLS: list[str] = [
    "shared_skill_count",
    "shared_essential_skill_count",
    "shared_optional_skill_count",
    "source_skill_count",
    "target_skill_count",
]

TRANSITION_COUNT_COLS: list[str] = [
    "missing_skill_count",
    "missing_essential_skill_count",
]
