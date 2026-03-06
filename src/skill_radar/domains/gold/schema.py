"""Gold schema — single source of truth for Gold table naming conventions.

**Why this module exists**

Every Gold pipeline producer (matching, analytics) and every consumer
(validation checks, downstream dashboards) must agree on column names.
Scattering string literals across files leads to silent drift —
a rename in one place silently breaks validation or analytics.

This module defines **naming conventions** (callable namespaces) and
the **table contracts** (required columns, composite keys, count columns)
consumed by both data producers and data consumers.

    ┌───────────────────┐
    │   gold.schema     │  ← naming conventions + table contracts
    │ (single source)   │
    └─────────┬─────────┘
              │ imported by
    ┌─────────┴───────────────────────────┐
    │                                     │
    ▼                                     ▼
  producers                          consumers
  (matching, analytics)        (validation, dashboards)

Naming conventions
------------------
Columns that represent an ESCO entity projected into Gold are prefixed
with the entity namespace to avoid join ambiguity and provide provenance.
Call the namespace with the field name to get the column name:

    >>> esco_skill("concept_uri")          # → "esco_skill_concept_uri"
    >>> esco_occupation("preferred_label")  # → "esco_occupation_preferred_label"
    >>> matched("label")                   # → "matched_label"
    >>> gold_meta("run_id")                # → "gold_run_id"

Adding a new column only requires calling the convention at the usage
site — no new constant needed. Changing a prefix propagates everywhere.

Columns that carry no prefix convention (``job_id``, ``country``,
``match_method``, ``match_score``, …) are used as plain string literals.
"""

from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════════════
# Namespace helper
# ═══════════════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════════════
# Naming conventions
# ═══════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════
# Lineage / partition columns (shared across all Gold tables)
# ═══════════════════════════════════════════════════════════════════════════

GOLD_LINEAGE_COLS: list[str] = [
    gold_meta("run_id"),  # gold_run_id
    gold_meta("generated_at_utc"),  # gold_generated_at_utc
    "esco_version",
    "esco_lang",
]


# ═══════════════════════════════════════════════════════════════════════════
# Required columns per Gold table  (validation + documentation contracts)
# ═══════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════
# Composite key definitions  (deduplication + validation uniqueness checks)
# ═══════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════
# Count-column sets  (non-negativity validation)
# ═══════════════════════════════════════════════════════════════════════════

DEMAND_COUNT_COLS: list[str] = [
    "jobs_count",
    "unique_companies_count",
    "unique_locations_count",
    "title_match_jobs_count",
    "description_match_jobs_count",
]

SALARY_COUNT_COLS: list[str] = ["salary_jobs_count"]
