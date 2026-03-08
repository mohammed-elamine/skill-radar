"""Declarative Elasticsearch mappings and settings for served datasets.

Each mapping is a plain dict matching the Elasticsearch ``PUT /<index>``
request body structure.  Mappings are version-controlled here — never
rely on dynamic mapping for critical fields.

The ``get_index_settings`` helper merges per-dataset mappings with
cluster-level settings from the search config (shards, replicas).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig

# ═══════════════════════════════════════════════════════════════════════════
# Shared field fragments
# ═══════════════════════════════════════════════════════════════════════════

_KEYWORD = {"type": "keyword"}
_DATE = {"type": "date", "format": "yyyy-MM-dd"}
# Timestamps: accept date-only, ISO datetime, or epoch_millis
_TIMESTAMP = {
    "type": "date",
    "format": "yyyy-MM-dd||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssXXX||strict_date_optional_time||epoch_millis",
}
_LONG = {"type": "long"}
_DOUBLE = {"type": "double"}
_FLOAT = {"type": "float"}
_TEXT_KEYWORD = {
    "type": "text",
    "fields": {"raw": {"type": "keyword"}},
}

# ═══════════════════════════════════════════════════════════════════════════
# Per-dataset mappings
# ═══════════════════════════════════════════════════════════════════════════

SKILL_DEMAND_DAILY_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "esco_skill_concept_uri": _KEYWORD,
        "esco_skill_preferred_label": _TEXT_KEYWORD,
        "jobs_count": _LONG,
        "unique_companies_count": _LONG,
        "unique_locations_count": _LONG,
        "title_match_jobs_count": _LONG,
        "description_match_jobs_count": _LONG,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}

SALARY_BY_SKILL_DAILY_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "esco_skill_concept_uri": _KEYWORD,
        "esco_skill_preferred_label": _TEXT_KEYWORD,
        "salary_jobs_count": _LONG,
        "avg_salary_mean": _DOUBLE,
        "min_salary_min": _DOUBLE,
        "max_salary_max": _DOUBLE,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}

OCCUPATION_SKILL_GRAPH_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "esco_occupation_concept_uri": _KEYWORD,
        "esco_occupation_preferred_label": _TEXT_KEYWORD,
        "esco_skill_concept_uri": _KEYWORD,
        "esco_skill_preferred_label": _TEXT_KEYWORD,
        "relation_type": _KEYWORD,
        "matched_jobs_count": _LONG,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}

JOB_SKILL_MATCHES_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "job_id": _KEYWORD,
        "esco_skill_concept_uri": _KEYWORD,
        "esco_skill_preferred_label": _TEXT_KEYWORD,
        "matched_label": _TEXT_KEYWORD,
        "matched_label_type": _KEYWORD,
        "match_method": _KEYWORD,
        "title_hit": {"type": "boolean"},
        "description_hit": {"type": "boolean"},
        "match_score": _FLOAT,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}

JOB_OCCUPATION_MATCHES_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "job_id": _KEYWORD,
        "esco_occupation_concept_uri": _KEYWORD,
        "esco_occupation_preferred_label": _TEXT_KEYWORD,
        "match_method": _KEYWORD,
        "match_score": _FLOAT,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}

SKILL_EMERGING_DAILY_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "esco_skill_concept_uri": _KEYWORD,
        "esco_skill_preferred_label": _TEXT_KEYWORD,
        "jobs_count": _LONG,
        "momentum_score": _DOUBLE,
        "acceleration_score": _DOUBLE,
        "novelty_score": _DOUBLE,
        "emerging_composite_score": _DOUBLE,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}

OCCUPATION_MARKET_DAILY_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "esco_occupation_concept_uri": _KEYWORD,
        "esco_occupation_preferred_label": _TEXT_KEYWORD,
        "total_jobs_count": _LONG,
        "unique_skills_count": _LONG,
        "avg_match_score": _DOUBLE,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}

SKILL_DEMAND_SEGMENTS_DAILY_MAPPING: dict = {
    "properties": {
        "doc_id": _KEYWORD,
        "ingestion_date": _DATE,
        "country": _KEYWORD,
        "esco_skill_concept_uri": _KEYWORD,
        "esco_skill_preferred_label": _TEXT_KEYWORD,
        "segment_id": _LONG,
        "segment_label": _KEYWORD,
        "jobs_count": _LONG,
        "unique_companies_count": _LONG,
        "unique_locations_count": _LONG,
        "gold_run_id": _KEYWORD,
        "gold_generated_at_utc": _TIMESTAMP,
        "esco_version": _KEYWORD,
        "esco_lang": _KEYWORD,
    }
}


# ═══════════════════════════════════════════════════════════════════════════
# Registry: dataset suffix → mapping
# ═══════════════════════════════════════════════════════════════════════════

MAPPINGS: dict[str, dict] = {
    "skill-demand-daily": SKILL_DEMAND_DAILY_MAPPING,
    "salary-by-skill-daily": SALARY_BY_SKILL_DAILY_MAPPING,
    "occupation-skill-graph": OCCUPATION_SKILL_GRAPH_MAPPING,
    "job-skill-matches": JOB_SKILL_MATCHES_MAPPING,
    "job-occupation-matches": JOB_OCCUPATION_MATCHES_MAPPING,
    "skill-emerging-daily": SKILL_EMERGING_DAILY_MAPPING,
    "occupation-market-daily": OCCUPATION_MARKET_DAILY_MAPPING,
    "skill-demand-segments-daily": SKILL_DEMAND_SEGMENTS_DAILY_MAPPING,
}


def get_mapping(dataset_suffix: str) -> dict:
    """Return the mapping dict for a dataset.

    Raises
    ------
    KeyError
        If the dataset suffix is not registered.
    """
    if dataset_suffix not in MAPPINGS:
        raise KeyError(
            f"No mapping registered for dataset '{dataset_suffix}'. "
            f"Known datasets: {sorted(MAPPINGS)}"
        )
    return MAPPINGS[dataset_suffix]


def get_index_body(dataset_suffix: str, config: SearchConfig) -> dict:
    """Return the full index creation body (settings + mappings).

    Parameters
    ----------
    dataset_suffix:
        Logical index suffix, e.g. ``skill-demand-daily``.
    config:
        Search configuration with shards/replicas settings.
    """
    return {
        "settings": {
            "number_of_shards": config.index_shards,
            "number_of_replicas": config.index_replicas,
        },
        "mappings": get_mapping(dataset_suffix),
    }
