"""Document builders — convert Spark rows to Elasticsearch documents.

Each builder is a **pure function** that transforms a Spark Row into a
flat dictionary ready for bulk indexing.  Document ``doc_id`` values are
deterministic so reruns overwrite logically identical documents.

Deterministic ID construction
-----------------------------
- skill demand:       ``{country}|{ingestion_date}|{skill_uri}``
- salary by skill:    ``{country}|{ingestion_date}|{skill_uri}``
- occupation graph:   ``{country}|{ingestion_date}|{occ_uri}|{skill_uri}``
- job-skill matches:  ``{country}|{ingestion_date}|{job_id}|{skill_uri}``
- job-occ matches:    ``{country}|{ingestion_date}|{job_id}|{occ_uri}|{method}``

All ID values are hashed with SHA-256 and truncated to 20 hex chars.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any


def _deterministic_id(*parts: str) -> str:
    """Build a deterministic document ID from key parts.

    Uses SHA-256 truncated to 20 hex characters (80 bits — collision-safe
    for the expected cardinality of served datasets).
    """
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _safe_str(value: Any) -> str:
    """Convert a value to string, handling None."""
    return str(value) if value is not None else ""


def _safe_datetime(value: Any) -> str:
    """Convert datetime/date to ISO 8601 string for Elasticsearch.

    Handles:
    - datetime objects → ISO 8601 with time
    - date objects → ISO 8601 date-only (yyyy-MM-dd)
    - strings → pass through
    - None → empty string
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _safe_float(value: Any) -> float | None:
    """Convert to float, returning None for missing values."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> int | None:
    """Convert to int, returning None for missing values."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Skill Demand Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_skill_demand_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build Elasticsearch documents from Gold skill_demand_daily rows.

    Parameters
    ----------
    rows:
        List of Spark Row objects (or dicts) from gold_skill_demand_daily.

    Returns
    -------
    list[dict]
        Documents with deterministic ``doc_id``.
    """
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date = _safe_str(r.get("ingestion_date"))
        skill_uri = _safe_str(r.get("esco_skill_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date, skill_uri),
            "ingestion_date": date,
            "country": country,
            "esco_skill_concept_uri": skill_uri,
            "esco_skill_preferred_label": _safe_str(r.get("esco_skill_preferred_label")),
            "jobs_count": _safe_int(r.get("jobs_count")),
            "unique_companies_count": _safe_int(r.get("unique_companies_count")),
            "unique_locations_count": _safe_int(r.get("unique_locations_count")),
            "title_match_jobs_count": _safe_int(r.get("title_match_jobs_count")),
            "description_match_jobs_count": _safe_int(r.get("description_match_jobs_count")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Salary by Skill Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_salary_by_skill_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold salary_by_skill_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date = _safe_str(r.get("ingestion_date"))
        skill_uri = _safe_str(r.get("esco_skill_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date, skill_uri),
            "ingestion_date": date,
            "country": country,
            "esco_skill_concept_uri": skill_uri,
            "esco_skill_preferred_label": _safe_str(r.get("esco_skill_preferred_label")),
            "salary_jobs_count": _safe_int(r.get("salary_jobs_count")),
            "avg_salary_mean": _safe_float(r.get("avg_salary_mean")),
            "min_salary_min": _safe_float(r.get("min_salary_min")),
            "max_salary_max": _safe_float(r.get("max_salary_max")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Occupation-Skill Graph
# ═══════════════════════════════════════════════════════════════════════════


def build_occupation_skill_graph_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold occupation_skill_graph rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date = _safe_str(r.get("ingestion_date"))
        occ_uri = _safe_str(r.get("esco_occupation_concept_uri"))
        skill_uri = _safe_str(r.get("esco_skill_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date, occ_uri, skill_uri),
            "ingestion_date": date,
            "country": country,
            "esco_occupation_concept_uri": occ_uri,
            "esco_occupation_preferred_label": _safe_str(r.get("esco_occupation_preferred_label")),
            "esco_skill_concept_uri": skill_uri,
            "esco_skill_preferred_label": _safe_str(r.get("esco_skill_preferred_label")),
            "relation_type": _safe_str(r.get("relation_type")),
            "matched_jobs_count": _safe_int(r.get("matched_jobs_count")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Job-Skill Matches (optional / exploration)
# ═══════════════════════════════════════════════════════════════════════════


def build_job_skill_matches_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold job_skill_matches rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date = _safe_str(r.get("ingestion_date"))
        job_id = _safe_str(r.get("job_id"))
        skill_uri = _safe_str(r.get("esco_skill_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date, job_id, skill_uri),
            "ingestion_date": date,
            "country": country,
            "job_id": job_id,
            "esco_skill_concept_uri": skill_uri,
            "esco_skill_preferred_label": _safe_str(r.get("esco_skill_preferred_label")),
            "matched_label": _safe_str(r.get("matched_label")),
            "matched_label_type": _safe_str(r.get("matched_label_type")),
            "match_method": _safe_str(r.get("match_method")),
            "title_hit": bool(r.get("title_hit")),
            "description_hit": bool(r.get("description_hit")),
            "match_score": _safe_float(r.get("match_score")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Job-Occupation Matches (optional / exploration)
# ═══════════════════════════════════════════════════════════════════════════


def build_job_occupation_matches_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold job_occupation_matches rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date = _safe_str(r.get("ingestion_date"))
        job_id = _safe_str(r.get("job_id"))
        occ_uri = _safe_str(r.get("esco_occupation_concept_uri"))
        method = _safe_str(r.get("match_method"))

        doc = {
            "doc_id": _deterministic_id(country, date, job_id, occ_uri, method),
            "ingestion_date": date,
            "country": country,
            "job_id": job_id,
            "esco_occupation_concept_uri": occ_uri,
            "esco_occupation_preferred_label": _safe_str(r.get("esco_occupation_preferred_label")),
            "match_method": method,
            "match_score": _safe_float(r.get("match_score")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Skill Emerging Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_skill_emerging_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold skill_emerging_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date_val = _safe_str(r.get("ingestion_date"))
        skill_uri = _safe_str(r.get("esco_skill_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date_val, skill_uri),
            "ingestion_date": date_val,
            "country": country,
            "esco_skill_concept_uri": skill_uri,
            "esco_skill_preferred_label": _safe_str(r.get("esco_skill_preferred_label")),
            "jobs_count": _safe_int(r.get("jobs_count")),
            "momentum_score": _safe_float(r.get("momentum_score")),
            "acceleration_score": _safe_float(r.get("acceleration_score")),
            "novelty_score": _safe_float(r.get("novelty_score")),
            "emerging_composite_score": _safe_float(r.get("emerging_composite_score")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Occupation Market Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_occupation_market_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold occupation_market_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date_val = _safe_str(r.get("ingestion_date"))
        occ_uri = _safe_str(r.get("esco_occupation_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date_val, occ_uri),
            "ingestion_date": date_val,
            "country": country,
            "esco_occupation_concept_uri": occ_uri,
            "esco_occupation_preferred_label": _safe_str(r.get("esco_occupation_preferred_label")),
            "total_jobs_count": _safe_int(r.get("total_jobs_count")),
            "unique_skills_count": _safe_int(r.get("unique_skills_count")),
            "avg_match_score": _safe_float(r.get("avg_match_score")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Skill Demand Segments Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_skill_demand_segments_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold skill_demand_segments_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date_val = _safe_str(r.get("ingestion_date"))
        skill_uri = _safe_str(r.get("esco_skill_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date_val, skill_uri),
            "ingestion_date": date_val,
            "country": country,
            "esco_skill_concept_uri": skill_uri,
            "esco_skill_preferred_label": _safe_str(r.get("esco_skill_preferred_label")),
            "segment_id": _safe_int(r.get("segment_id")),
            "segment_label": _safe_str(r.get("segment_label")),
            "jobs_count": _safe_int(r.get("jobs_count")),
            "unique_companies_count": _safe_int(r.get("unique_companies_count")),
            "unique_locations_count": _safe_int(r.get("unique_locations_count")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Occupation Profile Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_occupation_profile_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold occupation_profile_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date_val = _safe_str(r.get("ingestion_date"))
        occ_uri = _safe_str(r.get("esco_occupation_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date_val, occ_uri),
            "ingestion_date": date_val,
            "country": country,
            "esco_occupation_concept_uri": occ_uri,
            "esco_occupation_concept_uri_uuid": _safe_str(
                r.get("esco_occupation_concept_uri_uuid")
            ),
            "esco_occupation_preferred_label": _safe_str(r.get("esco_occupation_preferred_label")),
            "occupation_search_text": _safe_str(r.get("occupation_search_text")),
            "matched_jobs_count": _safe_int(r.get("matched_jobs_count")),
            "distinct_companies_count": _safe_int(r.get("distinct_companies_count")),
            "distinct_locations_count": _safe_int(r.get("distinct_locations_count")),
            "avg_salary_mean": _safe_float(r.get("avg_salary_mean")),
            "top_essential_skills_json": _safe_str(r.get("top_essential_skills_json")),
            "top_optional_skills_json": _safe_str(r.get("top_optional_skills_json")),
            "top_companies_json": _safe_str(r.get("top_companies_json")),
            "related_skills_count": _safe_int(r.get("related_skills_count")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Skill Profile Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_skill_profile_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold skill_profile_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date_val = _safe_str(r.get("ingestion_date"))
        skill_uri = _safe_str(r.get("esco_skill_concept_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date_val, skill_uri),
            "ingestion_date": date_val,
            "country": country,
            "esco_skill_concept_uri": skill_uri,
            "esco_skill_concept_uri_uuid": _safe_str(r.get("esco_skill_concept_uri_uuid")),
            "esco_skill_preferred_label": _safe_str(r.get("esco_skill_preferred_label")),
            "skill_type": _safe_str(r.get("skill_type")),
            "skill_search_text": _safe_str(r.get("skill_search_text")),
            "jobs_count": _safe_int(r.get("jobs_count")),
            "companies_count": _safe_int(r.get("companies_count")),
            "locations_count": _safe_int(r.get("locations_count")),
            "avg_salary_mean": _safe_float(r.get("avg_salary_mean")),
            "top_occupations_json": _safe_str(r.get("top_occupations_json")),
            "top_companies_json": _safe_str(r.get("top_companies_json")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Occupation Similarity Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_occupation_similarity_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold occupation_similarity_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date_val = _safe_str(r.get("ingestion_date"))
        src_uri = _safe_str(r.get("source_occupation_uri"))
        tgt_uri = _safe_str(r.get("target_occupation_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date_val, src_uri, tgt_uri),
            "ingestion_date": date_val,
            "country": country,
            "source_occupation_uri": src_uri,
            "source_occupation_label": _safe_str(r.get("source_occupation_label")),
            "target_occupation_uri": tgt_uri,
            "target_occupation_label": _safe_str(r.get("target_occupation_label")),
            "similarity_score": _safe_float(r.get("similarity_score")),
            "shared_skill_count": _safe_int(r.get("shared_skill_count")),
            "shared_essential_skill_count": _safe_int(r.get("shared_essential_skill_count")),
            "shared_optional_skill_count": _safe_int(r.get("shared_optional_skill_count")),
            "source_skill_count": _safe_int(r.get("source_skill_count")),
            "target_skill_count": _safe_int(r.get("target_skill_count")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Occupation Transition Daily
# ═══════════════════════════════════════════════════════════════════════════


def build_occupation_transition_documents(rows: list[Any]) -> list[dict[str, Any]]:
    """Build documents from Gold occupation_transition_daily rows."""
    documents: list[dict[str, Any]] = []
    for row in rows:
        r = row.asDict() if hasattr(row, "asDict") else dict(row)
        country = _safe_str(r.get("country"))
        date_val = _safe_str(r.get("ingestion_date"))
        from_uri = _safe_str(r.get("from_occupation_uri"))
        to_uri = _safe_str(r.get("to_occupation_uri"))

        doc = {
            "doc_id": _deterministic_id(country, date_val, from_uri, to_uri),
            "ingestion_date": date_val,
            "country": country,
            "from_occupation_uri": from_uri,
            "from_occupation_label": _safe_str(r.get("from_occupation_label")),
            "to_occupation_uri": to_uri,
            "to_occupation_label": _safe_str(r.get("to_occupation_label")),
            "similarity_score": _safe_float(r.get("similarity_score")),
            "shared_skills_json": _safe_str(r.get("shared_skills_json")),
            "missing_skills_json": _safe_str(r.get("missing_skills_json")),
            "missing_essential_skills_json": _safe_str(r.get("missing_essential_skills_json")),
            "missing_optional_skills_json": _safe_str(r.get("missing_optional_skills_json")),
            "missing_skill_count": _safe_int(r.get("missing_skill_count")),
            "missing_essential_skill_count": _safe_int(r.get("missing_essential_skill_count")),
            "transition_difficulty_score": _safe_float(r.get("transition_difficulty_score")),
            "from_avg_salary_mean": _safe_float(r.get("from_avg_salary_mean")),
            "to_avg_salary_mean": _safe_float(r.get("to_avg_salary_mean")),
            "salary_delta_mean": _safe_float(r.get("salary_delta_mean")),
            "from_jobs_count": _safe_int(r.get("from_jobs_count")),
            "to_jobs_count": _safe_int(r.get("to_jobs_count")),
            "jobs_delta": _safe_int(r.get("jobs_delta")),
            "gold_run_id": _safe_str(r.get("gold_run_id")),
            "gold_generated_at_utc": _safe_datetime(r.get("gold_generated_at_utc")),
            "esco_version": _safe_str(r.get("esco_version")),
            "esco_lang": _safe_str(r.get("esco_lang")),
        }
        documents.append(doc)
    return documents


# ═══════════════════════════════════════════════════════════════════════════
# Registry: document builder name → function
# ═══════════════════════════════════════════════════════════════════════════

DOCUMENT_BUILDERS: dict[str, Any] = {
    "build_skill_demand_documents": build_skill_demand_documents,
    "build_salary_by_skill_documents": build_salary_by_skill_documents,
    "build_occupation_skill_graph_documents": build_occupation_skill_graph_documents,
    "build_job_skill_matches_documents": build_job_skill_matches_documents,
    "build_job_occupation_matches_documents": build_job_occupation_matches_documents,
    "build_skill_emerging_documents": build_skill_emerging_documents,
    "build_occupation_market_documents": build_occupation_market_documents,
    "build_skill_demand_segments_documents": build_skill_demand_segments_documents,
    # Career Navigation datasets
    "build_occupation_profile_documents": build_occupation_profile_documents,
    "build_skill_profile_documents": build_skill_profile_documents,
    "build_occupation_similarity_documents": build_occupation_similarity_documents,
    "build_occupation_transition_documents": build_occupation_transition_documents,
}
