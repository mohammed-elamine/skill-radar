"""Gold matching models — scoring config, result dataclasses, and constants.

This module is the single source of truth for:
- match score scales (skill and occupation)
- candidate generation parameters
- Gold run metadata
- result dataclasses for orchestration reporting
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# ── Candidate generation parameters ───────────────────────────────────────

# Maximum n-gram size for job candidate phrase extraction.
#
# ESCO skill labels rarely exceed 6 whitespace-delimited tokens in their
# normalised form (e.g. "capacity to manage personal finances" = 6 tokens).
# Setting a higher cap increases the number of candidate rows (and memory)
# without meaningful recall gain.  The actual ceiling used at runtime is
# min(CANDIDATE_MAX_NGRAM_SIZE, max_label_tokens) so this is an upper
# bound, not a fixed value.
CANDIDATE_MAX_NGRAM_SIZE: int = 6

# ── Match score configuration ─────────────────────────────────────────────

MATCH_METHOD_SKILL = "candidate_equijoin_v2"
MATCH_METHOD_OCCUPATION_TITLE = "title_exact_v1"
MATCH_METHOD_OCCUPATION_RELATION = "relation_score_v1"

# Deterministic score scale for skill matching.
# Keys: (label_type, text_source) → score
#
# ESCO label semantics (see ESCO data model documentation):
#   - preferred: Canonical term — the authoritative name for this skill.
#   - alt (non-preferred term / NPT): Legitimate labour-market-used
#     synonyms, spelling variants, abbreviations, and declensions.
#     These remain strong evidence, scored close to preferred.
#   - hidden: Retrieval / indexing-oriented signals kept for search and
#     text-mining recall. May be outdated, misspelled, or politically
#     incorrect — intentionally hidden from end users and therefore
#     materially penalised relative to alt labels.
#
# Axis rationale:
#   preferred ≈ alt  >>  hidden       (label trustworthiness)
#   title     >  description           (text source signal strength)
SKILL_MATCH_SCORES: dict[tuple[str, str], float] = {
    ("preferred", "title"): 1.00,
    ("alt", "title"): 0.95,
    ("hidden", "title"): 0.65,
    ("preferred", "description"): 0.75,
    ("alt", "description"): 0.70,
    ("hidden", "description"): 0.45,
}

# Occupation title match score
OCCUPATION_TITLE_MATCH_SCORE = 1.0

# Occupation relation-inferred base score (scaled by supporting skill count)
OCCUPATION_RELATION_BASE_SCORE = 0.5
OCCUPATION_RELATION_SKILL_WEIGHT = 0.1
OCCUPATION_RELATION_MAX_SCORE = 0.95


def compute_occupation_relation_score(supporting_skill_count: int) -> float:
    """Compute deterministic occupation match score from supporting skill count.

    Score = base + (count * weight), capped at max.
    """
    raw = OCCUPATION_RELATION_BASE_SCORE + (
        supporting_skill_count * OCCUPATION_RELATION_SKILL_WEIGHT
    )
    return min(raw, OCCUPATION_RELATION_MAX_SCORE)


# ── Result dataclasses ────────────────────────────────────────────────────


@dataclass
class GoldMatchingResult:
    """Outcome of the Gold matching pipeline."""

    run_id: str = ""
    ingestion_date: str = ""
    country: str = ""
    esco_version: str = ""
    esco_lang: str = ""
    adzuna_jobs_count: int = 0
    skill_dictionary_size: int = 0
    job_skill_matches_count: int = 0
    job_occupation_matches_count: int = 0
    success: bool = True
    error: str = ""

    def summary_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class GoldAnalyticsResult:
    """Outcome of the Gold analytics pipeline."""

    run_id: str = ""
    ingestion_date: str = ""
    country: str = ""
    esco_version: str = ""
    esco_lang: str = ""
    skill_demand_rows: int = 0
    salary_by_skill_rows: int = 0
    occupation_skill_graph_rows: int = 0
    skill_emerging_rows: int = 0
    occupation_market_rows: int = 0
    skill_demand_segments_rows: int = 0
    success: bool = True
    error: str = ""

    def summary_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class GoldPipelineResult:
    """Combined outcome of the full Gold pipeline."""

    matching: GoldMatchingResult = field(default_factory=GoldMatchingResult)
    analytics: GoldAnalyticsResult = field(default_factory=GoldAnalyticsResult)

    @property
    def success(self) -> bool:
        return self.matching.success and self.analytics.success

    def summary_dict(self) -> dict[str, Any]:
        return {
            "matching": self.matching.summary_dict(),
            "analytics": self.analytics.summary_dict(),
            "success": self.success,
        }
