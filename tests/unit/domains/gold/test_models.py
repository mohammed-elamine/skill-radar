"""Unit tests for Gold matching models -- scoring config and result dataclasses."""

from __future__ import annotations

import pytest

from skill_radar.domains.gold.matching.models import (
    CANDIDATE_MAX_NGRAM_SIZE,
    MATCH_METHOD_OCCUPATION_RELATION,
    MATCH_METHOD_OCCUPATION_TITLE,
    MATCH_METHOD_SKILL,
    OCCUPATION_RELATION_BASE_SCORE,
    OCCUPATION_RELATION_MAX_SCORE,
    OCCUPATION_RELATION_SKILL_WEIGHT,
    OCCUPATION_TITLE_MATCH_SCORE,
    SKILL_MATCH_SCORES,
    GoldAnalyticsResult,
    GoldMatchingResult,
    GoldPipelineResult,
    compute_occupation_relation_score,
)

LABEL_TYPES = ("preferred", "alt", "hidden")
TEXT_SOURCES = ("title", "description")


class TestSkillMatchScores:
    """SKILL_MATCH_SCORES integrity checks."""

    def test_all_scores_in_0_1_range(self) -> None:
        for key, score in SKILL_MATCH_SCORES.items():
            assert 0.0 <= score <= 1.0, f"Score {score} for {key} out of [0, 1]"

    def test_preferred_title_is_highest(self) -> None:
        assert SKILL_MATCH_SCORES[("preferred", "title")] == 1.00

    def test_hidden_description_is_lowest(self) -> None:
        assert SKILL_MATCH_SCORES[("hidden", "description")] == 0.45

    def test_alt_close_to_preferred(self) -> None:
        """Alt labels are legitimate NPTs -- gap to preferred must be small."""
        for source in TEXT_SOURCES:
            pref = SKILL_MATCH_SCORES[("preferred", source)]
            alt = SKILL_MATCH_SCORES[("alt", source)]
            gap = pref - alt
            assert gap <= 0.10, f"alt-to-preferred gap too large for {source}: {gap}"

    def test_hidden_materially_below_alt(self) -> None:
        """Hidden labels are indexing signals -- gap to alt must be significant."""
        for source in TEXT_SOURCES:
            alt = SKILL_MATCH_SCORES[("alt", source)]
            hid = SKILL_MATCH_SCORES[("hidden", source)]
            gap = alt - hid
            assert gap >= 0.20, f"alt-to-hidden gap too small for {source}: {gap}"

    def test_title_always_higher_than_description(self) -> None:
        for label_type in LABEL_TYPES:
            title_score = SKILL_MATCH_SCORES[(label_type, "title")]
            desc_score = SKILL_MATCH_SCORES[(label_type, "description")]
            msg = f"{label_type} title should score higher than description"
            assert title_score > desc_score, msg

    def test_preferred_always_higher_than_alt(self) -> None:
        for source in TEXT_SOURCES:
            pref = SKILL_MATCH_SCORES[("preferred", source)]
            alt = SKILL_MATCH_SCORES[("alt", source)]
            msg = f"preferred should score higher than alt for {source}"
            assert pref > alt, msg

    def test_alt_always_higher_than_hidden(self) -> None:
        for source in TEXT_SOURCES:
            alt = SKILL_MATCH_SCORES[("alt", source)]
            hid = SKILL_MATCH_SCORES[("hidden", source)]
            msg = f"alt should score higher than hidden for {source}"
            assert alt > hid, msg

    def test_all_six_combinations_present(self) -> None:
        expected_keys = {(lt, src) for lt in LABEL_TYPES for src in TEXT_SOURCES}
        assert set(SKILL_MATCH_SCORES.keys()) == expected_keys


class TestMatchMethodConstants:
    """Match method string constants."""

    def test_skill_method_stable(self) -> None:
        assert MATCH_METHOD_SKILL == "candidate_equijoin_v2"

    def test_occupation_title_method_stable(self) -> None:
        assert MATCH_METHOD_OCCUPATION_TITLE == "title_exact_v1"

    def test_occupation_relation_method_stable(self) -> None:
        assert MATCH_METHOD_OCCUPATION_RELATION == "relation_score_v1"

    def test_candidate_max_ngram_size(self) -> None:
        assert CANDIDATE_MAX_NGRAM_SIZE == 6
        assert isinstance(CANDIDATE_MAX_NGRAM_SIZE, int)


class TestOccupationRelationScore:
    """compute_occupation_relation_score deterministic scoring."""

    def test_zero_skills_returns_base(self) -> None:
        assert compute_occupation_relation_score(0) == OCCUPATION_RELATION_BASE_SCORE

    def test_one_skill(self) -> None:
        expected = OCCUPATION_RELATION_BASE_SCORE + OCCUPATION_RELATION_SKILL_WEIGHT
        assert compute_occupation_relation_score(1) == pytest.approx(expected)

    def test_three_skills(self) -> None:
        expected = OCCUPATION_RELATION_BASE_SCORE + 3 * OCCUPATION_RELATION_SKILL_WEIGHT
        assert compute_occupation_relation_score(3) == pytest.approx(expected)

    def test_capped_at_max(self) -> None:
        assert compute_occupation_relation_score(10) == OCCUPATION_RELATION_MAX_SCORE
        assert compute_occupation_relation_score(100) == OCCUPATION_RELATION_MAX_SCORE

    def test_cap_boundary(self) -> None:
        # At 4 skills: 0.5 + 4*0.1 = 0.9 -> not capped
        assert compute_occupation_relation_score(4) == pytest.approx(0.9)
        # At 5 skills: 0.5 + 5*0.1 = 1.0 -> capped to 0.95
        assert compute_occupation_relation_score(5) == OCCUPATION_RELATION_MAX_SCORE

    def test_monotonically_increasing(self) -> None:
        scores = [compute_occupation_relation_score(i) for i in range(10)]
        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1]

    def test_occupation_title_match_score(self) -> None:
        assert OCCUPATION_TITLE_MATCH_SCORE == 1.0


class TestGoldMatchingResult:
    """GoldMatchingResult dataclass."""

    def test_defaults(self) -> None:
        result = GoldMatchingResult()
        assert result.success is True
        assert result.job_skill_matches_count == 0
        assert result.error == ""

    def test_summary_dict(self) -> None:
        result = GoldMatchingResult(run_id="abc", adzuna_jobs_count=42)
        summary = result.summary_dict()
        assert isinstance(summary, dict)
        assert summary["run_id"] == "abc"
        assert summary["adzuna_jobs_count"] == 42


class TestGoldAnalyticsResult:
    """GoldAnalyticsResult dataclass."""

    def test_defaults(self) -> None:
        result = GoldAnalyticsResult()
        assert result.success is True
        assert result.skill_demand_rows == 0

    def test_summary_dict(self) -> None:
        result = GoldAnalyticsResult(run_id="xyz", skill_demand_rows=100)
        summary = result.summary_dict()
        assert summary["skill_demand_rows"] == 100


class TestGoldPipelineResult:
    """GoldPipelineResult dataclass."""

    def test_defaults(self) -> None:
        result = GoldPipelineResult()
        assert result.matching is not None
        assert result.analytics is not None
        assert result.success is True

    def test_success_follows_children(self) -> None:
        matching = GoldMatchingResult(success=True)
        analytics = GoldAnalyticsResult(success=False, error="oops")
        result = GoldPipelineResult(matching=matching, analytics=analytics)
        assert result.success is False

    def test_success_when_both_pass(self) -> None:
        matching = GoldMatchingResult(success=True)
        analytics = GoldAnalyticsResult(success=True)
        result = GoldPipelineResult(matching=matching, analytics=analytics)
        assert result.success is True
