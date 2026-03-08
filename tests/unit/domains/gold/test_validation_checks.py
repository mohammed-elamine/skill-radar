"""Unit tests for Gold validation check factories.

These tests verify the check factory returns the expected NamedCheck
objects without actually running them (no Spark required).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from skill_radar.config.models import PlatformSettings
from skill_radar.domains.gold import schema as gold_schema
from skill_radar.platform.validate.checks.gold import (
    OCCUPATION_MATCHES_REQUIRED,
    OCCUPATION_SKILL_GRAPH_REQUIRED,
    SALARY_BY_SKILL_DAILY_REQUIRED,
    SKILL_DEMAND_DAILY_REQUIRED,
    SKILL_EMERGING_DAILY_REQUIRED,
    SKILL_MATCHES_REQUIRED,
    get_gold_checks,
)
from skill_radar.platform.validate.models import ExitCode, NamedCheck


class TestGoldCheckFactory:
    """get_gold_checks returns the expected list of NamedCheck objects."""

    @pytest.fixture
    def mock_spark(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def config(self) -> PlatformSettings:
        return PlatformSettings()

    def test_returns_list_of_named_checks(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_gold_checks(
            mock_spark,
            config,
            ingestion_date="2025-01-15",
            country="fr",
            esco_version="v1.2.1",
            esco_lang="fr",
        )
        assert isinstance(checks, list)
        assert all(isinstance(c, NamedCheck) for c in checks)

    def test_check_count_minimum(self, mock_spark: MagicMock, config: PlatformSettings) -> None:
        """We expect at least 1 namespace + 7 per table * 5 tables ≈ 36 checks."""
        checks = get_gold_checks(
            mock_spark,
            config,
            ingestion_date="2025-01-15",
            country="fr",
            esco_version="v1.2.1",
            esco_lang="fr",
        )
        assert len(checks) >= 30

    def test_all_checks_have_unique_names(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_gold_checks(
            mock_spark,
            config,
            ingestion_date="2025-01-15",
            country="fr",
            esco_version="v1.2.1",
            esco_lang="fr",
        )
        names = [c.name for c in checks]
        assert len(names) == len(set(names)), f"Duplicate check names: {names}"

    def test_all_checks_prefixed_with_gold(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_gold_checks(
            mock_spark,
            config,
            ingestion_date="2025-01-15",
            country="fr",
            esco_version="v1.2.1",
            esco_lang="fr",
        )
        for c in checks:
            assert c.name.startswith("gold."), f"Check name should start with 'gold.': {c.name}"

    def test_namespace_check_present(self, mock_spark: MagicMock, config: PlatformSettings) -> None:
        checks = get_gold_checks(
            mock_spark,
            config,
            ingestion_date="2025-01-15",
            country="fr",
            esco_version="v1.2.1",
            esco_lang="fr",
        )
        ns_checks = [c for c in checks if "namespace" in c.name]
        assert len(ns_checks) >= 1

    def test_table_exists_checks_for_all_8_tables(
        self, mock_spark: MagicMock, config: PlatformSettings
    ) -> None:
        checks = get_gold_checks(
            mock_spark,
            config,
            ingestion_date="2025-01-15",
            country="fr",
            esco_version="v1.2.1",
            esco_lang="fr",
        )
        exists_checks = [c for c in checks if "table_exists" in c.name]
        assert len(exists_checks) == 8


class TestRequiredColumns:
    """Verify required column lists are non-empty and contain expected fields."""

    def test_required_lists_are_schema_aliases(self) -> None:
        """Validation required lists must be the exact same objects as gold.schema."""
        assert SKILL_MATCHES_REQUIRED is gold_schema.SKILL_MATCHES_REQUIRED
        assert OCCUPATION_MATCHES_REQUIRED is gold_schema.OCCUPATION_MATCHES_REQUIRED
        assert SKILL_DEMAND_DAILY_REQUIRED is gold_schema.SKILL_DEMAND_DAILY_REQUIRED
        assert SALARY_BY_SKILL_DAILY_REQUIRED is gold_schema.SALARY_BY_SKILL_DAILY_REQUIRED
        assert OCCUPATION_SKILL_GRAPH_REQUIRED is gold_schema.OCCUPATION_SKILL_GRAPH_REQUIRED
        assert SKILL_EMERGING_DAILY_REQUIRED is gold_schema.SKILL_EMERGING_DAILY_REQUIRED

    def test_skill_matches_has_key_columns(self) -> None:
        assert "job_id" in SKILL_MATCHES_REQUIRED
        assert gold_schema.esco_skill("concept_uri") in SKILL_MATCHES_REQUIRED
        assert "match_score" in SKILL_MATCHES_REQUIRED

    def test_occupation_matches_has_key_columns(self) -> None:
        assert "job_id" in OCCUPATION_MATCHES_REQUIRED
        assert gold_schema.esco_occupation("concept_uri") in OCCUPATION_MATCHES_REQUIRED
        assert "match_score" in OCCUPATION_MATCHES_REQUIRED

    def test_demand_daily_has_key_columns(self) -> None:
        assert gold_schema.esco_skill("concept_uri") in SKILL_DEMAND_DAILY_REQUIRED
        assert "jobs_count" in SKILL_DEMAND_DAILY_REQUIRED
        assert "ingestion_date" in SKILL_DEMAND_DAILY_REQUIRED

    def test_salary_daily_has_key_columns(self) -> None:
        assert gold_schema.esco_skill("concept_uri") in SALARY_BY_SKILL_DAILY_REQUIRED
        assert "avg_salary_mean" in SALARY_BY_SKILL_DAILY_REQUIRED

    def test_graph_has_key_columns(self) -> None:
        assert gold_schema.esco_occupation("concept_uri") in OCCUPATION_SKILL_GRAPH_REQUIRED
        assert gold_schema.esco_skill("concept_uri") in OCCUPATION_SKILL_GRAPH_REQUIRED
        assert gold_schema.matched("jobs_count") in OCCUPATION_SKILL_GRAPH_REQUIRED


class TestGoldExitCode:
    """GOLD_FAILURE exit code exists and has expected value."""

    def test_gold_failure_code(self) -> None:
        assert ExitCode.GOLD_FAILURE == 45

    def test_gold_failure_between_silver_and_unexpected(self) -> None:
        assert ExitCode.SILVER_FAILURE < ExitCode.GOLD_FAILURE < ExitCode.UNEXPECTED
