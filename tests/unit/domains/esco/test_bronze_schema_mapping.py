"""Unit tests for Bronze schema mapping (no Spark dependency).

Tests verify the contract-driven rename logic: ``rename_mapping()`` and
``newline_raw_fields()`` read from :class:`ContractEntity` objects loaded
from ``contract.yaml``.
"""

from __future__ import annotations

import pytest

from skill_radar.domains.esco.bronze.schema_mapping import (
    LINEAGE_COLUMNS,
    count_column_name,
    newline_raw_fields,
    norm_column_name,
    rename_mapping,
    to_snake_case,
)
from skill_radar.domains.esco.contract import load_esco_contract

# ---------------------------------------------------------------------------
# Fixture: contract entities
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def contract():
    return load_esco_contract()


@pytest.fixture(scope="module")
def skills_entity(contract):
    return next(e for e in contract.entities if e.name == "skills")


@pytest.fixture(scope="module")
def occupations_entity(contract):
    return next(e for e in contract.entities if e.name == "occupations")


@pytest.fixture(scope="module")
def relations_entity(contract):
    return next(e for e in contract.entities if e.name == "relations")


# ---------------------------------------------------------------------------
# to_snake_case
# ---------------------------------------------------------------------------


class TestToSnakeCase:
    @pytest.mark.parametrize(
        ("input_", "expected"),
        [
            ("preferredLabel", "preferred_label"),
            ("altLabels", "alt_labels"),
            ("hiddenLabels", "hidden_labels"),
            ("skillType", "skill_type"),
            ("reuseLevel", "reuse_level"),
            ("occupationLabel", "occupation_label"),
            ("relationType", "relation_type"),
            ("occupationSkillRelations", "occupation_skill_relations"),
            ("description", "description"),
            ("already_snake", "already_snake"),
            ("URL", "url"),
        ],
    )
    def test_conversion(self, input_: str, expected: str) -> None:
        assert to_snake_case(input_) == expected


# ---------------------------------------------------------------------------
# rename_mapping (contract-driven)
# ---------------------------------------------------------------------------


class TestRenameMapping:
    def test_skills_explicit_renames(self, skills_entity) -> None:
        vendor_cols = [
            "preferredLabel",
            "altLabels",
            "hiddenLabels",
            "description",
            "skillType",
            "reuseLevel",
        ]
        mapping = rename_mapping(skills_entity, vendor_cols)
        assert mapping == {
            "preferredLabel": "preferred_label",
            "altLabels": "alt_labels_raw",
            "hiddenLabels": "hidden_labels_raw",
            "description": "description",
            "skillType": "skill_type",
            "reuseLevel": "reuse_level",
        }

    def test_relations_explicit_renames(self, relations_entity) -> None:
        vendor_cols = ["occupationLabel", "relationType", "skillType", "skillLabel"]
        mapping = rename_mapping(relations_entity, vendor_cols)
        assert mapping == {
            "occupationLabel": "occupation_label",
            "relationType": "relation_type",
            "skillType": "skill_type",
            "skillLabel": "skill_label",
        }

    def test_occupations_explicit_renames(self, occupations_entity) -> None:
        vendor_cols = ["preferredLabel", "altLabels", "hiddenLabels", "description"]
        mapping = rename_mapping(occupations_entity, vendor_cols)
        assert mapping == {
            "preferredLabel": "preferred_label",
            "altLabels": "alt_labels_raw",
            "hiddenLabels": "hidden_labels_raw",
            "description": "description",
        }

    def test_unknown_column_falls_back_to_snake(self, skills_entity) -> None:
        """Vendor columns not in the contract ``renames`` use to_snake_case."""
        mapping = rename_mapping(skills_entity, ["conceptUri"])
        assert mapping == {"conceptUri": "concept_uri"}


# ---------------------------------------------------------------------------
# Newline field helpers
# ---------------------------------------------------------------------------


class TestNewlineHelpers:
    def test_skills_newline_raw_fields(self, skills_entity) -> None:
        fields = newline_raw_fields(skills_entity)
        assert "alt_labels_raw" in fields
        assert "hidden_labels_raw" in fields

    def test_occupations_newline_raw_fields(self, occupations_entity) -> None:
        fields = newline_raw_fields(occupations_entity)
        assert "alt_labels_raw" in fields
        assert "hidden_labels_raw" in fields

    def test_relations_no_newline_fields(self, relations_entity) -> None:
        fields = newline_raw_fields(relations_entity)
        assert fields == ()

    def test_norm_column_name(self) -> None:
        assert norm_column_name("alt_labels_raw") == "alt_labels_norm"
        assert norm_column_name("hidden_labels_raw") == "hidden_labels_norm"

    def test_count_column_name(self) -> None:
        assert count_column_name("alt_labels_raw") == "alt_labels_count"
        assert count_column_name("hidden_labels_raw") == "hidden_labels_count"


# ---------------------------------------------------------------------------
# Lineage columns
# ---------------------------------------------------------------------------


class TestLineageColumns:
    def test_required_lineage_columns(self) -> None:
        expected = {
            "dataset",
            "entity",
            "version",
            "lang",
            "source_zip_key",
            "manifest_key",
            "artifact_sha256",
            "ingested_at_utc",
            "run_id",
        }
        assert set(LINEAGE_COLUMNS) == expected
