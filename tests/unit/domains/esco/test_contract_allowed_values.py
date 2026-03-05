"""Unit tests for Bronze contract column & allowed-value enforcement.

These tests verify the validation functions used by the Bronze extraction
pipeline *without* requiring Spark (column-existence checks operate on
plain lists; allowed-value checks are tested via a minimal Spark local
session only where needed, but we isolate the logic here).
"""

from __future__ import annotations

import zipfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from skill_radar.domains.esco.bronze.errors import BronzeValidationError
from skill_radar.domains.esco.bronze.validation import (
    extract_csv_from_zip,
    validate_required_columns,
)
from skill_radar.domains.esco.contract import load_esco_contract

# ---------------------------------------------------------------------------
# Fixture: load the ESCO contract once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def contract():
    return load_esco_contract()


# ---------------------------------------------------------------------------
# Required-column enforcement
# ---------------------------------------------------------------------------


class TestRequiredColumns:
    """Test _validate_required_columns against the contract."""

    def test_skills_all_present(self, contract) -> None:
        """No error when all required columns are present."""
        ent = next(e for e in contract.entities if e.name == "skills")
        actual = [
            "preferredLabel",
            "altLabels",
            "hiddenLabels",
            "description",
            "skillType",
            "reuseLevel",
            "extraCol",
        ]
        # Should not raise
        validate_required_columns(actual, ent)

    def test_skills_missing_column_raises(self, contract) -> None:
        ent = next(e for e in contract.entities if e.name == "skills")
        actual = ["preferredLabel", "altLabels"]  # missing several
        with pytest.raises(BronzeValidationError, match="missing required columns"):
            validate_required_columns(actual, ent)

    def test_occupations_all_present(self, contract) -> None:
        ent = next(e for e in contract.entities if e.name == "occupations")
        actual = ["preferredLabel", "altLabels", "hiddenLabels", "description"]
        validate_required_columns(actual, ent)

    def test_occupations_missing_raises(self, contract) -> None:
        ent = next(e for e in contract.entities if e.name == "occupations")
        actual = ["preferredLabel"]
        with pytest.raises(BronzeValidationError, match="missing required columns"):
            validate_required_columns(actual, ent)

    def test_relations_all_present(self, contract) -> None:
        ent = next(e for e in contract.entities if e.name == "relations")
        actual = ["occupationLabel", "relationType", "skillType", "skillLabel"]
        validate_required_columns(actual, ent)

    def test_relations_missing_raises(self, contract) -> None:
        ent = next(e for e in contract.entities if e.name == "relations")
        actual = ["occupationLabel", "relationType"]
        with pytest.raises(BronzeValidationError, match="missing required columns"):
            validate_required_columns(actual, ent)


# ---------------------------------------------------------------------------
# CSV extraction from ZIP
# ---------------------------------------------------------------------------


class TestCsvExtraction:
    def test_extract_existing_csv(self, tmp_path: Path) -> None:
        """Extracting a CSV that exists in the ZIP works."""
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("skills_fr.csv", "col1,col2\na,b\n")

        out = extract_csv_from_zip(zip_path, "skills_fr.csv", tmp_path)
        assert out.exists()
        assert out.read_text().startswith("col1,col2")

    def test_extract_missing_csv_raises(self, tmp_path: Path) -> None:
        """Extracting a CSV not in the ZIP raises BronzeValidationError."""
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("other.csv", "x\n")

        with pytest.raises(BronzeValidationError, match="not found in ZIP"):
            extract_csv_from_zip(zip_path, "skills_fr.csv", tmp_path)


# ---------------------------------------------------------------------------
# Contract entity names align with the YAML
# ---------------------------------------------------------------------------


class TestContractEntities:
    def test_expected_entities_exist(self, contract) -> None:
        names = {e.name for e in contract.entities}
        assert names == {"skills", "occupations", "relations"}
