"""Tests for ESCO ZIP artifact validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from skill_radar.domains.esco.contract import load_esco_contract
from skill_radar.domains.esco.landing.validation import (
    validate_artifact,
    validate_language,
    validate_version,
    validate_zip,
)


def _contract():
    return load_esco_contract()


# ---------------------------------------------------------------------------
# Version validation
# ---------------------------------------------------------------------------


class TestVersionValidation:
    def test_valid_semver(self):
        assert validate_version("v1.2.1", _contract()).passed

    def test_valid_semver_zeros(self):
        assert validate_version("v0.0.0", _contract()).passed

    def test_missing_v_prefix(self):
        assert not validate_version("1.2.1", _contract()).passed

    def test_incomplete_version(self):
        assert not validate_version("v1", _contract()).passed

    def test_extra_components(self):
        assert not validate_version("v1.2.1.4", _contract()).passed

    def test_alpha_version(self):
        assert not validate_version("v1.2.1-alpha", _contract()).passed


# ---------------------------------------------------------------------------
# Language validation
# ---------------------------------------------------------------------------


class TestLanguageValidation:
    def test_supported_fr(self):
        assert validate_language("fr", _contract()).passed

    def test_supported_en(self):
        assert validate_language("en", _contract()).passed

    def test_unsupported_de(self):
        assert not validate_language("de", _contract()).passed

    def test_unsupported_empty(self):
        assert not validate_language("", _contract()).passed


# ---------------------------------------------------------------------------
# ZIP validation
# ---------------------------------------------------------------------------


class TestZipValidation:
    def test_valid_zip(self, valid_esco_zip: Path):
        result = validate_zip(valid_esco_zip, "fr", _contract())
        assert result.passed

    def test_not_a_zip(self, not_a_zip: Path):
        result = validate_zip(not_a_zip, "fr", _contract())
        assert not result.passed
        assert any(c.name == "valid_zip" and not c.passed for c in result.checks)

    def test_missing_csv(self, missing_csv_zip: Path):
        result = validate_zip(missing_csv_zip, "fr", _contract())
        assert not result.passed
        # At least one file_present check should fail
        failing = [c for c in result.checks if c.name.startswith("file_present") and not c.passed]
        assert len(failing) >= 1

    def test_missing_columns(self, missing_columns_zip: Path):
        result = validate_zip(missing_columns_zip, "fr", _contract())
        assert not result.passed
        # At least one column check should fail
        failing = [c for c in result.checks if c.name.startswith("column:") and not c.passed]
        assert len(failing) >= 1


# ---------------------------------------------------------------------------
# Full artifact validation
# ---------------------------------------------------------------------------


class TestFullArtifactValidation:
    def test_valid_artifact(self, valid_esco_zip: Path):
        result = validate_artifact(valid_esco_zip, "v1.2.1", "fr", _contract())
        assert result.passed

    def test_invalid_version_short_circuits(self, valid_esco_zip: Path):
        result = validate_artifact(valid_esco_zip, "bad", "fr", _contract())
        assert not result.passed
        # Should NOT have any ZIP-level checks (short-circuited)
        assert not any(c.name.startswith("file_present") for c in result.checks)

    def test_invalid_language_short_circuits(self, valid_esco_zip: Path):
        result = validate_artifact(valid_esco_zip, "v1.0.0", "de", _contract())
        assert not result.passed
        assert not any(c.name.startswith("file_present") for c in result.checks)

    def test_both_invalid(self, valid_esco_zip: Path):
        result = validate_artifact(valid_esco_zip, "bad", "de", _contract())
        assert not result.passed
        assert len(result.checks) == 2  # version + language only
