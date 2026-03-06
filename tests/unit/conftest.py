"""Shared fixtures for unit tests."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from skill_radar.config.models import PlatformSettings
from skill_radar.domains.esco.contract import load_esco_contract

if TYPE_CHECKING:
    from skill_radar.domains.esco.contract.models import EscoContract

# ---------------------------------------------------------------------------
# Platform config
# ---------------------------------------------------------------------------


@pytest.fixture
def platform_config() -> PlatformSettings:
    """Default platform configuration for tests."""
    return PlatformSettings()


# ---------------------------------------------------------------------------
# ESCO contract
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def esco_contract() -> EscoContract:
    """Load the live ESCO contract YAML."""
    return load_esco_contract()


# ---------------------------------------------------------------------------
# Fixture directory
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Path to the test fixtures/esco directory (auto-created)."""
    d = Path(__file__).parent.parent / "fixtures" / "esco"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_csv_bytes(columns: list[str], rows: list[list[str]] | None = None) -> bytes:
    """Create CSV bytes from column names and optional data rows."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in rows or []:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------------------
# ESCO ZIP fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def valid_esco_zip(fixtures_dir: Path) -> Path:
    """Create a valid ESCO ZIP containing all required CSVs and columns."""
    zip_path = fixtures_dir / "esco_valid.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(
            "skills_fr.csv",
            _make_csv_bytes(
                [
                    "conceptUri",
                    "preferredLabel",
                    "altLabels",
                    "hiddenLabels",
                    "description",
                    "skillType",
                    "reuseLevel",
                ]
            ),
        )
        zf.writestr(
            "occupations_fr.csv",
            _make_csv_bytes(
                [
                    "conceptUri",
                    "preferredLabel",
                    "altLabels",
                    "hiddenLabels",
                    "description",
                ]
            ),
        )
        zf.writestr(
            "occupationSkillRelations_fr.csv",
            _make_csv_bytes(
                [
                    "occupationUri",
                    "occupationLabel",
                    "relationType",
                    "skillType",
                    "skillLabel",
                    "skillUri",
                ]
            ),
        )
    return zip_path


@pytest.fixture(scope="session")
def missing_csv_zip(fixtures_dir: Path) -> Path:
    """ZIP with one or more required entity CSVs missing."""
    zip_path = fixtures_dir / "esco_missing_csv.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        # Only skills — occupations and relation CSVs are missing.
        zf.writestr(
            "skills_fr.csv",
            _make_csv_bytes(
                [
                    "preferredLabel",
                    "altLabels",
                    "hiddenLabels",
                    "description",
                    "skillType",
                    "reuseLevel",
                ]
            ),
        )
    return zip_path


@pytest.fixture(scope="session")
def missing_columns_zip(fixtures_dir: Path) -> Path:
    """ZIP with CSVs that are missing required columns."""
    zip_path = fixtures_dir / "esco_missing_cols.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("skills_fr.csv", _make_csv_bytes(["preferredLabel"]))
        zf.writestr("occupations_fr.csv", _make_csv_bytes(["preferredLabel"]))
        zf.writestr(
            "occupationSkillRelations_fr.csv",
            _make_csv_bytes(["occupationLabel"]),
        )
    return zip_path


@pytest.fixture
def not_a_zip(tmp_path: Path) -> Path:
    """A file that is not a valid ZIP archive."""
    p = tmp_path / "not_a_zip.zip"
    p.write_text("this is not a zip file")
    return p
