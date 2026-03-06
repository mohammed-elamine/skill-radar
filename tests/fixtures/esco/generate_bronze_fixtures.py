"""Generate ESCO test fixture ZIP files for Bronze extraction tests.

Run with:  python -m tests.fixtures.esco.generate_bronze_fixtures
"""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent


def _csv_bytes(header: list[str], rows: list[list[str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def build_bronze_valid_zip(lang: str = "fr") -> bytes:
    """Build a small valid ESCO ZIP with data rows for Bronze tests."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # skills — must include conceptUri per contract.yaml
        zf.writestr(
            f"skills_{lang}.csv",
            _csv_bytes(
                [
                    "conceptUri",
                    "preferredLabel",
                    "altLabels",
                    "hiddenLabels",
                    "description",
                    "skillType",
                    "reuseLevel",
                ],
                [
                    [
                        "http://data.europa.eu/esco/skill/python-1",
                        "Programmation Python",
                        "Python\nPython3\nPy",
                        "langage serpent",
                        "Compétence en programmation Python",
                        "skill/competence",
                        "cross-sector",
                    ],
                    [
                        "http://data.europa.eu/esco/skill/data-analysis-2",
                        "Analyse de données",
                        "Data analysis\nAnalyse données",
                        "",
                        "Capacité à analyser des jeux de données",
                        "knowledge",
                        "sector-specific",
                    ],
                    [
                        "http://data.europa.eu/esco/skill/project-mgmt-3",
                        "Gestion de projet",
                        "Project management",
                        "PM\r\nchef de projet",
                        "Compétence de gestion",
                        "skill/competence",
                        "transversal",
                    ],
                ],
            ).decode("utf-8"),
        )

        # occupations — must include conceptUri per contract.yaml
        zf.writestr(
            f"occupations_{lang}.csv",
            _csv_bytes(
                ["conceptUri", "preferredLabel", "altLabels", "hiddenLabels", "description"],
                [
                    [
                        "http://data.europa.eu/esco/occupation/dev-1",
                        "Développeur logiciel",
                        "Software developer\nDéveloppeur",
                        "codeur",
                        "Développe des applications logicielles",
                    ],
                    [
                        "http://data.europa.eu/esco/occupation/ds-2",
                        "Data scientist",
                        "Scientifique des données",
                        "",
                        "Analyse des données complexes",
                    ],
                ],
            ).decode("utf-8"),
        )

        # relations — must include occupationUri and skillUri per contract.yaml
        zf.writestr(
            f"occupationSkillRelations_{lang}.csv",
            _csv_bytes(
                [
                    "occupationUri",
                    "occupationLabel",
                    "relationType",
                    "skillType",
                    "skillLabel",
                    "skillUri",
                ],
                [
                    [
                        "http://data.europa.eu/esco/occupation/dev-1",
                        "Développeur logiciel",
                        "essential",
                        "skill/competence",
                        "Programmation Python",
                        "http://data.europa.eu/esco/skill/python-1",
                    ],
                    [
                        "http://data.europa.eu/esco/occupation/dev-1",
                        "Développeur logiciel",
                        "optional",
                        "knowledge",
                        "Analyse de données",
                        "http://data.europa.eu/esco/skill/data-analysis-2",
                    ],
                    [
                        "http://data.europa.eu/esco/occupation/ds-2",
                        "Data scientist",
                        "essential",
                        "knowledge",
                        "Analyse de données",
                        "http://data.europa.eu/esco/skill/data-analysis-2",
                    ],
                    [
                        "http://data.europa.eu/esco/occupation/ds-2",
                        "Data scientist",
                        "optional",
                        "skill/competence",
                        "Gestion de projet",
                        "http://data.europa.eu/esco/skill/project-mgmt-3",
                    ],
                ],
            ).decode("utf-8"),
        )

    return buf.getvalue()


def build_bronze_invalid_values_zip(lang: str = "fr") -> bytes:
    """Build a ZIP where relationType has invalid values."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            f"skills_{lang}.csv",
            _csv_bytes(
                [
                    "preferredLabel",
                    "altLabels",
                    "hiddenLabels",
                    "description",
                    "skillType",
                    "reuseLevel",
                ],
                [
                    [
                        "Test skill",
                        "alt",
                        "",
                        "desc",
                        "skill/competence",
                        "cross-sector",
                    ],
                ],
            ).decode("utf-8"),
        )
        zf.writestr(
            f"occupations_{lang}.csv",
            _csv_bytes(
                ["preferredLabel", "altLabels", "hiddenLabels", "description"],
                [["Test occ", "alt", "", "desc"]],
            ).decode("utf-8"),
        )
        zf.writestr(
            f"occupationSkillRelations_{lang}.csv",
            _csv_bytes(
                ["occupationLabel", "relationType", "skillType", "skillLabel"],
                [
                    ["Test occ", "essential", "skill/competence", "Test skill"],
                    ["Test occ", "INVALID_TYPE", "skill/competence", "Test skill"],
                    ["Test occ", "bonus", "knowledge", "Test skill"],
                ],
            ).decode("utf-8"),
        )

    return buf.getvalue()


def build_bronze_missing_column_zip(lang: str = "fr") -> bytes:
    """Build a ZIP where skills CSV is missing 'description' column."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Missing 'description' column
        zf.writestr(
            f"skills_{lang}.csv",
            _csv_bytes(
                ["preferredLabel", "altLabels", "hiddenLabels", "skillType", "reuseLevel"],
                [["Test", "alt", "", "skill/competence", "cross-sector"]],
            ).decode("utf-8"),
        )
        zf.writestr(
            f"occupations_{lang}.csv",
            _csv_bytes(
                ["preferredLabel", "altLabels", "hiddenLabels", "description"],
                [["Test occ", "alt", "", "desc"]],
            ).decode("utf-8"),
        )
        zf.writestr(
            f"occupationSkillRelations_{lang}.csv",
            _csv_bytes(
                ["occupationLabel", "relationType", "skillType", "skillLabel"],
                [["Test occ", "essential", "skill/competence", "Test skill"]],
            ).decode("utf-8"),
        )

    return buf.getvalue()


if __name__ == "__main__":
    # Write fixture files to disk when run directly
    out = FIXTURES_DIR / "esco_bronze_valid.zip"
    out.write_bytes(build_bronze_valid_zip())
    print(f"Wrote {out}")

    out2 = FIXTURES_DIR / "esco_bronze_invalid_values.zip"
    out2.write_bytes(build_bronze_invalid_values_zip())
    print(f"Wrote {out2}")

    out3 = FIXTURES_DIR / "esco_bronze_missing_col.zip"
    out3.write_bytes(build_bronze_missing_column_zip())
    print(f"Wrote {out3}")
