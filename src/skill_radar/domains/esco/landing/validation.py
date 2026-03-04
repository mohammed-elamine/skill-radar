"""ESCO ZIP artifact validator.

All validation logic for ESCO artifacts lives here.
No upload or storage logic — pure validation only.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import zipfile
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from skill_radar.domains.esco.contract.models import EscoContract

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------


@dataclass
class Check:
    """A single validation check result."""

    name: str
    passed: bool
    message: str


@dataclass
class ValidationResult:
    """Aggregate validation outcome."""

    passed: bool = True
    checks: list[Check] = field(default_factory=list)

    def add(self, check: Check) -> None:
        """Record a check and update overall status."""
        self.checks.append(check)
        if not check.passed:
            self.passed = False


# ---------------------------------------------------------------------------
# Individual validators
# ---------------------------------------------------------------------------


def validate_version(version: str, contract: EscoContract) -> Check:
    """Validate *version* against the contract's ``version_pattern``."""
    pattern = contract.version_pattern
    ok = bool(re.match(pattern, version))
    return Check(
        name="version_format",
        passed=ok,
        message=(
            f"Version '{version}' matches pattern '{pattern}'"
            if ok
            else f"Version '{version}' does not match pattern '{pattern}'"
        ),
    )


def validate_language(lang: str, contract: EscoContract) -> Check:
    """Validate that *lang* is declared as a supported language."""
    ok = lang in contract.supported_languages
    return Check(
        name="language_supported",
        passed=ok,
        message=(
            f"Language '{lang}' is supported"
            if ok
            else f"Language '{lang}' not in {contract.supported_languages}"
        ),
    )


# ---------------------------------------------------------------------------
# ZIP-level validation
# ---------------------------------------------------------------------------


def _read_csv_header(zf: zipfile.ZipFile, filename: str) -> list[str] | None:
    """Read the header row of a CSV inside a ZIP.  Returns ``None`` if the
    file is missing from the archive."""
    try:
        with zf.open(filename) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8")
            reader = csv.reader(text)
            return next(reader)
    except KeyError:
        return None
    except StopIteration:
        return []


def validate_zip(
    zip_path: Path,
    lang: str,
    contract: EscoContract,
) -> ValidationResult:
    """Run structural validation checks on an ESCO ZIP artifact.

    Checks performed:

    1. File is a valid ZIP archive.
    2. Each contract-entity CSV is present.
    3. Required columns exist in each CSV header.
    """
    result = ValidationResult()

    # 1. Valid ZIP?
    is_zip = zipfile.is_zipfile(zip_path)
    result.add(
        Check(
            name="valid_zip",
            passed=is_zip,
            message=(
                "File is a valid ZIP archive"
                if is_zip
                else f"{zip_path.name} is not a valid ZIP archive"
            ),
        )
    )
    if not is_zip:
        return result

    # 2-3. Entity CSVs and columns
    with zipfile.ZipFile(zip_path, "r") as zf:
        for entity in contract.entities:
            filename = entity.filename_pattern.replace("{lang}", lang)

            # CSV present?
            present = filename in zf.namelist()
            result.add(
                Check(
                    name=f"file_present:{entity.name}",
                    passed=present,
                    message=(
                        f"{filename} found in archive"
                        if present
                        else f"{filename} missing from archive"
                    ),
                )
            )
            if not present:
                continue

            # Required columns?
            columns = _read_csv_header(zf, filename)
            if columns is None:
                result.add(
                    Check(
                        name=f"columns_readable:{entity.name}",
                        passed=False,
                        message=f"Could not read columns from {filename}",
                    )
                )
                continue

            for col_spec in entity.required_columns:
                col_ok = col_spec.name in columns
                result.add(
                    Check(
                        name=f"column:{entity.name}.{col_spec.name}",
                        passed=col_ok,
                        message=(
                            f"Column '{col_spec.name}' present in {entity.name}"
                            if col_ok
                            else f"Column '{col_spec.name}' missing from {entity.name}"
                        ),
                    )
                )

    return result


# ---------------------------------------------------------------------------
# Full validation pipeline
# ---------------------------------------------------------------------------


def validate_artifact(
    zip_path: Path,
    version: str,
    lang: str,
    contract: EscoContract,
) -> ValidationResult:
    """Full validation pipeline: version + language + ZIP contents.

    Short-circuits if version or language validation fails.
    """
    result = ValidationResult()

    result.add(validate_version(version, contract))
    result.add(validate_language(lang, contract))

    # Short-circuit on pre-condition failures
    if not result.passed:
        return result

    zip_result = validate_zip(zip_path, lang, contract)
    for check in zip_result.checks:
        result.add(check)

    return result
