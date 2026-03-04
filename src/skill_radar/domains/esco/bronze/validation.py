"""Pure validation helpers for ESCO Bronze extraction.

These functions operate on plain Python data structures (lists, dicts)
and do **not** require PySpark.  They are used by the Bronze extraction
pipeline and can be imported in unit tests without a Spark dependency.
"""

from __future__ import annotations

import zipfile
from typing import TYPE_CHECKING

from .errors import BronzeValidationError

if TYPE_CHECKING:
    from pathlib import Path

    from skill_radar.domains.esco.contract.models import ContractEntity

__all__ = ["extract_csv_from_zip", "validate_required_columns"]


def validate_required_columns(
    actual_columns: list[str],
    contract_entity: ContractEntity,
) -> None:
    """Raise if any required column from the contract is absent.

    Parameters
    ----------
    actual_columns:
        Column names as read from the CSV header.
    contract_entity:
        The contract entity definition with ``required_columns``.

    Raises
    ------
    BronzeValidationError
        When one or more required columns are missing.
    """
    actual_set = set(actual_columns)
    missing = [
        spec.name for spec in contract_entity.required_columns if spec.name not in actual_set
    ]
    if missing:
        raise BronzeValidationError(
            f"Entity '{contract_entity.name}' is missing required columns: {missing}"
        )


def extract_csv_from_zip(
    zip_path: Path,
    csv_filename: str,
    target_dir: Path,
) -> Path:
    """Extract a single CSV from a ZIP to *target_dir* and return its path.

    Raises
    ------
    BronzeValidationError
        If the CSV file is not found inside the ZIP.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        if csv_filename not in zf.namelist():
            raise BronzeValidationError(
                f"CSV '{csv_filename}' not found in ZIP. Available: {zf.namelist()}"
            )
        zf.extract(csv_filename, path=target_dir)
    return target_dir / csv_filename
