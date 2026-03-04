"""Bronze column renaming, normalisation, and lineage injection.

This module contains the **contract-driven** schema-mapping logic that
transforms raw ESCO CSV columns into Bronze-layer Iceberg columns.

The mapping rules come from ``contract.yaml`` per entity:

* ``renames`` — explicit vendor → bronze column name.
* ``newline_fields`` — vendor columns with ``\\n``-separated terms.
* ``derived_newline_helpers`` — whether to materialise ``_norm`` / ``_count``.

For any unmapped vendor column the fallback is pure ``to_snake_case``.

This module has **zero** Spark dependency so it can be unit-tested without
a SparkSession.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skill_radar.domains.esco.contract.models import ContractEntity

# ---------------------------------------------------------------------------
# camelCase → snake_case utility
# ---------------------------------------------------------------------------

_CAMEL_RE1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_RE2 = re.compile(r"([a-z0-9])([A-Z])")


def to_snake_case(name: str) -> str:
    """Convert a camelCase or PascalCase column name to snake_case.

    >>> to_snake_case("preferredLabel")
    'preferred_label'
    >>> to_snake_case("altLabels")
    'alt_labels'
    >>> to_snake_case("occupationSkillRelations")
    'occupation_skill_relations'
    """
    s = _CAMEL_RE1.sub(r"\1_\2", name)
    return _CAMEL_RE2.sub(r"\1_\2", s).lower()


# ---------------------------------------------------------------------------
# Contract-driven rename mapping
# ---------------------------------------------------------------------------


def rename_mapping(contract_entity: ContractEntity, vendor_columns: list[str]) -> dict[str, str]:
    """Build the full vendor → bronze rename mapping from the contract.

    Uses the entity's explicit ``renames`` dict first, then falls back to
    :func:`to_snake_case` for unmapped vendor columns.

    Returns
    -------
    dict[str, str]
        ``{vendor_col: bronze_col}``
    """
    renames = contract_entity.renames
    return {c: renames.get(c, to_snake_case(c)) for c in vendor_columns}


# ---------------------------------------------------------------------------
# Newline fields — derived from contract
# ---------------------------------------------------------------------------


def newline_raw_fields(contract_entity: ContractEntity) -> tuple[str, ...]:
    """Return the *bronze* column names for newline-separated fields.

    The contract declares vendor column names in ``newline_fields``
    (e.g. ``["altLabels", "hiddenLabels"]``).  This function maps them to
    their renamed bronze names via the entity's ``renames`` dict.
    """
    if not contract_entity.newline_fields:
        return ()
    renames = contract_entity.renames
    return tuple(renames.get(f, to_snake_case(f)) for f in contract_entity.newline_fields)


def norm_column_name(raw_col: str) -> str:
    """``alt_labels_raw`` → ``alt_labels_norm``."""
    return raw_col.replace("_raw", "_norm")


def count_column_name(raw_col: str) -> str:
    """``alt_labels_raw`` → ``alt_labels_count``."""
    return raw_col.replace("_raw", "_count")


# ---------------------------------------------------------------------------
# Lineage columns (appended to every Bronze table)
# ---------------------------------------------------------------------------

LINEAGE_COLUMNS = (
    "dataset",
    "entity",
    "version",
    "lang",
    "source_zip_key",
    "manifest_key",
    "artifact_sha256",
    "ingested_at_utc",
    "run_id",
)
