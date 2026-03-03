"""Manifest schema version management."""

from __future__ import annotations

CURRENT_SCHEMA_VERSION = "1.0.0"

SUPPORTED_SCHEMA_VERSIONS: frozenset[str] = frozenset({"1.0.0"})


def validate_schema_version(version: str) -> bool:
    """Return ``True`` if *version* is a recognised manifest schema version."""
    return version in SUPPORTED_SCHEMA_VERSIONS
