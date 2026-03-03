"""Enumerations for lake layer and domain concepts."""

from __future__ import annotations

from enum import StrEnum


class LakeLayer(StrEnum):
    """Logical layers of the data lakehouse."""

    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Domain(StrEnum):
    """Data domains."""

    TAXONOMY = "taxonomy"
    JOBS = "jobs"


class Source(StrEnum):
    """Data sources."""

    ESCO = "esco"
    ADZUNA = "adzuna"
