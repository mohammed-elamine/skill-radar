"""Adzuna domain contract — dataset contract, models, and loader.

Public API::

    from skill_radar.domains.adzuna.contract import load_adzuna_contract
"""

from skill_radar.domains.adzuna.contract.loader import load_adzuna_contract
from skill_radar.domains.adzuna.contract.models import (
    AdzunaContract,
    ExtractionDefaults,
    ExtractionPreset,
    FieldExpectations,
    SourceMetadata,
    SupportedCountry,
)

__all__ = [
    "AdzunaContract",
    "ExtractionDefaults",
    "ExtractionPreset",
    "FieldExpectations",
    "SourceMetadata",
    "SupportedCountry",
    "load_adzuna_contract",
]
