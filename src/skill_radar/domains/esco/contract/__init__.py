"""ESCO contract package — dataset contract, models, and loader.

Public API::

    from skill_radar.domains.esco.contract import load_esco_contract
"""

from skill_radar.domains.esco.contract.loader import load_esco_contract
from skill_radar.domains.esco.contract.models import (
    ColumnSpec,
    ContractArtifact,
    ContractEntity,
    ContractSource,
    EscoContract,
)

__all__ = [
    "ColumnSpec",
    "ContractArtifact",
    "ContractEntity",
    "ContractSource",
    "EscoContract",
    "load_esco_contract",
]
