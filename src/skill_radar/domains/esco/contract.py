"""Load and validate the ESCO domain contract from YAML."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from .models import EscoContract

_CONTRACT_PATH = Path(__file__).parent / "contract.yaml"


@lru_cache(maxsize=1)
def load_esco_contract(path: Path | None = None) -> EscoContract:
    """Parse ``contract.yaml`` into a strongly-typed :class:`EscoContract`.

    Results are cached for the lifetime of the process.

    Parameters
    ----------
    path:
        Override path to the YAML file (useful in tests).
    """
    target = path or _CONTRACT_PATH
    with Path.open(target) as fh:
        raw = yaml.safe_load(fh)
    return EscoContract(**raw)
