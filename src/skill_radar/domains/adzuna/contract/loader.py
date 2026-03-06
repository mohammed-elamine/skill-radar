"""Load and validate the Adzuna domain contract from YAML."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from .models import AdzunaContract

_CONTRACT_PATH = Path(__file__).parent / "contract.yaml"


@lru_cache(maxsize=1)
def load_adzuna_contract(path: Path | None = None) -> AdzunaContract:
    """Parse ``contract.yaml`` into a strongly-typed :class:`AdzunaContract`.

    Results are cached for the lifetime of the process.

    Parameters
    ----------
    path:
        Override path to the YAML file (useful in tests).
    """
    target = path or _CONTRACT_PATH
    with Path.open(target) as fh:
        raw = yaml.safe_load(fh)
    return AdzunaContract(**raw)
