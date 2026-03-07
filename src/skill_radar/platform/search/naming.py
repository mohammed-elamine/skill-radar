"""Index naming — single source of truth for Elasticsearch index names.

Strategy
--------
Stable logical **aliases** plus date-versioned **physical indices** for
safe publish / reindex:

- Physical index:  ``{prefix}-{dataset}-{country}-{date}``
  e.g. ``skillradar-skill-demand-daily-fr-2026.03.06``
- Alias:           ``{prefix}-{dataset}-{country}``
  e.g. ``skillradar-skill-demand-daily-fr``

The alias always points to the latest successfully-published physical
index, enabling atomic swaps and zero-downtime reindexing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig


def build_index_base_name(dataset_suffix: str, config: SearchConfig) -> str:
    """Return the base name (prefix + dataset) without country/date.

    Example: ``skillradar-skill-demand-daily``
    """
    return f"{config.index_prefix}-{dataset_suffix}"


def build_alias_name(dataset_suffix: str, country: str, config: SearchConfig) -> str:
    """Return the stable alias name for a served dataset.

    Example: ``skillradar-skill-demand-daily-fr``
    """
    return f"{config.index_prefix}-{dataset_suffix}-{country}"


def build_index_name(
    dataset_suffix: str,
    ingestion_date: str,
    country: str,
    config: SearchConfig,
) -> str:
    """Return the versioned physical index name.

    Parameters
    ----------
    dataset_suffix:
        Logical index suffix, e.g. ``skill-demand-daily``.
    ingestion_date:
        Partition date in ``YYYY-MM-DD`` format.
    country:
        Country code, e.g. ``fr``.
    config:
        Search configuration.

    Returns
    -------
    str
        e.g. ``skillradar-skill-demand-daily-fr-2026.03.06``
    """
    date_part = ingestion_date.replace("-", ".")
    return f"{config.index_prefix}-{dataset_suffix}-{country}-{date_part}"
