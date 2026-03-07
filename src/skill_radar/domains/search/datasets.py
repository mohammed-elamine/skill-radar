"""Served dataset registry — single source of truth for the search stage.

Each entry declares:
- A human-readable ``name``
- The Elasticsearch index suffix (from config)
- The method to retrieve the Gold Iceberg table FQN from ``LakeLayout``
- The document builder function reference (by name in :mod:`.documents`)
- The mapping key (in :mod:`skill_radar.platform.search.mappings`)

This module is the **only** place where served datasets are enumerated.
Adding or removing a dataset from the serving stage requires a single
change here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig


@dataclass(frozen=True)
class ServedDataset:
    """A dataset served from Gold to Elasticsearch.

    Attributes
    ----------
    name:
        Human-readable name used in CLI and logs.
    config_key:
        Key in ``SearchIndicesConfig`` to resolve the index suffix.
    fqn_method:
        Name of the ``LakeLayout`` method returning the Gold table FQN.
    document_builder:
        Name of the function in :mod:`.documents` that builds ES docs.
    mapping_key:
        Key into ``MAPPINGS`` in :mod:`skill_radar.platform.search.mappings`.
    partition_keys:
        Partition column names used for scoped reads.
    """

    name: str
    config_key: str
    fqn_method: str
    document_builder: str
    mapping_key: str
    partition_keys: tuple[str, ...] = ("ingestion_date", "country")


# ═══════════════════════════════════════════════════════════════════════════
# Dataset registry
# ═══════════════════════════════════════════════════════════════════════════

SERVED_DATASETS: list[ServedDataset] = [
    ServedDataset(
        name="skill_demand_daily",
        config_key="skill_demand_daily",
        fqn_method="gold_skill_demand_daily_fqn",
        document_builder="build_skill_demand_documents",
        mapping_key="skill-demand-daily",
    ),
    ServedDataset(
        name="salary_by_skill_daily",
        config_key="salary_by_skill_daily",
        fqn_method="gold_salary_by_skill_daily_fqn",
        document_builder="build_salary_by_skill_documents",
        mapping_key="salary-by-skill-daily",
    ),
    ServedDataset(
        name="occupation_skill_graph",
        config_key="occupation_skill_graph",
        fqn_method="gold_occupation_skill_graph_fqn",
        document_builder="build_occupation_skill_graph_documents",
        mapping_key="occupation-skill-graph",
    ),
    ServedDataset(
        name="job_skill_matches",
        config_key="job_skill_matches",
        fqn_method="gold_job_skill_matches_fqn",
        document_builder="build_job_skill_matches_documents",
        mapping_key="job-skill-matches",
    ),
    ServedDataset(
        name="job_occupation_matches",
        config_key="job_occupation_matches",
        fqn_method="gold_job_occupation_matches_fqn",
        document_builder="build_job_occupation_matches_documents",
        mapping_key="job-occupation-matches",
    ),
]

# Primary serve set (compact, high-value, dashboard-friendly)
PRIMARY_DATASETS: list[str] = [
    "skill_demand_daily",
    "salary_by_skill_daily",
    "occupation_skill_graph",
]

# All available dataset names
ALL_DATASET_NAMES: list[str] = [d.name for d in SERVED_DATASETS]


def get_served_dataset(name: str) -> ServedDataset:
    """Look up a served dataset by name.

    Raises
    ------
    KeyError
        If the name is not in the registry.
    """
    for ds in SERVED_DATASETS:
        if ds.name == name:
            return ds
    raise KeyError(f"Unknown served dataset: '{name}'. Available: {ALL_DATASET_NAMES}")


def resolve_index_suffix(dataset: ServedDataset, config: SearchConfig) -> str:
    """Resolve the Elasticsearch index suffix from config.

    Parameters
    ----------
    dataset:
        Served dataset descriptor.
    config:
        Search configuration.

    Returns
    -------
    str
        Index suffix, e.g. ``skill-demand-daily``.
    """
    suffix: str = getattr(config.indices, dataset.config_key)
    return suffix
