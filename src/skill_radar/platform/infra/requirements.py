"""Platform requirements — single source of truth for infrastructure.

This module is the authoritative source for:
- Required S3 buckets
- Required Iceberg namespaces

All provisioning (apply) and validation (validate) code MUST use this module
to determine what resources are required. No hardcoded lists elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings


@dataclass(frozen=True)
class IcebergRequirements:
    """Iceberg catalog and namespace requirements.

    Attributes
    ----------
    catalog:
        The Iceberg catalog name (e.g. "sr").
    namespaces:
        List of required namespace names (e.g. ["sr_bronze", "sr_silver", "sr_gold"]).
    """

    catalog: str
    namespaces: tuple[str, ...]

    def namespace_fqn(self, namespace: str) -> str:
        """Return fully-qualified namespace: catalog.namespace."""
        return f"{self.catalog}.{namespace}"

    def all_fqns(self) -> list[str]:
        """Return all fully-qualified namespace names."""
        return [self.namespace_fqn(ns) for ns in self.namespaces]


@dataclass(frozen=True)
class PlatformRequirements:
    """Complete platform infrastructure requirements.

    This is the single source of truth for what resources must exist
    for the Skill Radar platform to function correctly.

    Attributes
    ----------
    buckets:
        List of required S3 bucket names.
    iceberg:
        Iceberg catalog and namespace requirements.
    """

    buckets: tuple[str, ...]
    iceberg: IcebergRequirements

    @property
    def catalog(self) -> str:
        """Shortcut to iceberg catalog name."""
        return self.iceberg.catalog

    @property
    def namespaces(self) -> tuple[str, ...]:
        """Shortcut to iceberg namespaces."""
        return self.iceberg.namespaces


def get_platform_requirements(config: PlatformSettings) -> PlatformRequirements:
    """Compute platform requirements from configuration.

    This function is the ONLY place that determines what infrastructure
    resources are required. It reads from config and returns a frozen
    dataclass that can be used by both provisioning and validation code.

    Parameters
    ----------
    config:
        Platform configuration.

    Returns
    -------
    PlatformRequirements:
        The complete set of required infrastructure resources.

    Examples
    --------
    >>> from skill_radar.config import load_platform_config
    >>> config = load_platform_config()
    >>> reqs = get_platform_requirements(config)
    >>> print(reqs.buckets)
    ('skillradar-lake', 'skillradar-logs')
    >>> print(reqs.namespaces)
    ('sr_bronze', 'sr_silver', 'sr_gold')
    """
    # -------------------------------------------------------------------------
    # Buckets: combine core buckets + any additional from config
    # -------------------------------------------------------------------------
    core_buckets = [
        config.storage.s3.bucket,  # lake bucket
        config.logging.logs_bucket,  # logs bucket
    ]

    # Add any additional buckets from config
    additional_buckets = config.storage.s3.additional_buckets or []

    # Deduplicate while preserving order
    seen: set[str] = set()
    all_buckets: list[str] = []
    for b in core_buckets + list(additional_buckets):
        if b and b not in seen:
            all_buckets.append(b)
            seen.add(b)

    # -------------------------------------------------------------------------
    # Iceberg: catalog + namespaces from config
    # -------------------------------------------------------------------------
    iceberg_cfg = config.storage.iceberg
    catalog = iceberg_cfg.catalog_name

    # Get required namespaces from config
    namespaces = list(iceberg_cfg.required_namespaces)

    iceberg_reqs = IcebergRequirements(
        catalog=catalog,
        namespaces=tuple(namespaces),
    )

    return PlatformRequirements(
        buckets=tuple(all_buckets),
        iceberg=iceberg_reqs,
    )
