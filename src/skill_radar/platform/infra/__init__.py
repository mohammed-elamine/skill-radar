"""Infrastructure provisioning module.

Provides idempotent apply operations for:
- S3 buckets
- Iceberg namespaces
"""

from skill_radar.platform.infra.apply import (
    apply_infra,
    ensure_buckets,
    ensure_namespaces,
)
from skill_radar.platform.infra.requirements import (
    IcebergRequirements,
    PlatformRequirements,
    get_platform_requirements,
)

__all__ = [
    "IcebergRequirements",
    "PlatformRequirements",
    "apply_infra",
    "ensure_buckets",
    "ensure_namespaces",
    "get_platform_requirements",
]
