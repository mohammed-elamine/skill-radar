"""Storage abstraction package.

Provides S3/MinIO client abstractions and Iceberg catalog integration.
"""

from skill_radar.platform.storage.s3_client import (
    S3Client,
    build_s3_client,
    check_s3_connectivity,
)

__all__ = [
    "S3Client",
    "build_s3_client",
    "check_s3_connectivity",
]
