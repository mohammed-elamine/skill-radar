"""Manifest builder — creates structured, versioned intake manifests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings


class ManifestBuilder:
    """Produces a JSON manifest describing a landed artifact.

    The manifest conforms to the platform manifest schema version
    declared in the configuration.

    Parameters
    ----------
    config:
        Platform configuration providing schema version, environment,
        and bucket name.
    """

    def __init__(self, config: PlatformSettings) -> None:
        self._schema_version = config.manifest.schema_version
        self._environment = config.platform.environment
        self._bucket = config.storage.s3.bucket

    def build(
        self,
        *,
        dataset: str,
        artifact_type: str,
        content: str,
        file_type: str,
        version: str,
        language: str,
        checksum: str,
        size_bytes: int,
        storage_key: str,
        provider: str,
        acquisition_method: str,
        provider_url: str,
        validation_status: str,
        validation_checks: list[dict[str, Any]],
        uploaded_by: str = "cli",
    ) -> dict[str, Any]:
        """Build a manifest dict conforming to the platform manifest schema.

        Returns a plain ``dict`` ready for JSON serialisation.
        """
        return {
            "schema_version": self._schema_version,
            "dataset": dataset,
            "artifact": {
                "type": artifact_type,
                "content": content,
                "file_type": file_type,
                "version": version,
                "language": language,
                "checksum": {
                    "algorithm": "sha256",
                    "value": checksum,
                },
                "size_bytes": size_bytes,
                "storage": {
                    "bucket": self._bucket,
                    "key": storage_key,
                },
            },
            "source": {
                "provider": provider,
                "acquisition_method": acquisition_method,
                "provider_url": provider_url,
            },
            "validation": {
                "status": validation_status,
                "checks": validation_checks,
            },
            "audit": {
                "uploaded_at_utc": datetime.now(UTC).isoformat(),
                "uploaded_by": uploaded_by,
                "environment": self._environment,
            },
        }

    @staticmethod
    def to_json(manifest: dict[str, Any]) -> bytes:
        """Serialise a manifest dict to pretty-printed JSON bytes (UTF-8)."""
        return json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
