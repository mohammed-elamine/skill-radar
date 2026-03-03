"""Artifact registry — track landed artifacts.

Currently in-memory; will be backed by an Iceberg metadata table in a later
milestone.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ArtifactRegistry:
    """In-memory artifact registry (placeholder for Iceberg backing)."""

    def __init__(self) -> None:
        self._entries: list[dict[str, Any]] = []

    def register(self, manifest: dict[str, Any]) -> None:
        """Record a landed artifact manifest."""
        self._entries.append(manifest)
        logger.info(
            "Registered artifact: %s v%s (%s)",
            manifest.get("dataset"),
            manifest.get("artifact", {}).get("version"),
            manifest.get("artifact", {}).get("language"),
        )

    @property
    def entries(self) -> list[dict[str, Any]]:
        """Return a copy of all registered entries."""
        return list(self._entries)
