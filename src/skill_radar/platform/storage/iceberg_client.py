"""Iceberg abstraction (placeholder for Spark Bronze phase).

Actual implementation will be provided during the Bronze extraction
phase when Spark is involved.  This stub establishes the interface contract.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class IcebergClient:
    """Wrapper for Iceberg catalog operations.

    .. note::

        This is a **placeholder**.  Methods raise ``NotImplementedError``
        and will be implemented during the data-plane (Bronze/Silver) phase.
    """

    def table_exists(self, namespace: str, table: str) -> bool:
        """Check whether an Iceberg table exists."""
        raise NotImplementedError("IcebergClient is a placeholder for the data-plane phase.")

    def register_table(
        self,
        namespace: str,
        table: str,
        location: str,
    ) -> None:
        """Register a new Iceberg table."""
        raise NotImplementedError

    def append_metadata_row(
        self,
        namespace: str,
        table: str,
        row: dict,
    ) -> None:
        """Append a metadata row to an Iceberg table."""
        raise NotImplementedError
