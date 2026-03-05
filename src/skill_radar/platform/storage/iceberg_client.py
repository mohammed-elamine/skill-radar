"""Iceberg catalog abstraction via Spark SQL.

Provides helpers for namespace/table lifecycle management used by the
Bronze and Silver extraction layers.  All DDL is routed through the
Spark SQL API so that the underlying Iceberg catalog type (Hadoop,
REST, Hive) is transparent.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class IcebergClient:
    """Gateway for Iceberg catalog operations.

    Parameters
    ----------
    spark:
        An active Spark session configured with an Iceberg catalog
        (typically ``sr``).
    catalog:
        Catalog name registered in Spark (default ``sr``).
    """

    def __init__(self, spark: SparkSession, *, catalog: str = "sr") -> None:
        self._spark = spark
        self._catalog = catalog

    # -- namespace --------------------------------------------------------

    def ensure_namespace(self, namespace: str) -> None:
        """Create the namespace if it does not already exist."""
        fqn = f"{self._catalog}.{namespace}"
        self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fqn}")
        logger.info("Ensured namespace: %s", fqn)

    # -- table queries ----------------------------------------------------

    def table_exists(self, namespace: str, table: str) -> bool:
        """Return ``True`` if the table exists in the catalog."""
        fqn = f"{self._catalog}.{namespace}.{table}"
        return self._spark.catalog.tableExists(fqn)

    def table_columns(self, namespace: str, table: str) -> list[str]:
        """Return the list of column names for an existing table."""
        fqn = f"{self._catalog}.{namespace}.{table}"
        return [c.name for c in self._spark.catalog.listColumns(fqn)]

    # -- table mutations --------------------------------------------------

    def create_table(
        self,
        namespace: str,
        table: str,
        df: DataFrame,
        *,
        partition_by: tuple[str, ...] = (),
        properties: dict[str, str] | None = None,
    ) -> None:
        """Create an Iceberg table from a DataFrame schema.

        Parameters
        ----------
        namespace / table:
            Target table coordinates.
        df:
            DataFrame whose schema becomes the table schema.
            Data is written as part of creation.
        partition_by:
            Columns to partition by.
        properties:
            Iceberg table properties (e.g. format-version, compression).
        """
        fqn = f"{self._catalog}.{namespace}.{table}"
        props = {"format-version": "2", "write.format.default": "parquet"}
        if properties:
            props.update(properties)

        writer = df.writeTo(fqn).using("iceberg")
        for k, v in props.items():
            writer = writer.tableProperty(k, v)
        if partition_by:
            writer = writer.partitionedBy(*partition_by)  # type: ignore[arg-type]
        writer.create()
        logger.info("Created Iceberg table: %s (partitioned by %s)", fqn, partition_by)

    def overwrite_partitions(
        self,
        namespace: str,
        table: str,
        df: DataFrame,
    ) -> None:
        """Overwrite matching partitions in an existing Iceberg table."""
        fqn = f"{self._catalog}.{namespace}.{table}"
        df.writeTo(fqn).overwritePartitions()
        logger.info("Overwrote partitions in: %s", fqn)

    def write_idempotent(
        self,
        namespace: str,
        table: str,
        df: DataFrame,
        *,
        partition_by: tuple[str, ...] = ("version", "lang"),
        properties: dict[str, str] | None = None,
    ) -> None:
        """Create-or-overwrite with partition-level idempotency.

        If the table does not exist, create it with the given schema and
        partition spec.  If it exists, overwrite matching partitions.
        """
        if self.table_exists(namespace, table):
            self.overwrite_partitions(namespace, table, df)
        else:
            self.create_table(
                namespace,
                table,
                df,
                partition_by=partition_by,
                properties=properties,
            )
