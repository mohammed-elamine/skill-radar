"""Bronze Iceberg write helpers.

Thin wrappers around :class:`~skill_radar.platform.storage.iceberg_client.IcebergClient`
with Bronze-specific naming conventions.  All table naming goes through
:class:`~skill_radar.platform.lake.layout.LakeLayout`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


logger = logging.getLogger(__name__)

# ── Bronze namespace / table naming ────────────────────────────────────────

BRONZE_NAMESPACE = "sr_bronze"


def ensure_bronze_namespace(spark: SparkSession, *, catalog: str = "sr") -> None:
    """Create the Bronze Iceberg namespace if it does not exist."""
    fqn = f"{catalog}.{BRONZE_NAMESPACE}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fqn}")
    logger.info("Ensured Iceberg namespace: %s", fqn)


def write_bronze_table(
    df: DataFrame,
    table_fqn: str,
    spark: SparkSession,
) -> None:
    """Write a DataFrame to an Iceberg Bronze table with partition overwrite.

    If the table does not exist, it is created with Iceberg format-version=2
    partitioned by ``(version, lang)``.  If it exists, overwrite matching
    ``(version, lang)`` partitions for idempotency.
    """
    if not spark.catalog.tableExists(table_fqn):
        logger.info("Creating Iceberg table: %s", table_fqn)
        (
            df.writeTo(table_fqn)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .partitionedBy("version", "lang")
            .create()
        )
    else:
        logger.info("Overwriting partitions in: %s", table_fqn)
        df.writeTo(table_fqn).overwritePartitions()
