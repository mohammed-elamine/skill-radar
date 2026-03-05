"""Generic Iceberg/lakehouse validation checks.

Provides reusable checks for:
- Namespace existence
- Table existence
- Table row counts
- Schema validation
- Partitioning checks
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from skill_radar.platform.validate.models import CheckResult, create_check

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


def check_namespace_exists(
    spark: SparkSession,
    namespace: str,
    *,
    catalog: str = "sr",
) -> CheckResult:
    """Check that an Iceberg namespace exists.

    Parameters
    ----------
    spark:
        Active SparkSession.
    namespace:
        Namespace to check (e.g. "sr_bronze").
    catalog:
        Iceberg catalog name.
    """
    fqn = f"{catalog}.{namespace}"

    try:
        namespaces = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
        namespace_names = [row[0] for row in namespaces]

        if namespace in namespace_names:
            return create_check(
                name=f"bronze.namespace.exists.{namespace}",
                description=f"Namespace {fqn} exists",
                passed=True,
            )
        else:
            return create_check(
                name=f"bronze.namespace.exists.{namespace}",
                description=f"Namespace {fqn} exists",
                passed=False,
                detail=f"Namespace not found (available: {namespace_names[:5]}...)",
            )

    except Exception as exc:
        return create_check(
            name=f"bronze.namespace.exists.{namespace}",
            description=f"Namespace {fqn} exists",
            passed=False,
            detail=str(exc)[:200],
        )


def check_table_exists(
    spark: SparkSession,
    table_fqn: str,
) -> CheckResult:
    """Check that an Iceberg table exists.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name (e.g. "sr.sr_bronze.esco_skills_raw").
    """
    try:
        # Try to describe the table
        spark.sql(f"DESCRIBE TABLE {table_fqn}").collect()
        return create_check(
            name=f"bronze.table.exists.{table_fqn.split('.')[-1]}",
            description=f"Table {table_fqn} exists",
            passed=True,
        )
    except Exception as exc:
        exc_str = str(exc).lower()
        if "table or view not found" in exc_str or "nosuch" in exc_str:
            return create_check(
                name=f"bronze.table.exists.{table_fqn.split('.')[-1]}",
                description=f"Table {table_fqn} exists",
                passed=False,
                detail="Table does not exist",
            )
        return create_check(
            name=f"bronze.table.exists.{table_fqn.split('.')[-1]}",
            description=f"Table {table_fqn} exists",
            passed=False,
            detail=str(exc)[:200],
        )


def check_table_non_empty(
    spark: SparkSession,
    table_fqn: str,
    *,
    min_rows: int = 1,
) -> CheckResult:
    """Check that an Iceberg table has rows.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name.
    min_rows:
        Minimum expected row count.
    """
    try:
        count_row = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_fqn}").collect()
        row_count = count_row[0]["cnt"]

        passed = row_count >= min_rows
        return create_check(
            name=f"bronze.table.non_empty.{table_fqn.split('.')[-1]}",
            description=f"Table {table_fqn} has >= {min_rows} rows",
            passed=passed,
            detail=f"row_count={row_count}",
            metrics={"row_count": row_count},
        )
    except Exception as exc:
        return create_check(
            name=f"bronze.table.non_empty.{table_fqn.split('.')[-1]}",
            description=f"Table {table_fqn} has >= {min_rows} rows",
            passed=False,
            detail=str(exc)[:200],
        )


def check_table_schema_contains(
    spark: SparkSession,
    table_fqn: str,
    required_columns: list[str],
) -> CheckResult:
    """Check that an Iceberg table contains required columns.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name.
    required_columns:
        List of column names that must exist.
    """
    try:
        df = spark.table(table_fqn)
        actual_columns = set(df.columns)

        missing = [col for col in required_columns if col not in actual_columns]

        if missing:
            return create_check(
                name=f"bronze.schema.contains.{table_fqn.split('.')[-1]}",
                description=f"Table {table_fqn} contains required columns",
                passed=False,
                detail=f"Missing: {missing}",
                metrics={"actual_columns": len(actual_columns), "missing_columns": len(missing)},
            )

        return create_check(
            name=f"bronze.schema.contains.{table_fqn.split('.')[-1]}",
            description=f"Table {table_fqn} contains required columns",
            passed=True,
            detail=f"{len(required_columns)} required columns present",
            metrics={"actual_columns": len(actual_columns)},
        )
    except Exception as exc:
        return create_check(
            name=f"bronze.schema.contains.{table_fqn.split('.')[-1]}",
            description=f"Table {table_fqn} contains required columns",
            passed=False,
            detail=str(exc)[:200],
        )


def check_lineage_values(
    spark: SparkSession,
    table_fqn: str,
    *,
    expected_dataset: str | None = None,
    expected_version: str | None = None,
    expected_lang: str | None = None,
) -> CheckResult:
    """Check that lineage columns have expected values.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name.
    expected_dataset:
        Expected value for 'dataset' column.
    expected_version:
        Expected value for 'version' column.
    expected_lang:
        Expected value for 'lang' column.
    """

    try:
        df = spark.table(table_fqn)
        cols = set(df.columns)

        lineage_cols = ["dataset", "version", "lang"]
        missing = [c for c in lineage_cols if c not in cols]
        if missing:
            return create_check(
                name=f"bronze.lineage.values.{table_fqn.split('.')[-1]}",
                description=f"Lineage values correct in {table_fqn}",
                passed=False,
                detail=f"Missing lineage columns: {missing}",
            )

        # Single tiny result set (grouped) instead of 3 distinct collects
        q = f"""
        SELECT dataset, version, lang, COUNT(*) AS cnt
        FROM {table_fqn}
        GROUP BY dataset, version, lang
        """
        rows = spark.sql(q).collect()

        # Validate expectations against grouped keys
        keys = {(r["dataset"], r["version"], r["lang"]) for r in rows}

        issues: list[str] = []
        if expected_dataset or expected_version or expected_lang:

            def match(k: tuple[str, str, str]) -> bool:
                ds, ver, lng = k
                if expected_dataset and ds != expected_dataset:
                    return False
                if expected_version and ver != expected_version:
                    return False
                return not (expected_lang and lng != expected_lang)

            if not any(match(k) for k in keys):
                issues.append(
                    f"observed_keys={list(keys)[:5]} expected=({expected_dataset},{expected_version},{expected_lang})"
                )

        if issues:
            return create_check(
                name=f"bronze.lineage.values.{table_fqn.split('.')[-1]}",
                description=f"Lineage values correct in {table_fqn}",
                passed=False,
                detail="; ".join(issues)[:200],
                metrics={"groups": len(rows)},
            )

        return create_check(
            name=f"bronze.lineage.values.{table_fqn.split('.')[-1]}",
            description=f"Lineage values correct in {table_fqn}",
            passed=True,
            metrics={"groups": len(rows)},
        )

    except Exception as exc:
        return create_check(
            name=f"bronze.lineage.values.{table_fqn.split('.')[-1]}",
            description=f"Lineage values correct in {table_fqn}",
            passed=False,
            detail=str(exc)[:200],
        )


def check_partitioning(
    spark: SparkSession,
    table_fqn: str,
) -> CheckResult:
    """Check table partitioning configuration.

    Currently returns SKIP with info - partitioning is optional.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified table name.
    """
    # Partitioning is optional in MVP - skip but provide info
    try:
        # Try to inspect partitioning via table metadata
        spark.sql(f"DESCRIBE TABLE EXTENDED {table_fqn}").collect()
        return create_check(
            name=f"bronze.partitioning.{table_fqn.split('.')[-1]}",
            description=f"Partitioning check for {table_fqn}",
            passed=False,
            skip=True,
            skip_reason="Partitioning check not implemented in MVP",
        )
    except Exception:
        return create_check(
            name=f"bronze.partitioning.{table_fqn.split('.')[-1]}",
            description=f"Partitioning check for {table_fqn}",
            passed=False,
            skip=True,
            skip_reason="Partitioning check not implemented in MVP",
        )
