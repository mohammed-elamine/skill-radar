# SPDX-License-Identifier: Apache-2.0
"""Spark / PySpark compatibility utilities.

This module provides helpers to detect and diagnose version mismatches
between the PySpark pip package and the Spark JVM runtime. Such mismatches
produce cryptic errors like ``'JavaPackage' object is not callable``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_jvm_version(spark: SparkSession) -> str:
    """Get the Spark JVM runtime version from the session.

    Parameters
    ----------
    spark:
        Active SparkSession.

    Returns
    -------
    str:
        Version string like '3.5.7'.
    """
    return spark.version


def get_pyspark_version() -> str:
    """Get the installed PySpark package version.

    Returns
    -------
    str:
        Version string like '3.5.7'.
    """
    import pyspark

    return pyspark.__version__


def parse_major_minor(version: str) -> tuple[int, int]:
    """Parse major.minor from a version string.

    Parameters
    ----------
    version:
        Version string like '3.5.7' or '4.0.0'.

    Returns
    -------
    tuple[int, int]:
        (major, minor) as integers.

    Raises
    ------
    ValueError:
        If version string cannot be parsed.
    """
    parts = version.split(".")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse version: {version}")
    return int(parts[0]), int(parts[1])


def check_spark_pyspark_compatible(
    spark: SparkSession,
) -> tuple[bool, str, str, str]:
    """Check if PySpark and Spark JVM versions are compatible.

    Versions are considered compatible if their major.minor match.
    For example, PySpark 3.5.7 is compatible with Spark JVM 3.5.x.

    Parameters
    ----------
    spark:
        Active SparkSession.

    Returns
    -------
    tuple[bool, str, str, str]:
        (is_compatible, pyspark_version, spark_version, message)
    """
    pyspark_ver = get_pyspark_version()
    spark_ver = get_spark_jvm_version(spark)

    try:
        py_major, py_minor = parse_major_minor(pyspark_ver)
        jvm_major, jvm_minor = parse_major_minor(spark_ver)
    except ValueError as e:
        return (
            False,
            pyspark_ver,
            spark_ver,
            f"Cannot parse versions: {e}",
        )

    compatible = (py_major == jvm_major) and (py_minor == jvm_minor)

    if compatible:
        msg = f"PySpark {pyspark_ver} matches Spark JVM {spark_ver}"
        logger.debug(msg)
    else:
        msg = (
            f"Version mismatch: PySpark {pyspark_ver} vs Spark JVM {spark_ver}. "
            f"This causes 'JavaPackage' object is not callable errors. "
            f"Fix: pin pyspark=={jvm_major}.{jvm_minor}.* in pyproject.toml"
        )
        logger.error(msg)

    return compatible, pyspark_ver, spark_ver, msg


def assert_spark_pyspark_compatible(spark: SparkSession) -> None:
    """Assert PySpark and Spark JVM versions are compatible.

    Raises a clear RuntimeError if versions don't match.

    Parameters
    ----------
    spark:
        Active SparkSession.

    Raises
    ------
    RuntimeError:
        If major.minor versions don't match.
    """
    compatible, _pyspark_ver, _spark_ver, msg = check_spark_pyspark_compatible(spark)
    if not compatible:
        raise RuntimeError(msg)
