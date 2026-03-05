"""Runtime context detection and endpoint resolution.

This package provides:
- Runtime context detection (host vs docker)
- Endpoint resolution based on context
- Fast health-check client builders
- Spark/PySpark version compatibility checks
"""

from skill_radar.platform.runtime.context import (
    RuntimeContext,
    detect_runtime_context,
    get_runtime_context,
)
from skill_radar.platform.runtime.endpoints import resolve_s3_endpoint
from skill_radar.platform.runtime.spark_compat import (
    assert_spark_pyspark_compatible,
    check_spark_pyspark_compatible,
)

__all__ = [
    "RuntimeContext",
    "assert_spark_pyspark_compatible",
    "check_spark_pyspark_compatible",
    "detect_runtime_context",
    "get_runtime_context",
    "resolve_s3_endpoint",
]
