"""Shared fixtures for Gold integration tests."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a minimal local SparkSession for Gold tests."""
    return (
        SparkSession.builder.appName("test_gold_integration")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture(scope="module", autouse=True)
def stop_spark(spark: SparkSession):
    """Stop Spark after all tests in the module."""
    yield
    spark.stop()


# ---------------------------------------------------------------------------
# Sample ESCO skills DataFrame
# ---------------------------------------------------------------------------

_SKILLS_SCHEMA = T.StructType(
    [
        T.StructField("concept_uri", T.StringType()),
        T.StructField("concept_uri_uuid", T.StringType()),
        T.StructField("preferred_label", T.StringType()),
        T.StructField("skill_type", T.StringType()),
        T.StructField("reuse_level", T.StringType()),
        T.StructField("alt_labels", T.ArrayType(T.StringType())),
        T.StructField("hidden_labels", T.ArrayType(T.StringType())),
    ]
)


@pytest.fixture(scope="module")
def sample_skills_df(spark: SparkSession):
    """Small ESCO skills DataFrame for testing."""
    rows = [
        (
            "http://data.europa.eu/esco/skill/001",
            "uuid-001",
            "Python",
            "skill",
            "cross-sector",
            ["python programming", "Python lang"],
            ["snake language"],
        ),
        (
            "http://data.europa.eu/esco/skill/002",
            "uuid-002",
            "Machine Learning",
            "skill",
            "cross-sector",
            ["ML"],
            None,
        ),
        (
            "http://data.europa.eu/esco/skill/003",
            "uuid-003",
            "SQL",
            "knowledge",
            "sector-specific",
            [],
            ["structured query language"],
        ),
    ]
    return spark.createDataFrame(rows, schema=_SKILLS_SCHEMA)


# ---------------------------------------------------------------------------
# Sample Adzuna jobs DataFrame
# ---------------------------------------------------------------------------

_JOBS_SCHEMA = T.StructType(
    [
        T.StructField("job_id", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("ingestion_date", T.StringType()),
        T.StructField("title_normalized", T.StringType()),
        T.StructField("description_normalized", T.StringType()),
        T.StructField("adref", T.StringType()),
        T.StructField("posted_date", T.StringType()),
        T.StructField("silver_run_id", T.StringType()),
        T.StructField("source_system", T.StringType()),
    ]
)


@pytest.fixture(scope="module")
def sample_jobs_df(spark: SparkSession):
    """Small Adzuna jobs DataFrame for testing."""
    rows = [
        (
            "job-1",
            "fr",
            "2024-01-15",
            "senior python developer",
            "we need expertise in machine learning and sql for data projects",
            "adref-1",
            "2024-01-14",
            "run-abc",
            "adzuna",
        ),
        (
            "job-2",
            "fr",
            "2024-01-15",
            "data engineer",
            "experience with sql and python required",
            "adref-2",
            "2024-01-14",
            "run-abc",
            "adzuna",
        ),
    ]
    return spark.createDataFrame(rows, schema=_JOBS_SCHEMA)
