from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from skill_radar.text_pipeline.contracts import TextPipelinePaths
from skill_radar.text_pipeline.run import run


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("skill-radar-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _write_esco(spark: SparkSession, path: Path) -> None:
    schema = T.StructType(
        [
            T.StructField("skill_id", T.StringType(), False),
            T.StructField("skill_label", T.StringType(), False),
            T.StructField("aliases", T.ArrayType(T.StringType()), False),
        ]
    )
    rows = [
        ("S1", "Python", ["python", "py"]),
        ("S2", "Apache Spark", ["spark", "pyspark"]),
        ("S3", "Java", ["java"]),
        ("S4", "JavaScript", ["javascript", "js"]),
    ]
    spark.createDataFrame(rows, schema=schema).write.mode("overwrite").parquet(str(path))


def _write_jobs(spark: SparkSession, path: Path) -> None:
    schema = T.StructType(
        [
            T.StructField("job_id", T.StringType(), False),
            T.StructField("dt", T.StringType(), False),
            T.StructField("title", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
        ]
    )
    rows = [
        ("J1", "2026-02-25", "PySpark dev", "Use Spark and Python."),
        ("J2", "2026-02-25", "Java dev", "Java 17 backend."),
        ("J3", "2026-02-25", "Frontend", "JavaScript (JS) and React."),
    ]
    spark.createDataFrame(rows, schema=schema).write.mode("overwrite").partitionBy("dt").parquet(
        str(path)
    )


def test_text_pipeline_smoke(tmp_path: Path, spark: SparkSession):
    # Arrange: write synthetic inputs
    jobs_in = tmp_path / "formatted" / "jobs" / "partitioned"
    esco_in = tmp_path / "curated" / "reference" / "esco_skills"
    matches_out = tmp_path / "curated" / "text" / "job_skill_matches"
    kpis_out = tmp_path / "curated" / "kpis" / "skill_kpis_daily"

    _write_jobs(spark, jobs_in)
    _write_esco(spark, esco_in)

    # Act: run pipeline (it will create its own Spark session internally)
    paths = TextPipelinePaths(
        jobs_input=jobs_in,
        esco_input=esco_in,
        matches_output=matches_out,
        kpis_output=kpis_out,
    )
    run(paths, spark=spark)

    # Assert: read outputs + invariants
    matches = spark.read.parquet(str(matches_out))
    kpis = spark.read.parquet(str(kpis_out))

    assert matches.count() > 0
    assert kpis.count() > 0

    # Uniqueness
    dups = matches.groupBy("job_id", "dt", "skill_id").count().filter(F.col("count") > 1).count()
    assert dups == 0

    # share bounds
    assert kpis.filter((F.col("share") < 0) | (F.col("share") > 1)).count() == 0

    # expected skill presence (basic)
    skills = {r["skill_label"] for r in matches.select("skill_label").distinct().collect()}
    assert "Python" in skills
    assert "Apache Spark" in skills
    assert "Java" in skills
    assert "JavaScript" in skills
