"""Integration tests for Gold centralized scoring expression."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from skill_radar.domains.gold.matching.models import SKILL_MATCH_SCORES  # noqa: E402
from skill_radar.domains.gold.matching.scoring import build_skill_score_expr  # noqa: E402


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a minimal local SparkSession."""
    return (
        SparkSession.builder.appName("test_scoring")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture(scope="module", autouse=True)
def stop_spark(spark: SparkSession):
    yield
    spark.stop()


class TestBuildSkillScoreExpr:
    """Tests for build_skill_score_expr()."""

    def test_all_known_combinations_scored(self, spark: SparkSession) -> None:
        """Every (label_type, text_source) pair from SKILL_MATCH_SCORES maps to correct score."""
        rows = [(lt, ts, expected) for (lt, ts), expected in SKILL_MATCH_SCORES.items()]
        df = spark.createDataFrame(rows, ["label_type", "text_source", "expected_score"])
        scored = df.withColumn(
            "actual_score",
            build_skill_score_expr("label_type", "text_source"),
        )
        for row in scored.collect():
            assert row["actual_score"] == pytest.approx(row["expected_score"]), (
                f"Mismatch for ({row['label_type']}, {row['text_source']}): "
                f"expected {row['expected_score']}, got {row['actual_score']}"
            )

    def test_unknown_combination_returns_zero(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("unknown_type", "unknown_source")], ["label_type", "text_source"]
        )
        scored = df.withColumn(
            "score",
            build_skill_score_expr("label_type", "text_source"),
        )
        assert scored.collect()[0]["score"] == 0.0

    def test_preferred_title_is_highest(self, spark: SparkSession) -> None:
        """preferred+title should be the maximum score."""
        df = spark.createDataFrame(
            [(lt, ts) for (lt, ts) in SKILL_MATCH_SCORES],
            ["label_type", "text_source"],
        )
        scored = df.withColumn(
            "score",
            build_skill_score_expr("label_type", "text_source"),
        )
        max_row = scored.orderBy(F.col("score").desc()).first()
        assert max_row["label_type"] == "preferred"
        assert max_row["text_source"] == "title"

    def test_title_scores_higher_than_description(self) -> None:
        """For the same label_type, title should score >= description."""
        for label_type in ("preferred", "alt", "hidden"):
            title_score = SKILL_MATCH_SCORES.get((label_type, "title"), 0.0)
            desc_score = SKILL_MATCH_SCORES.get((label_type, "description"), 0.0)
            assert title_score >= desc_score, (
                f"{label_type}: title score {title_score} < description score {desc_score}"
            )

    def test_works_with_column_objects(self, spark: SparkSession) -> None:
        """build_skill_score_expr should accept Column objects, not just strings."""
        df = spark.createDataFrame(
            [("preferred", "title")],
            ["lt", "ts"],
        )
        scored = df.withColumn(
            "score",
            build_skill_score_expr(F.col("lt"), F.col("ts")),
        )
        expected = SKILL_MATCH_SCORES[("preferred", "title")]
        assert scored.collect()[0]["score"] == pytest.approx(expected)
