"""Integration tests for platform Spark transforms.

These tests require PySpark and should be run inside the Spark container.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from skill_radar.platform.spark.transforms import (
    dedupe_by_key,
    extract_uri_uuid,
    normalize_text_col,
    parse_date_col,
    split_newline_labels,
    stable_row_hash,
)

# Skip entire module if pyspark not available
pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder.appName("test_spark_transforms")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


@pytest.fixture(scope="module", autouse=True)
def stop_spark(spark: SparkSession) -> None:
    """Stop Spark after all tests."""
    yield
    spark.stop()


class TestNormalizeTextCol:
    """Tests for normalize_text_col function."""

    def test_trims_whitespace(self, spark: SparkSession) -> None:
        """Test that leading/trailing whitespace is trimmed."""
        df = spark.createDataFrame([(1, "  hello  ")], ["id", "text"])
        result = df.withColumn("normalized", normalize_text_col("text"))
        row = result.collect()[0]
        assert row["normalized"] == "hello"

    def test_collapses_internal_whitespace(self, spark: SparkSession) -> None:
        """Test that multiple spaces collapse to single space."""
        df = spark.createDataFrame([(1, "hello   world")], ["id", "text"])
        result = df.withColumn("normalized", normalize_text_col("text"))
        row = result.collect()[0]
        assert row["normalized"] == "hello world"

    def test_handles_newlines_and_tabs(self, spark: SparkSession) -> None:
        """Test that newlines and tabs are collapsed."""
        df = spark.createDataFrame([(1, "hello\n\tworld")], ["id", "text"])
        result = df.withColumn("normalized", normalize_text_col("text"))
        row = result.collect()[0]
        assert row["normalized"] == "hello world"

    def test_handles_null(self, spark: SparkSession) -> None:
        """Test that null values remain null."""
        df = spark.createDataFrame([(1, None)], ["id", "text"])
        result = df.withColumn("normalized", normalize_text_col("text"))
        row = result.collect()[0]
        assert row["normalized"] is None


class TestSplitNewlineLabels:
    """Tests for split_newline_labels function."""

    def test_splits_by_newline(self, spark: SparkSession) -> None:
        """Test basic newline splitting."""
        df = spark.createDataFrame([(1, "label1\nlabel2\nlabel3")], ["id", "labels"])
        result = df.withColumn("arr", split_newline_labels("labels"))
        row = result.collect()[0]
        assert set(row["arr"]) == {"label1", "label2", "label3"}

    def test_handles_crlf(self, spark: SparkSession) -> None:
        """Test that CRLF is normalized."""
        df = spark.createDataFrame([(1, "label1\r\nlabel2")], ["id", "labels"])
        result = df.withColumn("arr", split_newline_labels("labels"))
        row = result.collect()[0]
        assert set(row["arr"]) == {"label1", "label2"}

    def test_trims_elements(self, spark: SparkSession) -> None:
        """Test that elements are trimmed."""
        df = spark.createDataFrame([(1, "  label1  \n  label2  ")], ["id", "labels"])
        result = df.withColumn("arr", split_newline_labels("labels"))
        row = result.collect()[0]
        assert set(row["arr"]) == {"label1", "label2"}

    def test_filters_empty_strings(self, spark: SparkSession) -> None:
        """Test that empty strings are filtered out."""
        df = spark.createDataFrame([(1, "label1\n\n\nlabel2")], ["id", "labels"])
        result = df.withColumn("arr", split_newline_labels("labels"))
        row = result.collect()[0]
        assert set(row["arr"]) == {"label1", "label2"}

    def test_removes_duplicates(self, spark: SparkSession) -> None:
        """Test that duplicates are removed."""
        df = spark.createDataFrame([(1, "label1\nlabel1\nlabel2")], ["id", "labels"])
        result = df.withColumn("arr", split_newline_labels("labels"))
        row = result.collect()[0]
        assert len(row["arr"]) == 2
        assert set(row["arr"]) == {"label1", "label2"}

    def test_sorts_alphabetically(self, spark: SparkSession) -> None:
        """Test that output is sorted for determinism."""
        df = spark.createDataFrame([(1, "charlie\nalpha\nbravo")], ["id", "labels"])
        result = df.withColumn("arr", split_newline_labels("labels"))
        row = result.collect()[0]
        assert row["arr"] == ["alpha", "bravo", "charlie"]

    def test_handles_null(self, spark: SparkSession) -> None:
        """Test that null returns empty array."""
        df = spark.createDataFrame([(1, None)], ["id", "labels"])
        result = df.withColumn("arr", split_newline_labels("labels"))
        row = result.collect()[0]
        assert row["arr"] == []


class TestExtractUriUuid:
    """Tests for extract_uri_uuid function."""

    def test_extracts_uuid_from_uri(self, spark: SparkSession) -> None:
        """Test UUID extraction from standard ESCO URI."""
        uri = "http://data.europa.eu/esco/skill/abc12345-6789-abcd-ef01-234567890abc"
        df = spark.createDataFrame([(1, uri)], ["id", "uri"])
        result = df.withColumn("uuid", extract_uri_uuid("uri"))
        row = result.collect()[0]
        assert row["uuid"] == "abc12345-6789-abcd-ef01-234567890abc"

    def test_returns_null_for_no_uuid(self, spark: SparkSession) -> None:
        """Test that non-UUID URIs return null."""
        uri = "http://data.europa.eu/esco/skill/not-a-uuid"
        df = spark.createDataFrame([(1, uri)], ["id", "uri"])
        result = df.withColumn("uuid", extract_uri_uuid("uri"))
        row = result.collect()[0]
        assert row["uuid"] is None

    def test_handles_null(self, spark: SparkSession) -> None:
        """Test that null URIs return null."""
        df = spark.createDataFrame([(1, None)], ["id", "uri"])
        result = df.withColumn("uuid", extract_uri_uuid("uri"))
        row = result.collect()[0]
        assert row["uuid"] is None

    def test_case_insensitive(self, spark: SparkSession) -> None:
        """Test that UUIDs are matched case-insensitively."""
        uri = "http://data.europa.eu/esco/skill/ABC12345-6789-ABCD-EF01-234567890ABC"
        df = spark.createDataFrame([(1, uri)], ["id", "uri"])
        result = df.withColumn("uuid", extract_uri_uuid("uri"))
        row = result.collect()[0]
        assert row["uuid"] == "ABC12345-6789-ABCD-EF01-234567890ABC"


class TestParseDateCol:
    """Tests for parse_date_col function."""

    def test_parses_yyyy_mm_dd(self, spark: SparkSession) -> None:
        """Test standard date parsing."""
        df = spark.createDataFrame([(1, "2024-03-15")], ["id", "date_str"])
        result = df.withColumn("date", parse_date_col("date_str"))
        row = result.collect()[0]
        assert str(row["date"]) == "2024-03-15"

    def test_returns_null_for_invalid_date(self, spark: SparkSession) -> None:
        """Test that invalid dates return null."""
        df = spark.createDataFrame([(1, "not-a-date")], ["id", "date_str"])
        result = df.withColumn("date", parse_date_col("date_str"))
        row = result.collect()[0]
        assert row["date"] is None

    def test_handles_null(self, spark: SparkSession) -> None:
        """Test that null values return null."""
        df = spark.createDataFrame([(1, None)], ["id", "date_str"])
        result = df.withColumn("date", parse_date_col("date_str"))
        row = result.collect()[0]
        assert row["date"] is None


class TestStableRowHash:
    """Tests for stable_row_hash function."""

    def test_produces_sha256_hex(self, spark: SparkSession) -> None:
        """Test that output is valid SHA-256 hex digest."""
        df = spark.createDataFrame([(1, "hello", "world")], ["id", "col1", "col2"])
        result = df.withColumn("hash", stable_row_hash(["col1", "col2"]))
        row = result.collect()[0]
        assert len(row["hash"]) == 64  # SHA-256 hex is 64 chars
        assert all(c in "0123456789abcdef" for c in row["hash"])

    def test_deterministic(self, spark: SparkSession) -> None:
        """Test that same input produces same hash."""
        df = spark.createDataFrame([(1, "a", "b"), (2, "a", "b")], ["id", "col1", "col2"])
        result = df.withColumn("hash", stable_row_hash(["col1", "col2"]))
        rows = result.collect()
        assert rows[0]["hash"] == rows[1]["hash"]

    def test_different_values_different_hash(self, spark: SparkSession) -> None:
        """Test that different input produces different hash."""
        df = spark.createDataFrame([(1, "a", "b"), (2, "c", "d")], ["id", "col1", "col2"])
        result = df.withColumn("hash", stable_row_hash(["col1", "col2"]))
        rows = result.collect()
        assert rows[0]["hash"] != rows[1]["hash"]


class TestDedupeByKey:
    """Tests for dedupe_by_key function."""

    def test_basic_dedup(self, spark: SparkSession) -> None:
        """Test basic deduplication by key."""
        data = [
            (1, "key1", "2024-01-01"),
            (2, "key1", "2024-01-02"),  # newer date, should be kept
            (3, "key2", "2024-01-01"),
        ]
        df = spark.createDataFrame(data, ["id", "key", "date"])

        result = dedupe_by_key(
            df,
            key_cols=["key"],
            order_cols=[("date", True)],  # desc
        )

        rows = result.collect()
        assert len(rows) == 2

        key1_row = next(r for r in rows if r["key"] == "key1")
        assert key1_row["date"] == "2024-01-02"

    def test_multiple_key_cols(self, spark: SparkSession) -> None:
        """Test deduplication with multiple key columns."""
        data = [
            ("key1", "v1", "2024-01-01"),
            ("key1", "v1", "2024-01-02"),
            ("key1", "v2", "2024-01-01"),
        ]
        df = spark.createDataFrame(data, ["key", "version", "date"])

        result = dedupe_by_key(
            df,
            key_cols=["key", "version"],
            order_cols=[("date", True)],
        )

        rows = result.collect()
        assert len(rows) == 2

    def test_deterministic_with_tie(self, spark: SparkSession) -> None:
        """Test deduplication is deterministic even with identical order cols."""
        data = [
            ("key1", "2024-01-01", "value_a"),
            ("key1", "2024-01-01", "value_b"),
        ]
        df = spark.createDataFrame(data, ["key", "date", "value"])

        # Run twice and ensure same result
        result1 = dedupe_by_key(
            df,
            key_cols=["key"],
            order_cols=[("date", True)],
        ).collect()

        result2 = dedupe_by_key(
            df,
            key_cols=["key"],
            order_cols=[("date", True)],
        ).collect()

        assert len(result1) == 1
        assert result1[0]["value"] == result2[0]["value"]  # Same row chosen
