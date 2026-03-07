"""Integration tests for Gold label dimension builder."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from skill_radar.domains.gold.matching.label_dimension import (  # noqa: E402
    build_label_dimension,
    max_label_tokens,
)


class TestBuildLabelDimension:
    """Tests for build_label_dimension()."""

    def test_produces_all_label_types(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df)
        types = {r["label_type"] for r in dim.select("label_type").distinct().collect()}
        # At least preferred and alt should exist; hidden depends on data
        assert "preferred" in types
        assert "alt" in types

    def test_preferred_labels_present(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df)
        preferred = dim.where(dim.label_type == "preferred")
        labels = {r["label_normalized"] for r in preferred.collect()}
        assert "python" in labels
        assert "machine learning" in labels
        assert "sql" in labels

    def test_alt_labels_exploded(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df)
        alt = dim.where(dim.label_type == "alt")
        labels = {r["label_normalized"] for r in alt.collect()}
        assert "python programming" in labels
        assert "ml" in labels

    def test_hidden_labels_exploded(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df)
        hidden = dim.where(dim.label_type == "hidden")
        labels = {r["label_normalized"] for r in hidden.collect()}
        assert "snake language" in labels
        assert "structured query language" in labels

    def test_label_normalized_is_lowercase(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df)
        for row in dim.select("label_normalized").collect():
            val = row["label_normalized"]
            assert val == val.lower(), f"Expected lowercase, got {val!r}"

    def test_label_token_count_correct(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df)
        tokens = {
            r["label_normalized"]: r["label_token_count"]
            for r in dim.select("label_normalized", "label_token_count").collect()
        }
        assert tokens["python"] == 1
        assert tokens["machine learning"] == 2
        assert tokens["python programming"] == 2
        assert tokens["structured query language"] == 3

    def test_deduplicates_exact_matches(self, spark) -> None:
        """If preferred and alt yield the same normalized text, both kept (different label_type)."""
        from pyspark.sql import types as T

        # Skill where preferred = "Python" and alt includes "python" (same normalized)
        schema = T.StructType(
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
        dup_skill = spark.createDataFrame(
            [("uri-dup", "uuid-dup", "Python", "skill", "cross-sector", ["python"], None)],
            schema=schema,
        )
        dim = build_label_dimension(dup_skill)
        # Should have 2 rows: one preferred, one alt (different label_type)
        count = dim.count()
        assert count == 2

    def test_empty_skills_returns_empty(self, sample_skills_df) -> None:
        empty = sample_skills_df.limit(0)
        dim = build_label_dimension(empty)
        assert dim.count() == 0

    def test_null_alt_labels_handled(self, spark) -> None:
        """Skills with null alt_labels should not crash."""
        from pyspark.sql import types as T

        schema = T.StructType(
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
        df = spark.createDataFrame(
            [("uri-1", "uuid-1", "Test Skill", "skill", "cross-sector", None, None)],
            schema=schema,
        )
        dim = build_label_dimension(df)
        assert dim.count() == 1  # Only preferred


class TestMaxLabelTokens:
    """Tests for max_label_tokens()."""

    def test_returns_correct_max(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df)
        result = max_label_tokens(dim)
        # "structured query language" = 3 tokens
        assert result == 3

    def test_empty_dimension_returns_1(self, sample_skills_df) -> None:
        dim = build_label_dimension(sample_skills_df).limit(0)
        result = max_label_tokens(dim)
        assert result == 1
