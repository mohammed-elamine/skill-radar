"""Integration tests for Gold job candidate phrase generator."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from skill_radar.domains.gold.matching.job_candidates import (  # noqa: E402
    build_job_candidates,
)


class TestBuildJobCandidates:
    """Tests for build_job_candidates()."""

    def test_produces_title_and_description_sources(self, sample_jobs_df) -> None:
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=2)
        sources = {r["text_source"] for r in candidates.select("text_source").distinct().collect()}
        assert sources == {"title", "description"}

    def test_unigrams_present(self, sample_jobs_df) -> None:
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=1)
        phrases = {r["candidate_phrase"] for r in candidates.collect()}
        # From job-1 title "senior python developer"
        assert "senior" in phrases
        assert "python" in phrases
        assert "developer" in phrases

    def test_bigrams_present(self, sample_jobs_df) -> None:
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=2)
        phrases = {r["candidate_phrase"] for r in candidates.collect()}
        assert "senior python" in phrases
        assert "python developer" in phrases

    def test_ngram_capped_at_max(self, spark: SparkSession) -> None:
        """No candidate phrase should have more tokens than max_ngram_size."""
        schema = T.StructType(
            [
                T.StructField("job_id", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("ingestion_date", T.StringType()),
                T.StructField("title_normalized", T.StringType()),
                T.StructField("description_normalized", T.StringType()),
            ]
        )
        df = spark.createDataFrame(
            [("j1", "fr", "2024-01-01", "one two three four five six seven", "")],
            schema=schema,
        )
        max_n = 3
        candidates = build_job_candidates(df, max_ngram_size=max_n)
        for row in candidates.collect():
            token_count = len(row["candidate_phrase"].split())
            assert token_count <= max_n, (
                f"Candidate {row['candidate_phrase']!r} has {token_count} tokens, "
                f"expected <= {max_n}"
            )

    def test_job_identity_columns_present(self, sample_jobs_df) -> None:
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=1)
        assert "job_id" in candidates.columns
        assert "country" in candidates.columns
        assert "ingestion_date" in candidates.columns

    def test_deduplicates_within_same_source(self, spark: SparkSession) -> None:
        """Same phrase appearing multiple times in same text should appear once."""
        schema = T.StructType(
            [
                T.StructField("job_id", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("ingestion_date", T.StringType()),
                T.StructField("title_normalized", T.StringType()),
                T.StructField("description_normalized", T.StringType()),
            ]
        )
        df = spark.createDataFrame(
            [("j1", "fr", "2024-01-01", "python python python", "")],
            schema=schema,
        )
        candidates = build_job_candidates(df, max_ngram_size=1)
        python_title = candidates.where(
            (candidates.candidate_phrase == "python") & (candidates.text_source == "title")
        )
        assert python_title.count() == 1

    def test_empty_text_produces_no_candidates(self, spark: SparkSession) -> None:
        schema = T.StructType(
            [
                T.StructField("job_id", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("ingestion_date", T.StringType()),
                T.StructField("title_normalized", T.StringType()),
                T.StructField("description_normalized", T.StringType()),
            ]
        )
        df = spark.createDataFrame(
            [("j1", "fr", "2024-01-01", "", "")],
            schema=schema,
        )
        candidates = build_job_candidates(df, max_ngram_size=3)
        assert candidates.count() == 0

    def test_invalid_max_ngram_raises(self, sample_jobs_df) -> None:
        with pytest.raises(ValueError, match="max_ngram_size must be >= 1"):
            build_job_candidates(sample_jobs_df, max_ngram_size=0)

    def test_candidate_count_bounded(self, spark: SparkSession) -> None:
        """For T tokens with max_n grams, candidates per source ≤ sum(T-i+1 for i in 1..min(T,N))."""
        schema = T.StructType(
            [
                T.StructField("job_id", T.StringType()),
                T.StructField("country", T.StringType()),
                T.StructField("ingestion_date", T.StringType()),
                T.StructField("title_normalized", T.StringType()),
                T.StructField("description_normalized", T.StringType()),
            ]
        )
        # 5 tokens, max_n=3 → 1-grams: 5, 2-grams: 4, 3-grams: 3 = 12
        df = spark.createDataFrame(
            [("j1", "fr", "2024-01-01", "a b c d e", "")],
            schema=schema,
        )
        candidates = build_job_candidates(df, max_ngram_size=3)
        title_only = candidates.where(candidates.text_source == "title")
        count = title_only.count()
        assert count == 12  # 5 + 4 + 3
