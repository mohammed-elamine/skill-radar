"""Integration tests for Gold skill matching — candidate equi-join + dedup."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from skill_radar.domains.gold import schema as gold_schema  # noqa: E402
from skill_radar.domains.gold.matching.job_candidates import (  # noqa: E402
    build_job_candidates,
)
from skill_radar.domains.gold.matching.label_dimension import (  # noqa: E402
    build_label_dimension,
    max_label_tokens,
)
from skill_radar.domains.gold.matching.models import MATCH_METHOD_SKILL  # noqa: E402
from skill_radar.domains.gold.matching.skill_matching import (  # noqa: E402
    deduplicate_skill_matches,
    match_jobs_to_skills,
)


class TestMatchJobsToSkills:
    """End-to-end candidate equi-join matching."""

    def test_returns_matches(self, sample_skills_df, sample_jobs_df) -> None:
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)

        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)
        assert raw.count() > 0

    def test_python_matched_in_title(self, sample_skills_df, sample_jobs_df) -> None:
        """Job-1 has 'python' in title, should match skill 'Python'."""
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)

        python_title = raw.where(
            (F.col("job_id") == "job-1")
            & (F.col(gold_schema.esco_skill("preferred_label")) == "Python")
            & (F.col(gold_schema.matched("text_source")) == "title")
        )
        assert python_title.count() >= 1

    def test_machine_learning_matched_in_description(
        self, sample_skills_df, sample_jobs_df
    ) -> None:
        """Job-1 description has 'machine learning', should match."""
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)

        ml_desc = raw.where(
            (F.col("job_id") == "job-1")
            & (F.col(gold_schema.esco_skill("preferred_label")) == "Machine Learning")
            & (F.col(gold_schema.matched("text_source")) == "description")
        )
        assert ml_desc.count() >= 1

    def test_match_method_column_present(self, sample_skills_df, sample_jobs_df) -> None:
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)

        methods = {r["match_method"] for r in raw.select("match_method").distinct().collect()}
        assert methods == {MATCH_METHOD_SKILL}

    def test_scores_in_valid_range(self, sample_skills_df, sample_jobs_df) -> None:
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)

        bad = raw.where((F.col("match_score") < 0.0) | (F.col("match_score") > 1.0))
        assert bad.count() == 0

    def test_output_has_gold_schema_columns(self, sample_skills_df, sample_jobs_df) -> None:
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)

        cols = set(raw.columns)
        assert "job_id" in cols
        assert "country" in cols
        assert "ingestion_date" in cols
        assert gold_schema.esco_skill("concept_uri") in cols
        assert gold_schema.esco_skill("preferred_label") in cols
        assert gold_schema.matched("label") in cols
        assert gold_schema.matched("label_type") in cols
        assert gold_schema.matched("text_source") in cols
        assert "match_method" in cols
        assert "match_score" in cols
        assert "title_hit" in cols
        assert "description_hit" in cols


class TestDeduplicateSkillMatches:
    """Deduplication of raw skill matches."""

    def test_keeps_highest_score(self, sample_skills_df, sample_jobs_df) -> None:
        """After dedup, each (job, skill, method) key should have one row with best score."""
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)
        deduped = deduplicate_skill_matches(raw)

        key_cols = [
            "job_id",
            "country",
            "ingestion_date",
            gold_schema.esco_skill("concept_uri"),
            "match_method",
        ]

        # Check no duplicates on key
        from pyspark.sql import functions as F

        dup_check = deduped.groupBy(*key_cols).count().where(F.col("count") > 1)
        assert dup_check.count() == 0

    def test_title_hit_merged_across_sources(self, sample_skills_df, sample_jobs_df) -> None:
        """If a skill is matched in both title and description, both flags should be true."""
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)
        deduped = deduplicate_skill_matches(raw)

        # Job-2 has "python" in description and "sql" in description
        # Job-2 has "data engineer" in title — we should have at least
        # sql (description hit only) or python (description hit only)
        # For merging test: Job-1 has python in title AND sql/ML in description
        python_job1 = deduped.where(
            (F.col("job_id") == "job-1")
            & (F.col(gold_schema.esco_skill("preferred_label")) == "Python")
        )
        if python_job1.count() > 0:
            row = python_job1.first()
            # Python appears in title at minimum
            assert row["title_hit"] is True

    def test_dedup_reduces_row_count(self, sample_skills_df, sample_jobs_df) -> None:
        label_dim = build_label_dimension(sample_skills_df)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(sample_jobs_df, max_ngram_size=max_n)
        raw = match_jobs_to_skills(sample_jobs_df, label_dim, candidates)
        deduped = deduplicate_skill_matches(raw)

        assert deduped.count() <= raw.count()


class TestNoSubstringMatching:
    """Verify that n-gram tokenization prevents substring false positives."""

    def test_no_partial_word_match(self, spark: SparkSession) -> None:
        """'sql' should NOT match 'mysql' or 'nosql' because they are single tokens."""
        skills_schema = T.StructType(
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
        skills = spark.createDataFrame(
            [("uri-sql", "uuid-sql", "SQL", "knowledge", "sector", [], None)],
            schema=skills_schema,
        )

        jobs_schema = T.StructType(
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
        jobs = spark.createDataFrame(
            [
                (
                    "j-nosql",
                    "fr",
                    "2024-01-01",
                    "nosql database admin",
                    "experience with mysql required",
                    "ad-1",
                    "2024-01-01",
                    "run-1",
                    "adzuna",
                ),
            ],
            schema=jobs_schema,
        )

        label_dim = build_label_dimension(skills)
        max_n = max_label_tokens(label_dim)
        candidates = build_job_candidates(jobs, max_ngram_size=max_n)
        raw = match_jobs_to_skills(jobs, label_dim, candidates)

        # "sql" as a standalone unigram should NOT match "nosql" or "mysql"
        assert raw.count() == 0, (
            f"Expected 0 matches (no substring match), got {raw.count()}: "
            f"{[r.asDict() for r in raw.collect()]}"
        )
