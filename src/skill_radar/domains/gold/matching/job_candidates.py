"""Job candidate phrase generator — bounded n-gram extraction from job text.

Produces candidate phrases from Adzuna Silver job ``title_normalized`` and
``description_normalized`` columns.  Each candidate is a contiguous n-gram
(1 to *N* tokens) that can be equi-joined against the ESCO label dimension.
"""

from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .models import CANDIDATE_MAX_NGRAM_SIZE

logger = logging.getLogger(__name__)


# ── Public API ────────────────────────────────────────────────────────────


def build_job_candidates(
    jobs_df: DataFrame,
    *,
    max_ngram_size: int = CANDIDATE_MAX_NGRAM_SIZE,
) -> DataFrame:
    """Generate bounded n-gram candidate phrases from job text.

    Parameters
    ----------
    jobs_df:
        Adzuna Silver jobs with at minimum: ``job_id``, ``country``,
        ``ingestion_date``, ``title_normalized``,
        ``description_normalized``.
    max_ngram_size:
        Maximum n-gram token count. Defaults to
        :data:`CANDIDATE_MAX_NGRAM_SIZE`.

    Returns
    -------
    DataFrame
        Columns: ``job_id``, ``country``, ``ingestion_date``,
        ``candidate_phrase``, ``text_source``.
        One row per unique (job_id, candidate_phrase, text_source).
    """
    if max_ngram_size < 1:
        raise ValueError(f"max_ngram_size must be >= 1, got {max_ngram_size}")

    logger.info("Building job candidates with max_ngram_size=%d", max_ngram_size)

    # Common job identity columns
    id_cols = ["job_id", "country", "ingestion_date"]

    # ── Title candidates ──────────────────────────────────────────────────
    title_candidates = _extract_ngrams(
        jobs_df.select(*id_cols, F.col("title_normalized").alias("_text")),
        max_n=max_ngram_size,
    ).withColumn("text_source", F.lit("title"))

    # ── Description candidates ────────────────────────────────────────────
    desc_candidates = _extract_ngrams(
        jobs_df.select(*id_cols, F.col("description_normalized").alias("_text")),
        max_n=max_ngram_size,
    ).withColumn("text_source", F.lit("description"))

    # ── Union + deduplicate ───────────────────────────────────────────────
    candidates = title_candidates.unionByName(desc_candidates)
    candidates = candidates.dropDuplicates(["job_id", "candidate_phrase", "text_source"])

    return candidates


# ── Internal helpers ──────────────────────────────────────────────────────


def _extract_ngrams(
    df: DataFrame,
    *,
    max_n: int,
) -> DataFrame:
    """Extract all contiguous n-grams (1..max_n) from ``_text`` column.

    Uses Spark-native array operations:
    1. Split ``_text`` into a token array.
    2. For each *n* from 1 to ``max_n``, generate start positions
       and slice *n* tokens, then concat with space.
    3. Union all *n*-gram DataFrames.

    Parameters
    ----------
    df:
        Must contain ``_text`` plus any identity columns to carry through.
    max_n:
        Maximum gram size.

    Returns
    -------
    DataFrame
        Original identity columns plus ``candidate_phrase``.
    """
    # Tokenize
    tokenized = df.where(F.col("_text").isNotNull() & (F.col("_text") != F.lit(""))).withColumn(
        "_tokens", F.split(F.col("_text"), r"\s+")
    )

    tokenized = tokenized.withColumn("_num_tokens", F.size("_tokens"))

    # Identity columns (everything except _text, _tokens, _num_tokens)
    id_cols = [c for c in df.columns if c != "_text"]

    ngram_dfs: list[DataFrame] = []
    for n in range(1, max_n + 1):
        # Generate valid starting positions for this n-gram size.
        # positions: 0, 1, ..., num_tokens - n
        # We use sequence(0, num_tokens - n) to produce the array,
        # then explode it.
        gram_df = (
            tokenized.where(F.col("_num_tokens") >= n)
            .withColumn("_positions", F.sequence(F.lit(0), F.col("_num_tokens") - F.lit(n)))
            .select(
                *id_cols,
                "_tokens",
                F.explode("_positions").alias("_pos"),
            )
            .withColumn(
                "candidate_phrase",
                F.concat_ws(" ", F.slice(F.col("_tokens"), F.col("_pos") + 1, n)),
            )
            .select(*id_cols, "candidate_phrase")
        )
        ngram_dfs.append(gram_df)

    if not ngram_dfs:
        # Edge case: max_n < 1 already guarded above, but be safe
        return tokenized.select(*id_cols).withColumn("candidate_phrase", F.lit(None).cast("string"))

    result = reduce(DataFrame.unionByName, ngram_dfs)

    # Filter out empty/null candidates
    result = result.where(
        F.col("candidate_phrase").isNotNull() & (F.col("candidate_phrase") != F.lit(""))
    )

    return result
