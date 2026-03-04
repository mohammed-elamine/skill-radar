from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DEFAULT_STOP_TERMS = [
    # lieux / bruit fréquent (exemples)
    "paris",
    "france",
    "lyon",
    "marseille",
    # mots trop génériques
    "administration",
    "politique",
    "économie",
]


def filter_bad_matches(
    matches: DataFrame,
    *,
    min_term_len: int = 3,
    stop_terms: list[str] | None = None,
) -> DataFrame:
    """
    Post-processing to reduce false positives.
    Applies basic rules that are cheap + prod-friendly.
    """
    stop_terms = stop_terms or DEFAULT_STOP_TERMS

    m = matches

    # lowercase match_string for filtering
    m = m.withColumn("match_lc", F.lower(F.col("match_string")))

    # basic filters
    m = m.filter(F.length("match_lc") >= min_term_len)
    m = m.filter(~F.col("match_lc").isin(stop_terms))

    return m.drop("match_lc")
