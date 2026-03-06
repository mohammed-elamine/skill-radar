"""Spark DataFrame transformation utilities.

Provides reusable column-level transformations and DataFrame helpers:
- :func:`normalize_text_col`: trim + collapse whitespace
- :func:`split_newline_labels`: split newline-separated labels to array
- :func:`extract_uri_uuid`: extract UUID from URI suffix
- :func:`dedupe_by_key`: deterministic deduplication via windowing
- :func:`stable_row_hash`: compute deterministic row hash
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════════════════
# Column-level transformations (return Column expressions)
# ═══════════════════════════════════════════════════════════════════════════════


def normalize_text_col(col: Column | str) -> Column:
    """Normalize a text column: trim and collapse internal whitespace.

    Applies:
    1. Trim leading/trailing whitespace
    2. Replace sequences of whitespace with single space
    3. Handle null values (returns null)

    Parameters
    ----------
    col:
        Column expression or column name.

    Returns
    -------
    Column
        Normalized text column expression.

    Examples
    --------
    >>> df.withColumn("clean_label", normalize_text_col("label"))
    """
    c = F.col(col) if isinstance(col, str) else col
    # First trim, then collapse multiple whitespace chars to single space
    return F.regexp_replace(F.trim(c), r"\s+", " ")


def split_newline_labels(col: Column | str) -> Column:
    """Split newline-separated labels into a deduplicated, sorted array.

    Applies:
    1. Normalize \\r\\n and \\r to \\n
    2. Split by \\n
    3. Trim each element
    4. Filter out empty strings
    5. Remove duplicates (array_distinct)
    6. Sort alphabetically for stable ordering

    Parameters
    ----------
    col:
        Column expression or column name containing newline-separated labels.

    Returns
    -------
    Column
        Array<string> column expression with cleaned, deduplicated, sorted labels.
        Returns empty array for null or empty input.

    Examples
    --------
    >>> df.withColumn("alt_labels", split_newline_labels("alt_labels_raw"))
    """
    c = F.col(col) if isinstance(col, str) else col

    # Step 1: Normalize CRLF and CR to LF
    normalized = F.regexp_replace(F.regexp_replace(c, r"\r\n", "\n"), r"\r", "\n")

    # Step 2: Split by newline
    split_arr = F.split(normalized, "\n")

    # Step 3+4: Trim each element and filter out empty strings
    trimmed = F.transform(split_arr, lambda x: F.trim(x))
    filtered = F.filter(trimmed, lambda x: x != F.lit(""))

    # Step 5: Remove duplicates
    distinct = F.array_distinct(filtered)

    # Step 6: Sort for stable ordering
    sorted_arr = F.array_sort(distinct)

    # Handle null input gracefully - return empty array
    return F.coalesce(sorted_arr, F.array())


def extract_uri_uuid(col: Column | str) -> Column:
    """Extract UUID from the end of a URI.

    Extracts a UUID (36-character pattern: 8-4-4-4-12 hex digits) from
    the end of a URI string. Returns null if no valid UUID found.

    Parameters
    ----------
    col:
        Column expression or column name containing URI.

    Returns
    -------
    Column
        UUID string if found, null otherwise.

    Examples
    --------
    >>> df.withColumn("concept_uuid", extract_uri_uuid("concept_uri"))
    # For "http://data.europa.eu/esco/skill/abc12345-6789-abcd-ef01-234567890abc"
    # Returns "abc12345-6789-abcd-ef01-234567890abc"
    """
    c = F.col(col) if isinstance(col, str) else col

    # UUID pattern: 8-4-4-4-12 hex digits at the end of string after a /
    # Using regexp_extract with group 1 to capture just the UUID
    uuid_pattern = (
        r"/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$"
    )

    extracted = F.regexp_extract(c, uuid_pattern, 1)

    # Return null if empty string (no match)
    return F.when(extracted == F.lit(""), F.lit(None).cast("string")).otherwise(extracted)


def stable_row_hash(cols: list[str]) -> Column:
    """Compute a deterministic SHA-256 hash of multiple columns.

    Creates a stable hash suitable for deterministic deduplication.
    Concatenates column values with a delimiter and computes SHA-256.

    Parameters
    ----------
    cols:
        List of column names to include in the hash.

    Returns
    -------
    Column
        SHA-256 hex digest string.

    Examples
    --------
    >>> df.withColumn("row_hash", stable_row_hash(["col1", "col2", "col3"]))
    """
    # Use concat_ws with || delimiter for deterministic ordering
    # Cast all columns to string and handle nulls
    concat_expr = F.concat_ws(
        "||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in cols],
    )
    return F.sha2(concat_expr, 256)


# ═══════════════════════════════════════════════════════════════════════════════
# DataFrame-level transformations
# ═══════════════════════════════════════════════════════════════════════════════


def dedupe_by_key(
    df: DataFrame,
    key_cols: list[str],
    order_cols: list[tuple[str, bool]],
    *,
    hash_cols: list[str] | None = None,
) -> DataFrame:
    """Deduplicate DataFrame deterministically by key columns.

    Uses window functions to select a single row per key based on ordered
    columns. Final tie-breaker is a stable row hash for full determinism.

    Parameters
    ----------
    df:
        Input DataFrame with potential duplicates.
    key_cols:
        Columns that define uniqueness (e.g., ["concept_uri", "version", "lang"]).
    order_cols:
        List of (column_name, descending) tuples for ordering.
        E.g., [("modified_date", True), ("ingested_at_utc", True)]
        True means descending, False means ascending.
    hash_cols:
        Columns to include in tie-breaker hash. If None, uses all DataFrame columns.

    Returns
    -------
    DataFrame
        Deduplicated DataFrame with exactly one row per unique key.

    Examples
    --------
    >>> dedupe_by_key(
    ...     df,
    ...     key_cols=["concept_uri", "version", "lang"],
    ...     order_cols=[("modified_date", True), ("ingested_at_utc", True)],
    ... )
    """
    # Build the stable hash column for tie-breaking
    hash_col_list = hash_cols if hash_cols is not None else df.columns
    df_with_hash = df.withColumn("__row_hash__", stable_row_hash(hash_col_list))

    # Build ordering expression
    order_exprs = []
    for col_name, desc in order_cols:
        if desc:
            order_exprs.append(F.col(col_name).desc_nulls_last())
        else:
            order_exprs.append(F.col(col_name).asc_nulls_last())

    # Add final tie-breaker: descending hash (deterministic)
    order_exprs.append(F.col("__row_hash__").desc())

    # Window specification
    window = Window.partitionBy(*key_cols).orderBy(*order_exprs)

    # Add row number and filter to first row
    df_ranked = df_with_hash.withColumn("__row_num__", F.row_number().over(window))
    df_deduped = df_ranked.where(F.col("__row_num__") == 1)

    # Drop helper columns
    return df_deduped.drop("__row_hash__", "__row_num__")


def parse_date_col(col: Column | str, date_format: str = "yyyy-MM-dd") -> Column:
    """Parse a string column to date type.

    Parameters
    ----------
    col:
        Column expression or column name containing date string.
    date_format:
        Date format pattern (default: "yyyy-MM-dd").

    Returns
    -------
    Column
        Date column expression. Returns null if parsing fails.

    Examples
    --------
    >>> df.withColumn("modified_date", parse_date_col("modified_date_raw"))
    """
    c = F.col(col) if isinstance(col, str) else col

    # Extract leading ISO-8601 yyyy-MM-dd prefix if present to avoid
    # Spark 3.x strict parsing errors on full datetime strings like
    # "2023-11-30T15:53:37.136Z" when using to_date with a date-only
    # pattern. If no such prefix exists, fall back to the original value.
    iso_prefix = F.regexp_extract(c, r"^(\d{4}-\d{2}-\d{2})", 1)
    date_str = F.when(iso_prefix != F.lit(""), iso_prefix).otherwise(c)

    return F.to_date(date_str, date_format)
