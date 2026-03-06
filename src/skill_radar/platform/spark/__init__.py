"""Platform Spark utilities.

Provides reusable Spark SQL functions and DataFrame helpers for all layers.
"""

from .transforms import (
    dedupe_by_key,
    extract_uri_uuid,
    normalize_text_col,
    parse_date_col,
    split_newline_labels,
    stable_row_hash,
)

__all__ = [
    "dedupe_by_key",
    "extract_uri_uuid",
    "normalize_text_col",
    "parse_date_col",
    "split_newline_labels",
    "stable_row_hash",
]
