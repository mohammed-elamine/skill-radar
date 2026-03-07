"""Data models for search export operations."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BulkIndexResult:
    """Result of a single bulk indexing operation.

    Attributes
    ----------
    index_name:
        Physical Elasticsearch index name.
    total_documents:
        Total documents submitted for indexing.
    success_count:
        Documents successfully indexed.
    error_count:
        Documents that failed to index.
    errors:
        Sample error messages (truncated for safety).
    """

    index_name: str
    total_documents: int = 0
    success_count: int = 0
    error_count: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Return True if all documents were indexed successfully."""
        return self.error_count == 0


@dataclass
class SearchExportResult:
    """Aggregated result of a full search export run.

    Attributes
    ----------
    success:
        Whether all datasets exported successfully.
    datasets_exported:
        List of dataset names that were exported.
    run_id:
        Logging run identifier.
    results:
        Per-dataset bulk indexing results.
    error:
        Error message if the export failed.
    """

    success: bool = True
    datasets_exported: list[str] = field(default_factory=list)
    run_id: str = ""
    results: dict[str, BulkIndexResult] = field(default_factory=dict)
    error: str = ""
