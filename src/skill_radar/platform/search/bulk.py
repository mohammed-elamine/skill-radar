"""Bulk indexing with chunking, retries, and structured failure reporting.

Encapsulates the mechanics of:
- Splitting documents into configurable chunks
- Building NDJSON bulk payloads with ``index`` actions
- Retrying transient failures with exponential backoff
- Collecting and reporting per-document errors
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig
    from skill_radar.platform.search.client import SearchClient

from .models import BulkIndexResult

logger = logging.getLogger(__name__)


def _build_bulk_payload(
    index: str,
    documents: list[dict[str, Any]],
) -> str:
    """Build an NDJSON bulk request body for ``index`` operations.

    Each document **must** contain a ``doc_id`` key which is used as the
    Elasticsearch document ``_id`` for deterministic upsert semantics.
    """
    lines: list[str] = []
    for doc in documents:
        doc_id = doc.get("doc_id")
        action = {"index": {"_index": index, "_id": doc_id}}
        lines.append(json.dumps(action, ensure_ascii=False))
        lines.append(json.dumps(doc, ensure_ascii=False))
    # Bulk payload must end with a newline
    lines.append("")
    return "\n".join(lines)


def _chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of ``chunk_size``."""
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def bulk_index(
    client: SearchClient,
    index: str,
    documents: list[dict[str, Any]],
    config: SearchConfig,
) -> BulkIndexResult:
    """Bulk-index documents with chunking and retries.

    Parameters
    ----------
    client:
        Elasticsearch HTTP client.
    index:
        Target physical Elasticsearch index.
    documents:
        List of document dicts to index. Each must have ``doc_id``.
    config:
        Search configuration (chunk size, retries, backoff).

    Returns
    -------
    BulkIndexResult
        Aggregated result with success/error counts and sample errors.
    """
    result = BulkIndexResult(index_name=index, total_documents=len(documents))
    chunks = _chunk_list(documents, config.bulk_chunk_size)

    logger.info(
        "Bulk indexing %d documents into %s (%d chunks of max %d)",
        len(documents),
        index,
        len(chunks),
        config.bulk_chunk_size,
    )

    for chunk_idx, chunk in enumerate(chunks):
        payload = _build_bulk_payload(index, chunk)
        last_error: str | None = None

        for attempt in range(1, config.bulk_max_retries + 1):
            try:
                response = client.bulk(payload)
                if response.get("errors"):
                    # Extract individual item errors
                    for item in response.get("items", []):
                        action_result = item.get("index", {})
                        if action_result.get("error"):
                            result.error_count += 1
                            err_msg = str(action_result["error"])[:200]
                            if len(result.errors) < 10:
                                result.errors.append(err_msg)
                        else:
                            result.success_count += 1
                else:
                    result.success_count += len(chunk)

                logger.debug(
                    "Chunk %d/%d indexed (%d docs)",
                    chunk_idx + 1,
                    len(chunks),
                    len(chunk),
                )
                last_error = None
                break  # success — no retry needed

            except Exception as exc:
                last_error = str(exc)[:200]
                logger.warning(
                    "Bulk chunk %d/%d attempt %d/%d failed: %s",
                    chunk_idx + 1,
                    len(chunks),
                    attempt,
                    config.bulk_max_retries,
                    last_error,
                )
                if attempt < config.bulk_max_retries:
                    time.sleep(config.bulk_retry_backoff_seconds * attempt)

        if last_error is not None:
            # All retries exhausted for this chunk
            result.error_count += len(chunk)
            if len(result.errors) < 10:
                result.errors.append(f"chunk {chunk_idx + 1}: {last_error}")

    logger.info(
        "Bulk indexing complete: %s — %d success, %d errors",
        index,
        result.success_count,
        result.error_count,
    )
    return result
