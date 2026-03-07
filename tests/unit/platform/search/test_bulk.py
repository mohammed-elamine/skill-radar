"""Unit tests for bulk indexing utilities."""

from __future__ import annotations

import json

from skill_radar.platform.search.bulk import _build_bulk_payload, _chunk_list


class TestBuildBulkPayload:
    """Tests for _build_bulk_payload()."""

    def test_single_document(self) -> None:
        docs = [{"doc_id": "abc123", "name": "Python", "count": 42}]
        payload = _build_bulk_payload("test-index", docs)
        lines = payload.strip().split("\n")
        assert len(lines) == 2  # action + source

        action = json.loads(lines[0])
        assert action == {"index": {"_index": "test-index", "_id": "abc123"}}

        source = json.loads(lines[1])
        assert source["doc_id"] == "abc123"
        assert source["name"] == "Python"

    def test_multiple_documents(self) -> None:
        docs = [
            {"doc_id": "id1", "val": 1},
            {"doc_id": "id2", "val": 2},
            {"doc_id": "id3", "val": 3},
        ]
        payload = _build_bulk_payload("idx", docs)
        lines = payload.strip().split("\n")
        assert len(lines) == 6  # 3 action-source pairs

    def test_payload_ends_with_newline(self) -> None:
        docs = [{"doc_id": "x"}]
        payload = _build_bulk_payload("idx", docs)
        assert payload.endswith("\n")

    def test_empty_documents_list(self) -> None:
        payload = _build_bulk_payload("idx", [])
        # Only the trailing newline
        assert payload.strip() == ""

    def test_doc_id_used_as_es_id(self) -> None:
        docs = [{"doc_id": "deterministic_hash_123"}]
        payload = _build_bulk_payload("idx", docs)
        action = json.loads(payload.strip().split("\n")[0])
        assert action["index"]["_id"] == "deterministic_hash_123"


class TestChunkList:
    """Tests for _chunk_list()."""

    def test_exact_division(self) -> None:
        result = _chunk_list([1, 2, 3, 4, 5, 6], 3)
        assert result == [[1, 2, 3], [4, 5, 6]]

    def test_remainder(self) -> None:
        result = _chunk_list([1, 2, 3, 4, 5], 2)
        assert result == [[1, 2], [3, 4], [5]]

    def test_single_chunk(self) -> None:
        result = _chunk_list([1, 2, 3], 10)
        assert result == [[1, 2, 3]]

    def test_empty_list(self) -> None:
        result = _chunk_list([], 5)
        assert result == []

    def test_chunk_size_one(self) -> None:
        result = _chunk_list([1, 2, 3], 1)
        assert result == [[1], [2], [3]]
