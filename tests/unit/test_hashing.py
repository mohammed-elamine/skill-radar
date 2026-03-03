"""Tests for hashing utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from skill_radar.utils.hashing import sha256_file


class TestSha256File:
    """Verify SHA-256 computation against known digests."""

    def test_known_content(self, tmp_path: Path):
        p = tmp_path / "test.txt"
        p.write_bytes(b"hello world")
        digest = sha256_file(p)
        assert digest == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

    def test_empty_file(self, tmp_path: Path):
        p = tmp_path / "empty.txt"
        p.write_bytes(b"")
        digest = sha256_file(p)
        assert digest == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_binary_content(self, tmp_path: Path):
        p = tmp_path / "binary.bin"
        data = bytes(range(256))
        p.write_bytes(data)
        digest = sha256_file(p)
        assert len(digest) == 64  # Valid hex digest
        assert all(c in "0123456789abcdef" for c in digest)
