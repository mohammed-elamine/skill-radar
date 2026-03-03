"""Cryptographic hashing utilities."""

from __future__ import annotations

import hashlib
from pathlib import Path

_CHUNK_SIZE = 1 << 16  # 64 KiB


def sha256_file(path: Path) -> str:
    """Compute the SHA-256 hex-digest of a file.

    Reads the file in 64 KiB chunks to handle arbitrarily large files
    without excessive memory usage.

    Parameters
    ----------
    path:
        Path to the file to hash.

    Returns
    -------
    str
        Lowercase hex digest.
    """
    h = hashlib.sha256()
    with Path.open(path, "rb") as fh:
        while True:
            chunk = fh.read(_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
