"""Storage-layer exceptions."""

from __future__ import annotations


class StorageError(Exception):
    """Base exception for all storage operations."""


class ObjectAlreadyExistsError(StorageError):
    """Raised when an upload would overwrite an existing, immutable object."""


class UploadError(StorageError):
    """Raised on a failed upload attempt."""


class ObjectNotFoundError(StorageError):
    """Raised when an expected object does not exist in storage."""
