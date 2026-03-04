"""Bronze extraction error types."""

from __future__ import annotations


class BronzeValidationError(Exception):
    """Raised when a Bronze extraction pre-condition is violated."""
