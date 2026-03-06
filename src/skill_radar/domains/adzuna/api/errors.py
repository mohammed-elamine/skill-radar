"""Domain-specific exceptions for the Adzuna API client."""

from __future__ import annotations


class AdzunaError(Exception):
    """Base exception for all Adzuna domain errors."""


class AdzunaAuthError(AdzunaError):
    """Raised when Adzuna credentials are invalid or missing."""


class AdzunaClientError(AdzunaError):
    """Raised for 4xx client/request errors (non-auth)."""


class AdzunaServerError(AdzunaError):
    """Raised for transient 5xx server/network errors after retries are exhausted."""


class AdzunaResponseError(AdzunaError):
    """Raised when the API response is malformed or unparseable."""
