"""Tests for Adzuna API error hierarchy."""

from __future__ import annotations

from skill_radar.domains.adzuna.api.errors import (
    AdzunaAuthError,
    AdzunaClientError,
    AdzunaError,
    AdzunaResponseError,
    AdzunaServerError,
)


class TestErrorHierarchy:
    """All Adzuna errors inherit from AdzunaError."""

    def test_auth_error_is_adzuna_error(self) -> None:
        exc = AdzunaAuthError("test")
        assert isinstance(exc, AdzunaError)
        assert isinstance(exc, Exception)

    def test_client_error_is_adzuna_error(self) -> None:
        exc = AdzunaClientError("test")
        assert isinstance(exc, AdzunaError)

    def test_server_error_is_adzuna_error(self) -> None:
        exc = AdzunaServerError("test")
        assert isinstance(exc, AdzunaError)

    def test_response_error_is_adzuna_error(self) -> None:
        exc = AdzunaResponseError("test")
        assert isinstance(exc, AdzunaError)

    def test_message_preserved(self) -> None:
        exc = AdzunaAuthError("Bad credentials")
        assert str(exc) == "Bad credentials"
