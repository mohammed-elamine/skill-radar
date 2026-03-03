"""Adzuna API settings (legacy module, preserved for backward compatibility)."""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Adzuna API credentials loaded from environment variables."""

    def __init__(self, adzuna_app_id: str, adzuna_app_key: str) -> None:
        self.adzuna_app_id = adzuna_app_id
        self.adzuna_app_key = adzuna_app_key

    @classmethod
    def from_env(cls) -> Settings:
        missing = [k for k in ("ADZUNA_APP_ID", "ADZUNA_APP_KEY") if not os.getenv(k)]
        if missing:
            raise KeyError(f"Missing env vars: {', '.join(missing)}")
        return cls(
            adzuna_app_id=os.environ["ADZUNA_APP_ID"],
            adzuna_app_key=os.environ["ADZUNA_APP_KEY"],
        )


def get_settings() -> Settings:
    """Lazy settings accessor (no import-time env dependency)."""
    return Settings.from_env()
