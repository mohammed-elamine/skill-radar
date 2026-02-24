import pytest

from skill_radar.config import Settings


def test_settings_from_env_success(monkeypatch):
    monkeypatch.setenv("ADZUNA_APP_ID", "test_id")
    monkeypatch.setenv("ADZUNA_APP_KEY", "test_key")

    settings = Settings.from_env()

    assert settings.adzuna_app_id == "test_id"
    assert settings.adzuna_app_key == "test_key"


def test_settings_from_env_missing(monkeypatch):
    monkeypatch.delenv("ADZUNA_APP_ID", raising=False)
    monkeypatch.delenv("ADZUNA_APP_KEY", raising=False)

    with pytest.raises(KeyError):
        Settings.from_env()
