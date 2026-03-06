"""Tests for LakeLayout Adzuna convenience methods and config loading."""

from __future__ import annotations

import pytest

from skill_radar.config.loader import load_platform_config
from skill_radar.config.models import AdzunaConfig, PlatformSettings
from skill_radar.platform.lake.layout import LakeLayout


class TestAdzunaConfig:
    """AdzunaConfig model defaults and platform integration."""

    def test_defaults(self) -> None:
        cfg = AdzunaConfig()
        assert cfg.base_url == "https://api.adzuna.com/v1/api"
        assert cfg.default_country == "fr"
        assert cfg.results_per_page == 50
        assert cfg.max_pages_per_run == 20
        assert cfg.request_timeout_seconds == 30
        assert cfg.max_retries == 3
        assert cfg.backoff_seconds == 2
        assert cfg.default_preset == "default_fr"

    def test_platform_settings_includes_adzuna(self) -> None:
        settings = PlatformSettings()
        assert hasattr(settings, "adzuna")
        assert isinstance(settings.adzuna, AdzunaConfig)

    def test_load_platform_config_has_adzuna(self) -> None:
        config = load_platform_config()
        assert config.adzuna.default_country == "fr"
        assert config.adzuna.results_per_page == 50


class TestLakeLayoutAdzunaHelpers:
    """LakeLayout.adzuna_* convenience methods produce correct FQNs."""

    @pytest.fixture
    def layout(self) -> LakeLayout:
        return LakeLayout(PlatformSettings())

    def test_bronze_jobs_raw_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.adzuna_bronze_jobs_raw_fqn()
        # Expected: sr.sr_bronze.adzuna_jobs_raw
        assert "bronze" in fqn.lower()
        assert "adzuna" in fqn.lower()
        assert fqn.endswith("_raw")

    def test_bronze_request_log_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.adzuna_bronze_request_log_fqn()
        assert "bronze" in fqn.lower()
        assert "request_log" in fqn.lower()
        assert fqn.endswith("_raw")

    def test_silver_jobs_fqn(self, layout: LakeLayout) -> None:
        fqn = layout.adzuna_silver_jobs_fqn()
        assert "silver" in fqn.lower()
        assert "adzuna" in fqn.lower()
        assert not fqn.endswith("_raw")

    def test_fqns_are_different(self, layout: LakeLayout) -> None:
        bronze = layout.adzuna_bronze_jobs_raw_fqn()
        silver = layout.adzuna_silver_jobs_fqn()
        log = layout.adzuna_bronze_request_log_fqn()
        assert bronze != silver
        assert bronze != log
        assert silver != log
