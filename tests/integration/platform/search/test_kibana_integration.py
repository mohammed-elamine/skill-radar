"""Integration tests for Kibana dashboard asset management.

These tests require a running Kibana instance.
Run with: make search-up && uv run pytest tests/integration/platform/search/test_kibana_integration.py -v
"""

from __future__ import annotations

import os

import pytest

from skill_radar.config.models import PlatformSettings, SearchConfig

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def _kibana_available() -> bool:
    """Check if Kibana is reachable."""
    cfg = SearchConfig()
    kibana_url = os.environ.get("SKILLRADAR_SEARCH_KIBANA_URL", cfg.kibana_url)

    from skill_radar.platform.search.kibana import is_kibana_reachable

    try:
        return is_kibana_reachable(kibana_url, timeout=5)
    except Exception:
        return False


skip_if_no_kibana = pytest.mark.skipif(
    not _kibana_available(),
    reason="Kibana not available. Run `make search-up` first.",
)


@pytest.fixture(scope="module")
def kibana_url() -> str:
    cfg = SearchConfig()
    return os.environ.get("SKILLRADAR_SEARCH_KIBANA_URL", cfg.kibana_url)


@pytest.fixture(scope="module")
def search_config() -> SearchConfig:
    return SearchConfig()


@pytest.fixture(scope="module")
def platform_config() -> PlatformSettings:
    return PlatformSettings()


# ─────────────────────────────────────────────────────────────────────────
# NDJSON generation (no Kibana required)
# ─────────────────────────────────────────────────────────────────────────


class TestNdjsonGeneration:
    """Test complete asset generation without a running Kibana."""

    def test_generate_assets_no_errors(self, search_config: SearchConfig) -> None:
        from skill_radar.platform.search.kibana_assets import generate_all_assets

        objects = generate_all_assets(search_config)
        assert len(objects) > 0

    def test_ndjson_can_be_parsed(self, search_config: SearchConfig) -> None:
        import json

        from skill_radar.platform.search.kibana_assets import (
            generate_all_assets,
            objects_to_ndjson,
        )

        objects = generate_all_assets(search_config)
        ndjson = objects_to_ndjson(objects)
        lines = [line for line in ndjson.strip().split("\n") if line]
        assert len(lines) == len(objects)
        for line in lines:
            parsed = json.loads(line)
            assert "type" in parsed
            assert "id" in parsed


# ─────────────────────────────────────────────────────────────────────────
# Kibana import (requires running Kibana)
# ─────────────────────────────────────────────────────────────────────────


@skip_if_no_kibana
class TestKibanaImport:
    """Test import of generated assets into a running Kibana instance."""

    def test_import_all_assets(
        self,
        search_config: SearchConfig,
        kibana_url: str,
    ) -> None:
        from skill_radar.platform.search.kibana import import_saved_objects
        from skill_radar.platform.search.kibana_assets import (
            generate_all_assets,
            objects_to_ndjson,
        )

        objects = generate_all_assets(search_config)
        ndjson = objects_to_ndjson(objects)

        result = import_saved_objects(
            ndjson,
            kibana_url=kibana_url,
            overwrite=True,
            timeout=30,
        )
        assert result.get("success") is True
        assert result.get("successCount", 0) == len(objects)

    def test_import_is_idempotent(
        self,
        search_config: SearchConfig,
        kibana_url: str,
    ) -> None:
        from skill_radar.platform.search.kibana import import_saved_objects
        from skill_radar.platform.search.kibana_assets import (
            generate_all_assets,
            objects_to_ndjson,
        )

        objects = generate_all_assets(search_config)
        ndjson = objects_to_ndjson(objects)

        # Import twice — should succeed both times with overwrite=True
        r1 = import_saved_objects(ndjson, kibana_url=kibana_url, overwrite=True)
        r2 = import_saved_objects(ndjson, kibana_url=kibana_url, overwrite=True)
        assert r1.get("success") is True
        assert r2.get("success") is True

    def test_dashboards_visible_after_import(
        self,
        search_config: SearchConfig,
        kibana_url: str,
    ) -> None:
        from skill_radar.platform.search.kibana import (
            find_saved_objects,
            import_saved_objects,
        )
        from skill_radar.platform.search.kibana_assets import (
            EXPECTED_DASHBOARDS,
            generate_all_assets,
            objects_to_ndjson,
        )

        objects = generate_all_assets(search_config)
        ndjson = objects_to_ndjson(objects)
        import_saved_objects(ndjson, kibana_url=kibana_url, overwrite=True)

        dashboards = find_saved_objects(kibana_url, "dashboard")
        dashboard_titles = {d["attributes"]["title"] for d in dashboards}

        for expected in EXPECTED_DASHBOARDS:
            assert expected["title"] in dashboard_titles, (
                f"Dashboard '{expected['title']}' not found in Kibana"
            )

    def test_data_views_visible_after_import(
        self,
        search_config: SearchConfig,
        kibana_url: str,
    ) -> None:
        from skill_radar.platform.search.kibana import (
            get_saved_object,
            import_saved_objects,
        )
        from skill_radar.platform.search.kibana_assets import (
            generate_all_assets,
            get_expected_data_view_ids,
            objects_to_ndjson,
        )

        objects = generate_all_assets(search_config)
        ndjson = objects_to_ndjson(objects)
        import_saved_objects(ndjson, kibana_url=kibana_url, overwrite=True)

        for dv_id in get_expected_data_view_ids(search_config):
            obj = get_saved_object(kibana_url, "index-pattern", dv_id)
            assert obj is not None, f"Data view '{dv_id}' not found in Kibana"


# ─────────────────────────────────────────────────────────────────────────
# Orchestrator (requires running Kibana)
# ─────────────────────────────────────────────────────────────────────────


@skip_if_no_kibana
class TestKibanaOrchestrator:
    """Test the high-level orchestrator workflow."""

    def test_apply_kibana_assets(self, platform_config: PlatformSettings) -> None:
        from skill_radar.domains.search.kibana_orchestrator import apply_kibana_assets

        result = apply_kibana_assets(platform_config, overwrite=True)
        assert result.success
        assert result.applied is True
        assert result.total_objects > 0

    def test_dry_run_does_not_apply(self, platform_config: PlatformSettings) -> None:
        from skill_radar.domains.search.kibana_orchestrator import apply_kibana_assets

        result = apply_kibana_assets(platform_config, dry_run=True)
        assert result.success
        assert result.applied is False
        assert result.total_objects > 0

    def test_bootstrap_is_idempotent(self, platform_config: PlatformSettings) -> None:
        from skill_radar.domains.search.kibana_orchestrator import bootstrap_kibana

        r1 = bootstrap_kibana(platform_config)
        r2 = bootstrap_kibana(platform_config)
        assert r1.success
        assert r2.success
