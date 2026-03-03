"""Tests for lake path layout builder."""

from __future__ import annotations

from skill_radar.config.models import PlatformSettings
from skill_radar.platform.lake.enums import LakeLayer
from skill_radar.platform.lake.layout import LakeLayout


class TestLakeLayout:
    """Verify path construction follows lake conventions."""

    def setup_method(self):
        self.config = PlatformSettings()
        self.layout = LakeLayout(self.config)

    def test_landing_artifact_path(self):
        path = self.layout.landing_artifact(
            domain="taxonomy", source="esco", version="v1.2.1", lang="fr"
        )
        assert path == "data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr"

    def test_landing_artifact_different_lang(self):
        path = self.layout.landing_artifact(
            domain="taxonomy", source="esco", version="v2.0.0", lang="en"
        )
        assert path == "data/landing/taxonomy/esco/artifact/version=v2.0.0/lang=en"

    def test_layer_prefix_bronze(self):
        path = self.layout.layer_prefix(LakeLayer.BRONZE, "taxonomy", "esco", "skills")
        assert path == "data/bronze/taxonomy/esco/skills"

    def test_layer_prefix_silver(self):
        path = self.layout.layer_prefix(LakeLayer.SILVER, "taxonomy", "esco", "skills")
        assert path == "data/silver/taxonomy/esco/skills"

    def test_layer_prefix_gold(self):
        path = self.layout.layer_prefix(LakeLayer.GOLD, "jobs", "adzuna", "postings")
        assert path == "data/gold/jobs/adzuna/postings"
