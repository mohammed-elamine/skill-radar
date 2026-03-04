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

    # -- New centralised key methods (Task 3) --------------------------------

    def test_landing_zip_key(self):
        key = self.layout.landing_zip_key(
            domain="taxonomy", source="esco", version="v1.2.1", lang="fr"
        )
        assert key == "data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/esco.zip"

    def test_landing_manifest_key(self):
        key = self.layout.landing_manifest_key(
            domain="taxonomy", source="esco", version="v1.2.1", lang="fr"
        )
        assert key == "data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/manifest.json"

    def test_bronze_staging_prefix(self):
        prefix = self.layout.bronze_staging_prefix(
            domain="taxonomy", source="esco", version="v1.2.0", lang="fr", run_id="abc123"
        )
        assert prefix == "data/bronze/taxonomy/esco/staging/version=v1.2.0/lang=fr/run_id=abc123"

    def test_iceberg_table_fqn(self):
        fqn = self.layout.iceberg_table_fqn("bronze", "esco", "skills")
        assert fqn == "sr.sr_bronze.esco_skills_raw"

    def test_iceberg_table_fqn_custom_catalog(self):
        fqn = self.layout.iceberg_table_fqn("bronze", "esco", "skills", catalog="custom")
        assert fqn == "custom.sr_bronze.esco_skills_raw"

    def test_iceberg_namespace(self):
        ns = self.layout.iceberg_namespace("bronze")
        assert ns == "sr.sr_bronze"

    def test_iceberg_namespace_custom_catalog(self):
        ns = self.layout.iceberg_namespace("silver", catalog="custom")
        assert ns == "custom.sr_silver"
