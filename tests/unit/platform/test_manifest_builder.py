"""Tests for manifest builder."""

from __future__ import annotations

import json

from skill_radar.config.models import PlatformSettings
from skill_radar.platform.manifest.builder import ManifestBuilder


class TestManifestBuilder:
    """Verify manifest structure and serialisation."""

    def setup_method(self):
        self.config = PlatformSettings()
        self.builder = ManifestBuilder(self.config)

    def _build_sample(self) -> dict:
        return self.builder.build(
            dataset="esco",
            artifact_type="zip",
            content="classification",
            file_type="csv",
            version="v1.2.1",
            language="fr",
            checksum="abc123def456",
            size_bytes=1024,
            storage_key="data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/esco.zip",
            provider="ESCO",
            acquisition_method="manual_download",
            provider_url="https://esco.ec.europa.eu",
            validation_status="passed",
            validation_checks=[{"name": "test", "passed": True, "message": "ok"}],
        )

    def test_schema_version(self):
        manifest = self._build_sample()
        assert manifest["schema_version"] == "1.0.0"

    def test_dataset(self):
        manifest = self._build_sample()
        assert manifest["dataset"] == "esco"

    def test_artifact_section(self):
        manifest = self._build_sample()
        artifact = manifest["artifact"]
        assert artifact["type"] == "zip"
        assert artifact["content"] == "classification"
        assert artifact["file_type"] == "csv"
        assert artifact["version"] == "v1.2.1"
        assert artifact["language"] == "fr"
        assert artifact["checksum"]["algorithm"] == "sha256"
        assert artifact["checksum"]["value"] == "abc123def456"
        assert artifact["size_bytes"] == 1024
        assert artifact["storage"]["bucket"] == "skillradar-lake"

    def test_source_section(self):
        manifest = self._build_sample()
        source = manifest["source"]
        assert source["provider"] == "ESCO"
        assert source["acquisition_method"] == "manual_download"
        assert source["provider_url"] == "https://esco.ec.europa.eu"

    def test_validation_section(self):
        manifest = self._build_sample()
        assert manifest["validation"]["status"] == "passed"
        assert len(manifest["validation"]["checks"]) == 1

    def test_audit_section(self):
        manifest = self._build_sample()
        audit = manifest["audit"]
        assert "uploaded_at_utc" in audit
        assert audit["uploaded_by"] == "cli"
        assert audit["environment"] == "local"

    def test_to_json_returns_bytes(self):
        manifest = self._build_sample()
        data = ManifestBuilder.to_json(manifest)
        assert isinstance(data, bytes)
        parsed = json.loads(data)
        assert parsed["dataset"] == "esco"
