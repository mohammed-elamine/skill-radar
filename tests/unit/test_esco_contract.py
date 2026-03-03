"""Tests for the ESCO domain contract loader."""

from __future__ import annotations

from skill_radar.domains.esco.contract import load_esco_contract
from skill_radar.domains.esco.models import EscoContract


class TestEscoContract:
    """Verify contract.yaml is loaded and parsed correctly."""

    def test_loads_as_esco_contract(self):
        contract = load_esco_contract()
        assert isinstance(contract, EscoContract)

    def test_dataset_name(self):
        contract = load_esco_contract()
        assert contract.dataset == "esco"

    def test_artifact_type(self):
        contract = load_esco_contract()
        assert contract.artifact.type == "zip"
        assert contract.artifact.content == "classification"
        assert contract.artifact.file_type == "csv"

    def test_supported_languages(self):
        contract = load_esco_contract()
        assert "fr" in contract.supported_languages
        assert "en" in contract.supported_languages

    def test_version_pattern(self):
        contract = load_esco_contract()
        assert contract.version_pattern.startswith("^v")

    def test_entities_count(self):
        contract = load_esco_contract()
        assert len(contract.entities) == 3

    def test_skills_entity_columns(self):
        contract = load_esco_contract()
        skills = next(e for e in contract.entities if e.name == "skills")
        col_names = [c.name for c in skills.required_columns]
        assert "preferredLabel" in col_names
        assert "skillType" in col_names

    def test_skills_entity_allowed_values(self):
        contract = load_esco_contract()
        skills = next(e for e in contract.entities if e.name == "skills")
        skill_type_col = next(c for c in skills.required_columns if c.name == "skillType")
        assert skill_type_col.allowed_values is not None
        assert "skill/competence" in skill_type_col.allowed_values

    def test_source_metadata(self):
        contract = load_esco_contract()
        assert contract.source.provider == "ESCO"
        assert contract.source.acquisition_method == "manual_download"
        assert "esco.ec.europa.eu" in contract.source.provider_url
