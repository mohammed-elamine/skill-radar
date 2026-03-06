"""Tests for the Adzuna domain contract — loading, validation, and accessors."""

from __future__ import annotations

from skill_radar.domains.adzuna.contract import load_adzuna_contract
from skill_radar.domains.adzuna.contract.models import (
    AdzunaContract,
    ExtractionDefaults,
    ExtractionPreset,
    FieldExpectations,
    SourceMetadata,
    SupportedCountry,
)


class TestLoadContract:
    """Loading and parsing the contract YAML."""

    def test_returns_adzuna_contract(self) -> None:
        contract = load_adzuna_contract()
        assert isinstance(contract, AdzunaContract)

    def test_dataset_name(self) -> None:
        contract = load_adzuna_contract()
        assert contract.dataset == "adzuna"

    def test_source_provider(self) -> None:
        contract = load_adzuna_contract()
        assert isinstance(contract.source, SourceMetadata)
        assert contract.source.provider == "Adzuna"
        assert contract.source.acquisition_method == "api"

    def test_supported_countries(self) -> None:
        contract = load_adzuna_contract()
        assert len(contract.supported_countries) >= 1
        assert all(isinstance(c, SupportedCountry) for c in contract.supported_countries)
        codes = contract.country_codes()
        assert "fr" in codes

    def test_refresh_cadence(self) -> None:
        contract = load_adzuna_contract()
        assert contract.refresh_cadence == "daily"

    def test_extraction_defaults(self) -> None:
        contract = load_adzuna_contract()
        ed = contract.extraction_defaults
        assert isinstance(ed, ExtractionDefaults)
        assert ed.results_per_page == 50
        assert ed.max_pages_per_run == 20
        assert ed.request_timeout_seconds > 0
        assert ed.max_retries >= 1
        assert ed.backoff_seconds >= 1

    def test_field_expectations(self) -> None:
        contract = load_adzuna_contract()
        fe = contract.field_expectations
        assert isinstance(fe, FieldExpectations)
        assert "id" in fe.job_identifiers
        assert len(fe.contract_time_values) > 0


class TestContractPresets:
    """Named extraction presets."""

    def test_default_fr_exists(self) -> None:
        contract = load_adzuna_contract()
        preset = contract.get_preset("default_fr")
        assert isinstance(preset, ExtractionPreset)
        assert preset.country == "fr"

    def test_preset_not_found_raises(self) -> None:
        contract = load_adzuna_contract()
        import pytest

        with pytest.raises(KeyError, match="no_such_preset"):
            contract.get_preset("no_such_preset")

    def test_country_codes_match_supported(self) -> None:
        contract = load_adzuna_contract()
        codes = contract.country_codes()
        for sc in contract.supported_countries:
            assert sc.code in codes
