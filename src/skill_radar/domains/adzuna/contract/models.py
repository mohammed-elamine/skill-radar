"""Pydantic models for the Adzuna domain contract."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SourceMetadata(BaseModel):
    """Source metadata for the Adzuna API provider."""

    provider: str
    acquisition_method: str
    provider_url: str


class SupportedCountry(BaseModel):
    """A country supported for Adzuna job search."""

    code: str
    api_country_code: str
    label: str


class ExtractionDefaults(BaseModel):
    """Default extraction parameters."""

    results_per_page: int = 50
    max_pages_per_run: int = 20
    request_timeout_seconds: int = 30
    max_retries: int = 3
    backoff_seconds: int = 2


class ExtractionPreset(BaseModel):
    """A named extraction preset for a specific scope.

    All filter fields are optional; only ``country`` is required.
    """

    country: str
    what: str = ""
    where: str = ""
    category: str = ""
    max_days_old: int | None = None
    sort_by: str | None = None
    full_time: bool | None = None
    part_time: bool | None = None
    salary_min: float | None = None
    salary_max: float | None = None


class FieldExpectationsEntry(BaseModel):
    """Known field identifiers and hint values."""

    job_identifiers: list[str] = Field(default_factory=list)
    important_fields: list[str] = Field(default_factory=list)
    contract_time_values: list[str] = Field(default_factory=list)
    contract_type_values: list[str] = Field(default_factory=list)


class FieldExpectations(FieldExpectationsEntry):
    """Alias kept for backward compatibility with the entry model."""


class AdzunaContract(BaseModel):
    """Strongly-typed Adzuna dataset contract.

    Loaded from ``contract.yaml`` and used to drive extraction,
    schema mapping, and validation.
    """

    dataset: str
    source: SourceMetadata
    supported_countries: list[SupportedCountry]
    refresh_cadence: str
    extraction_defaults: ExtractionDefaults = Field(default_factory=ExtractionDefaults)
    extraction_presets: dict[str, ExtractionPreset] = Field(default_factory=dict)
    field_expectations: FieldExpectations = Field(default_factory=FieldExpectations)

    def get_preset(self, name: str) -> ExtractionPreset:
        """Return a named preset or raise ``KeyError``."""
        if name not in self.extraction_presets:
            available = list(self.extraction_presets.keys())
            raise KeyError(f"Extraction preset '{name}' not found. Available: {available}")
        return self.extraction_presets[name]

    def country_codes(self) -> list[str]:
        """Return the list of supported country codes."""
        return [c.code for c in self.supported_countries]
