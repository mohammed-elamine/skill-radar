"""Pydantic models for the ESCO domain contract."""

from __future__ import annotations

from urllib.parse import urlparse

from pydantic import BaseModel, field_validator


class ColumnSpec(BaseModel):
    """A required column, optionally constrained to a set of allowed values."""

    name: str
    allowed_values: list[str] | None = None


class ContractEntity(BaseModel):
    """An entity (CSV file) expected inside the artifact ZIP.

    Each entity maps to one CSV with required columns declared in the contract.
    """

    name: str
    filename_pattern: str
    required_columns: list[ColumnSpec]

    @field_validator("required_columns", mode="before")
    @classmethod
    def _parse_columns(cls, v: list) -> list[dict]:
        """Accept both plain strings and ``{col: [values]}`` dicts from YAML."""
        result: list[dict] = []
        for item in v:
            if isinstance(item, str):
                result.append({"name": item})
            elif isinstance(item, dict):
                for col_name, allowed in item.items():
                    str_values = [str(x) for x in allowed] if allowed else None
                    result.append({"name": col_name, "allowed_values": str_values})
            else:
                result.append(item)
        return result


class ContractSource(BaseModel):
    """Source metadata for the artifact provider."""

    provider: str
    acquisition_method: str
    provider_url: str

    @field_validator("provider_url", mode="before")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        """Validate that the URL is well-formed and uses HTTP or HTTPS."""
        if v is not None:
            if not v.startswith("http://") and not v.startswith("https://"):
                raise ValueError("provider_url must start with http:// or https://")
            result = urlparse(v)
            if not (result.scheme and result.netloc):
                raise ValueError("provider_url must be a valid URL")
        return v


class ContractArtifact(BaseModel):
    """Artifact-level metadata."""

    type: str
    content: str
    file_type: str


class EscoContract(BaseModel):
    """Strongly-typed ESCO dataset contract.

    Loaded from ``contract.yaml`` and used to validate artifacts
    before they are landed in the lake.
    """

    dataset: str
    artifact: ContractArtifact
    supported_languages: list[str]
    version_pattern: str
    entities: list[ContractEntity]
    source: ContractSource
