# Coding Standards

Style conventions, architectural rules, and design patterns used across Skill Radar.

## Python Style

- **Formatter**: ruff format (Black-compatible)
- **Linter**: ruff check (100+ rules enabled)
- **Type checker**: mypy (strict mode)
- **Target**: Python 3.11+

## Architectural Rules

### Layer Separation

| Layer | Allowed Dependencies | I/O? |
|-------|---------------------|------|
| CLI (`cli/`) | Orchestrators, config | No (delegates to orchestrators) |
| Orchestrators (`domains/*/orchestrator.py`) | Agents, pure logic | No (sequences agent calls) |
| Platform Agents (`platform/`) | External systems | Yes (S3, Spark, ES) |
| Pure Domain Logic (`domains/*/transforms.py`) | Nothing external | No |

**Rule**: Pure functions never import from `platform/`. Orchestrators never import from `cli/`.

### SSOT Principle

- Table names → `LakeLayout` only
- Schemas → `schema.py` per domain
- Config → `PlatformSettings` (Pydantic)
- Infrastructure requirements → `requirements.py`

Never hardcode table FQNs, column lists, or config values.

### Contract-Driven Processing

ESCO and Adzuna use declarative contracts (`contract.yaml`) that define:
- Entity names and CSV sources
- Column mappings and renames
- Required fields and types

Transforms read the contract — adding a new entity means editing the contract, not the code.

## Naming Conventions

### Modules

- `orchestrator.py` — domain sequence logic
- `transforms.py` — pure functions (zero I/O)
- `schema.py` — column definitions
- `models.py` — Pydantic / dataclass models
- `contract.py` — schema contract definitions

### Functions

- `run_*()` — orchestrator entry points
- `transform_*()` — pure data transforms
- `check_*()` — validation check implementations
- `get_*_checks()` — check collection factories

### Tables

- Bronze: `{source}_{entity}_raw` (e.g., `esco_skills_raw`)
- Silver: `{source}_{entity}` (e.g., `esco_skills`)
- Gold: `{metric}_daily` (e.g., `skill_demand_daily`)

## Error Handling

### Gold Soft-Fail Pattern

Enhancement phases (6+) use:
```python
try:
    result = compute_emerging_skills(...)
except Exception:
    logger.warning("Phase 6 failed", exc_info=True)
    # Continue to next phase
```

Core phases (1–5) fail hard — errors propagate to the orchestrator.

### API Client Pattern

External API calls (Adzuna) use a typed error hierarchy:
- `AuthError` → no retry
- `RateLimitError` → retry with backoff
- `ApiError` → retry with backoff

## Lineage

Every table row carries lineage columns:
- `bronze_loaded_at` / `silver_processed_at` / `gold_computed_at` — timestamps
- `*_pipeline_version` — code version string
- ESCO: `dataset`, `version`, `lang`
- Adzuna: `ingestion_date`, `country`

## Configuration Layering

Priority (highest wins): CLI flags → environment variables → YAML config → contract defaults.

## References

- [Modular Design](../architecture/modular-design.md) — full architecture philosophy
- [Contributing](contributing.md) — workflow
- [Testing](testing.md) — test strategy
