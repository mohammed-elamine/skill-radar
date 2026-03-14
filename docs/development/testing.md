# Testing

Test strategy, test types, coverage, and how to run tests for Skill Radar.

## Test Organization

```
tests/
├── unit/                  # Fast, isolated, no external deps
│   ├── cli/               # CLI command tests
│   ├── config/            # Configuration loading tests
│   ├── dags/              # Airflow DAG validation
│   ├── domains/           # Domain logic tests
│   │   ├── adzuna/        # Adzuna transforms, client, validation
│   │   ├── esco/          # ESCO transforms, contract, validation
│   │   ├── gold/          # Matching, analytics, scoring
│   │   └── search/        # Document builders, datasets, mappings
│   └── platform/          # Platform agent tests
│       ├── lake/           # LakeLayout, S3, manifest
│       ├── search/         # ES client, Kibana, builders
│       └── validate/       # Check models, runner, sinks
├── integration/           # Requires running services (Spark, MinIO, ES)
│   ├── domains/
│   └── platform/
├── e2e/                   # Full pipeline tests
└── fixtures/              # Shared test fixtures
```

## Running Tests

### Unit Tests (fast, no services needed)

```bash
# All unit tests
make utest
uv run pytest tests/unit -v

# Specific domain
uv run pytest tests/unit/domains/gold/ -v
uv run pytest tests/unit/domains/adzuna/ -v
uv run pytest tests/unit/domains/esco/ -v

# Specific module
uv run pytest tests/unit/platform/search/test_kibana_builders.py -v
```

### Integration Tests (requires Spark, MinIO)

```bash
make up  # Start infrastructure first
uv run pytest tests/integration/ -v
```

### E2E Tests (full pipeline)

```bash
make up-all
uv run pytest tests/e2e/ -v
```

### Full Quality Check

```bash
make check    # lint + format + mypy + unit tests
make ci       # check + coverage report
```

## Test Coverage

| Domain | Unit Tests | Integration Tests |
|--------|-----------|-------------------|
| ESCO | Transforms, contract, landing, validation | Bronze extraction, Silver dedup |
| Adzuna | Client (mocked HTTP), transforms, validation | Bronze/Silver with Iceberg |
| Gold | Matching (n-gram, scoring), analytics, schema | Full matching pipeline |
| Search | Document builders, datasets, mappings, Kibana | ES indexing, alias swap |
| Platform | LakeLayout, S3Client, validation runner | Spark session, Iceberg |
| DAGs | DAG loading, task factory, shared layer | — |

Current counts: **609 unit tests**, 35+ integration tests.

## Mocking Strategy

- **boto3 clients**: Mocked for S3 checks and operations
- **Spark sessions**: Mocked or use local mode for unit tests
- **HTTP clients**: Mocked for Adzuna API and Elasticsearch
- **Filesystem**: Mocked for sink tests and manifest operations

## Fixture Patterns

Shared fixtures in `tests/fixtures/`:
- Sample DataFrames for each domain
- Mock configuration objects
- Test contract files

## Test Naming

```python
def test_skill_matching_prefers_title_over_description():
    """Preferred label in title scores 1.0, in description 0.8."""

def test_silver_dedup_keeps_latest_by_bronze_loaded_at():
    """Window dedup keeps newest version of each job."""
```

## CI Pipeline

`make ci` runs:
1. `ruff check .` — linting
2. `ruff format --check .` — formatting
3. `mypy src` — type checking (123 source files)
4. `pytest tests/unit --cov` — unit tests with coverage

## References

- [Contributing](contributing.md) — workflow and quality gates
- [Coding Standards](coding-standards.md) — conventions
- [Validation System](../platform/validation.md) — runtime validation (different from test-time)
