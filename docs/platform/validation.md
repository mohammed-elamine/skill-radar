# Validation System

Production-grade health check framework for infrastructure, data quality, and pipeline correctness.

## Purpose

Systematic verification of infrastructure readiness, data quality, and pipeline correctness across all lakehouse layers, with structured JSON reports and CI/CD integration.

## Architecture

```mermaid
graph TB
    CLI["CLI Commands"] -->|NamedCheck list| Runner["run_checks()"]
    Runner -->|ValidationReport| LS["Local JSON Sink"]
    Runner -->|ValidationReport| S3["S3 Upload Sink"]

    subgraph Check Implementations
        IC[infra.py]
        LC[lakehouse.py]
        EC[esco.py]
        AC[adzuna.py]
        GC[gold.py]
        SC[search.py]
    end

    Check Implementations --> Runner
```

## Core Data Models

### CheckStatus

| Status | Symbol | Color | Use Case |
|--------|--------|-------|----------|
| `PASS` (0) | ✓ | Green | Check succeeded |
| `FAIL` (1) | ✗ | Red | Check failed, action required |
| `WARN` (2) | ⚠ | Yellow | Passed with caveat |
| `SKIP` (3) | ↷ | Dim | Not applicable in context |

### CheckResult

```python
@dataclass
class CheckResult:
    name: str              # Stable identifier (e.g., "infra.minio.reachable")
    description: str       # Human-readable description
    status: CheckStatus    # PASS | FAIL | WARN | SKIP
    detail: str            # Short detail message
    metrics: dict          # Numeric metrics (row counts, etc.)
    duration_ms: int       # Execution time
```

### NamedCheck

```python
@dataclass(frozen=True)
class NamedCheck:
    name: str                          # Stable identifier
    description: str                   # Human-readable
    fn: Callable[[], CheckResult]      # Check callable
```

The `NamedCheck` pattern preserves check identity before execution — critical for skip cascading, progress reporting, and deterministic ordering.

### ValidationReport

Aggregates all check results with: validator name, run ID, environment, timing, overall status, and artifact paths.

### ExitCode

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 10 | Infrastructure failure |
| 20 | Landing failure |
| 30 | Bronze failure |
| 40 | Silver failure |
| 50 | Unexpected error |

Stable values — CI/CD can distinguish failure root cause by exit code.

## Validation Runner

### run_checks()

Executes checks sequentially with:
- Per-check timing measurement
- Fail-fast mode (stop on first failure) or continue mode (collect all results)
- JVM crash detection and skip cascading
- ANSI console output (respects `NO_COLOR`)

### JVM Crash Detection

When Spark's JVM dies mid-validation, the runner detects it via heuristic string matching (e.g., "answer from java side is empty") and marks all remaining checks as `SKIP` instead of crashing.

## Check Categories

### Infrastructure (infra.py)

| Check | Scope | Validates |
|-------|-------|-----------|
| `infra.minio.reachable` | Host | MinIO/S3 endpoint responds |
| `infra.s3.buckets.exist` | Host | `skillradar-lake`, `skillradar-logs` exist |
| `infra.spark.session` | Runtime | Spark session starts, `SELECT 1` succeeds |
| `infra.iceberg.configured` | Runtime | Iceberg extensions and catalog present |
| `infra.spark.s3a` | Runtime | S3A filesystem accessible from Spark |
| `infra.namespaces.exist` | Runtime | Iceberg namespaces exist |

### Lakehouse (lakehouse.py)

Generic checks reusable across domains:
- `check_table_exists` — table can be described
- `check_table_non_empty` — table has ≥ min_rows
- `check_table_schema_contains` — required columns exist
- `check_lineage_values` — lineage columns have expected values
- `check_namespace_exists` — Iceberg namespace present

### Domain Checks

| Domain | Landing | Bronze | Silver | Gold | Search |
|--------|---------|--------|--------|------|--------|
| ESCO | ✓ (artifact, manifest, checksum) | ✓ (tables, schema, lineage) | ✓ (transforms, dedup) | — | — |
| Adzuna | — | ✓ (tables, schema, lineage) | ✓ (tables, dedup, keys) | — | — |
| Gold | — | — | — | ✓ (37 checks) | — |
| Search | — | — | — | — | ✓ (indices, Kibana) |

## Report Persistence

### Local Filesystem

```
logs/validation/<validator>/<YYYYMMDD_HHMMSS>.<run_id>.json
```

### S3 Upload

```
s3://skillradar-logs/validation/<validator>/<YYYYMMDD_HHMMSS>.<run_id>.json
```

Controlled via `--upload` flag or `LOG_UPLOAD=1` environment variable.

## Runtime Context

The system auto-detects execution context:

| Context | Detection | Endpoint |
|---------|-----------|----------|
| `host` | Running via `uv run` | `http://localhost:9000` |
| `docker` | Running in container | `http://minio:9000` |

Override: `SKILLRADAR_RUNTIME_CONTEXT=host` or `docker`.

### Scoped Validation

```bash
skill-radar validate infra --scope host      # boto3-only checks
skill-radar validate infra --scope runtime   # Spark/Iceberg checks
skill-radar validate infra --scope all       # Both
```

## Apply + Validate Pattern

The `run` commands follow a two-phase pattern:

1. **Apply** — idempotent provisioning (create-if-not-exists)
2. **Validate** — read-only verification

Both use `get_platform_requirements()` as the single source of required resources.

```
  ── Phase 1: Apply ──
  ✓ apply.bucket.skillradar-lake (Already exists)
  ✓ apply.namespace.sr_bronze (Already exists)

  ── Phase 2: Validate ──
  ✓ infra.minio.reachable
  ✓ infra.s3.buckets.exist
```

## Check Factory

```python
from skill_radar.platform.validate.models import create_check

return create_check(
    name="infra.minio.reachable",
    description="MinIO/S3 endpoint reachable",
    passed=True,
    detail=f"Endpoint: {endpoint}",
    metrics={"latency_ms": 120},
)
```

Supports `skip=True` (not applicable), `warn=True` (passed with caveat).

## CLI Commands

```bash
# Infrastructure
skill-radar validate infra [--scope host|runtime|all] [--upload]

# ESCO
skill-radar validate esco-landing --version v1.2.1 --lang fr
skill-radar validate esco-bronze --version v1.2.1 --lang fr
skill-radar validate esco-silver --version v1.2.1 --lang fr

# Adzuna
skill-radar validate adzuna-bronze
skill-radar validate adzuna-silver

# Gold + Search
skill-radar validate gold --ingestion-date 2025-01-15 --country fr
skill-radar validate search --ingestion-date 2025-01-15 --country fr
```

## Extending the Framework

1. Implement check function returning `CheckResult`
2. Create check collection function returning `list[NamedCheck]`
3. Add CLI command in `validate.py`
4. Add Makefile target

## Module Map

```
src/skill_radar/platform/validate/
├── __init__.py        # Public API exports
├── models.py          # CheckResult, NamedCheck, ValidationReport, ExitCode
├── runner.py          # run_checks(), console output
├── sinks.py           # Local + S3 report persistence
└── checks/
    ├── infra.py       # Infrastructure checks (6)
    ├── lakehouse.py   # Generic Iceberg checks (6)
    ├── esco.py        # ESCO domain checks
    ├── adzuna.py      # Adzuna domain checks
    ├── gold.py        # Gold checks (37)
    └── search.py      # Search/Kibana checks
```

## References

- [Modular Design](../architecture/modular-design.md) — NamedCheck pattern context
- [Command Reference](command-reference.md) — validation CLI details
- [Testing](../development/testing.md) — validation test strategy
