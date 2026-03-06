# Platform Validation System — Data Engineering Guide

> **Audience:** Data engineers, reviewers, and future contributors.
> **Scope:** Design rationale, architecture decisions, module contracts, check patterns, and operational runbooks for the Skill Radar validation framework.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Core Data Models](#3-core-data-models)
   - 3.1 [CheckStatus Enum](#31-checkstatus-enum)
   - 3.2 [CheckResult Dataclass](#32-checkresult-dataclass)
   - 3.3 [NamedCheck Pattern](#33-namedcheck-pattern)
   - 3.4 [ValidationReport](#34-validationreport)
   - 3.5 [ExitCode Enum](#35-exitcode-enum)
4. [Validation Runner](#4-validation-runner)
   - 4.1 [run_checks() Orchestration](#41-run_checks-orchestration)
   - 4.2 [Fail-Fast vs Continue Modes](#42-fail-fast-vs-continue-modes)
   - 4.3 [JVM Crash Detection](#43-jvm-crash-detection)
   - 4.4 [Console Output & Formatting](#44-console-output--formatting)
5. [Check Categories](#5-check-categories)
   - 5.1 [Infrastructure Checks (infra.py)](#51-infrastructure-checks-infrapy)
   - 5.2 [Lakehouse Checks (lakehouse.py)](#52-lakehouse-checks-lakehousepy)
   - 5.3 [Domain Checks (esco.py, etc.)](#53-domain-checks-escopy-etc)
6. [Report Persistence (Sinks)](#6-report-persistence-sinks)
   - 6.1 [Local Filesystem](#61-local-filesystem)
   - 6.2 [S3/MinIO Upload](#62-s3minio-upload)
7. [CLI Integration](#7-cli-integration)
   - 7.1 [Validation Commands](#71-validation-commands)
   - 7.2 [Orchestration Commands (run.py)](#72-orchestration-commands-runpy)
8. [Check Factory Pattern](#8-check-factory-pattern)
9. [Runtime Context & Environment Awareness](#9-runtime-context--environment-awareness)
10. [Production Alignment Decisions](#10-production-alignment-decisions)
11. [Testing Strategy](#11-testing-strategy)
12. [Module Map](#12-module-map)
13. [Extending the Framework](#13-extending-the-framework)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Executive Summary

The **Platform Validation System** is a core infrastructure component of Skill Radar that provides **production-grade health checks** for the lakehouse platform. It enables systematic verification of infrastructure readiness, data quality, and pipeline correctness across all stages of the medallion architecture.

### Key Design Goals

- **Framework-Agnostic** — Validation logic is independent of any specific data source (ESCO, Adzuna, etc.)
- **Composable** — Checks are reusable building blocks that can be combined for different validation scenarios
- **Observable** — Every validation run produces structured JSON reports with timing, metrics, and artifacts
- **Production-Ready** — Deterministic exit codes, fail-fast modes, and JVM crash detection
- **Environment-Aware** — Distinguishes between host and container execution contexts

### What It Validates

| Layer | Example Checks |
|-------|----------------|
| **Infrastructure** | MinIO reachable, S3 buckets exist, Spark session starts, Iceberg catalog configured |
| **Landing Zone** | Artifact exists, manifest valid, checksums match |
| **Bronze Layer** | Tables exist, schemas correct, lineage values present |
| **Silver Layer** | Transformations complete, deduplication correct, key population |

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           CLI Layer (validate.py, run.py)                     │
│  • skill-radar validate infra                                                 │
│  • skill-radar validate esco-bronze                                           │
│  • skill-radar run infra                                                      │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  │ NamedCheck[]
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Validation Runner (runner.py)                         │
│  • run_checks() — executes checks sequentially                                │
│  • Timing measurement per check                                               │
│  • JVM crash detection and skip cascade                                       │
│  • Console output with ANSI colors                                            │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  │ ValidationReport
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Report Sinks (sinks.py)                               │
│  • write_report_local() — logs/validation/<validator>/<ts>.<run_id>.json      │
│  • write_report_s3() — s3://<logs-bucket>/validation/<validator>/...          │
│  • finalize_report() — orchestrates local + optional S3                       │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                          Check Implementations                                │
│                                                                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐   │
│  │ infra.py        │  │ lakehouse.py    │  │ esco.py / adzuna.py         │   │
│  │ (6 checks)      │  │ (6 checks)      │  │ (domain-specific checks)    │   │
│  │                 │  │                 │  │                             │   │
│  │ • minio reach   │  │ • namespace     │  │ • landing artifact exists   │   │
│  │ • buckets exist │  │ • table exists  │  │ • manifest valid            │   │
│  │ • spark session │  │ • non_empty     │  │ • checksum matches          │   │
│  │ • iceberg conf  │  │ • schema check  │  │ • bronze tables correct     │   │
│  │ • s3a access    │  │ • lineage vals  │  │ • silver transformations    │   │
│  │ • namespaces    │  │ • partitioning  │  │                             │   │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                           Core Models (models.py)                             │
│  • CheckStatus (PASS | FAIL | WARN | SKIP)                                    │
│  • CheckResult (single check outcome with metrics)                            │
│  • NamedCheck (check identity + callable)                                     │
│  • ValidationReport (aggregated run result)                                   │
│  • ExitCode (stable CLI return codes)                                         │
│  • create_check() (factory function)                                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **CLI Command** receives user input (scope, version, lang, etc.)
2. **Check Factory** builds `list[NamedCheck]` with config bound via closures
3. **Runner** executes checks sequentially, measuring timing
4. **Report** aggregates results with overall status
5. **Sinks** persist JSON report to filesystem and optionally S3
6. **Exit Code** reflects validation outcome for CI/CD integration

---

## 3. Core Data Models

### 3.1 CheckStatus Enum

```python
class CheckStatus(IntEnum):
    """Status of a single validation check."""
    PASS = 0   # Check succeeded
    FAIL = 1   # Check failed (blocks pipeline)
    WARN = 2   # Check succeeded with warning (advisory)
    SKIP = 3   # Check not applicable (e.g., pyspark not installed)
```

Each status has associated console formatting:

| Status | Symbol | Color | Use Case |
|--------|--------|-------|----------|
| PASS | ✓ | Green | Check succeeded |
| FAIL | ✗ | Red | Check failed, action required |
| WARN | ⚠ | Yellow | Passed with caveat (e.g., auth issue but reachable) |
| SKIP | ↷ | Dim | Not applicable in current context |

### 3.2 CheckResult Dataclass

```python
@dataclass
class CheckResult:
    name: str              # Stable identifier (e.g., "infra.minio.reachable")
    description: str       # Human-readable description
    status: CheckStatus    # PASS | FAIL | WARN | SKIP
    detail: str = ""       # Short detail message
    metrics: dict[str, Any] = field(default_factory=dict)  # Numeric metrics
    started_at_utc: str = ""   # ISO timestamp
    duration_ms: int = 0       # Execution time

    def as_dict(self) -> dict[str, Any]:
        """JSON-serializable representation."""
```

**Design Rationale:**
- `name` is a **stable identifier** for machine consumption (e.g., `bronze.table.exists.esco_skills_raw`)
- `description` is for human display
- `metrics` enables quantitative reporting (row counts, percentages)
- Timing fields support performance tracking

### 3.3 NamedCheck Pattern

```python
@dataclass(frozen=True)
class NamedCheck:
    """A check definition with identity preserved."""
    name: str
    description: str
    fn: Callable[[], CheckResult]

    def __call__(self) -> CheckResult:
        """Execute the check callable."""
        return self.fn()
```

**Why NamedCheck?**

The `NamedCheck` pattern solves a critical problem: when checks are defined as lambdas capturing configuration, the runner cannot determine check identity before execution. This causes issues for:

1. **Skip cascading** — When JVM dies, remaining checks must be marked SKIP with proper names
2. **Progress reporting** — Display check names before execution completes
3. **Deterministic ordering** — Check sequence is explicit, not implicit

**Example Usage:**

```python
def get_infra_checks_host(config: PlatformSettings) -> list[NamedCheck]:
    ctx = get_runtime_context()
    return [
        NamedCheck(
            name="infra.minio.reachable",
            description="MinIO/S3 endpoint is reachable",
            fn=lambda: check_minio_reachable(config, context=ctx),
        ),
        NamedCheck(
            name="infra.s3.buckets.exist",
            description="Required S3 buckets exist",
            fn=lambda: check_s3_buckets_exist(config, context=ctx),
        ),
    ]
```

### 3.4 ValidationReport

```python
@dataclass
class ValidationReport:
    validator_name: str           # e.g., "infra", "esco_bronze"
    env: str                      # e.g., "local", "dev", "prod"
    run_id: str                   # Unique run identifier
    report_id: str                # Short UUID for this report
    started_at_utc: str
    finished_at_utc: str
    duration_ms: int
    status: CheckStatus           # Overall PASS or FAIL
    checks: list[CheckResult]     # Individual check results
    artifacts: dict[str, str]     # Paths/keys to related files

    @property
    def passed(self) -> bool: ...
    @property
    def failed_checks(self) -> list[CheckResult]: ...
    @property
    def warned_checks(self) -> list[CheckResult]: ...
```

**Report Artifacts:**

Reports capture additional context via `artifacts`:
- `local_report_path` — Filesystem path to JSON report
- `s3_report_key` — Object key if uploaded
- `spark_app_id` — Spark application identifier (when applicable)
- `scope` — Validation scope (host/runtime/all)

### 3.5 ExitCode Enum

```python
class ExitCode(IntEnum):
    """Consistent exit codes by failure type."""
    OK = 0                # All checks passed
    INFRA_FAILURE = 10    # Infrastructure validation failed
    LANDING_FAILURE = 20  # Landing zone validation failed
    BRONZE_FAILURE = 30   # Bronze layer validation failed
    SILVER_FAILURE = 40   # Silver layer validation failed
    UNEXPECTED = 50       # Unexpected error
```

**Design Rationale:**
- **Stable values** — Exit codes are API contracts; they never change
- **Layer-specific** — CI/CD can distinguish failure root cause
- **Offset ranges** — Allow future codes (e.g., Gold = 60) without collision

---

## 4. Validation Runner

### 4.1 run_checks() Orchestration

```python
def run_checks(
    checks: list[NamedCheck],
    *,
    validator_name: str,
    run_id: str = "",
    env: str = "",
    fail_fast: bool = False,
    quiet: bool = False,
) -> ValidationReport:
```

The runner:

1. **Initializes** a `ValidationReport` with timing started
2. **Iterates** through checks sequentially
3. **Executes** each check and captures result
4. **Measures** per-check duration
5. **Aggregates** overall status (any FAIL → report FAIL)
6. **Finalizes** timing and returns report

### 4.2 Fail-Fast vs Continue Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `fail_fast=False` (default) | Execute all checks, collect all results | Full validation report for debugging |
| `fail_fast=True` | Stop on first FAIL | Fast feedback in CI pipelines |

```bash
# Full report (default)
skill-radar validate esco-bronze --version v1.2.0 --lang fr

# Fail-fast mode
skill-radar validate esco-bronze-e2e --version v1.2.0 --lang fr --fail-fast
```

### 4.3 JVM Crash Detection

Spark's JVM can die mid-validation (e.g., out-of-memory, connectivity loss). The runner detects this and **skips remaining checks** with a clear reason:

```python
def _is_spark_jvm_down(exc: BaseException) -> bool:
    """Heuristic: treat these as terminal Spark JVM failures."""
    signatures = [
        "answer from java side is empty",
        "error while sending or receiving",
        "connection refused",
        "gateway is not connected",
    ]
    return any(s in str(exc).lower() for s in signatures)
```

When JVM death is detected:
1. Current check is marked **FAIL** with error detail
2. All subsequent checks are marked **SKIP** with reason: `"Spark JVM became unavailable..."`
3. Report proceeds to finalization (no crash)

### 4.4 Console Output & Formatting

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Validator: infra
  run_id: a1b2c3d4  env: local
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ infra.minio.reachable (localhost:9000 [host]) [120ms]
  ✓ infra.s3.buckets.exist (Found: skillradar-lake, skillradar-logs) [45ms]

  ────────────────────────────────────────────────
  PASS  (✓ 2)
  Duration: 165ms
  Report: logs/validation/infra/20260306_081542.a1b2c3d4.json
```

- **Header** shows validator name, run_id, environment
- **Check lines** show symbol, name, detail, timing
- **Footer** shows overall status, counts, duration, report path
- **Colors** are disabled when `NO_COLOR` env var is set or not a TTY

---

## 5. Check Categories

### 5.1 Infrastructure Checks (infra.py)

Infrastructure checks verify platform components are operational **before** running data pipelines.

| Check | Scope | What It Validates |
|-------|-------|-------------------|
| `infra.minio.reachable` | Host | MinIO/S3 endpoint responds to `list_buckets` |
| `infra.s3.buckets.exist` | Host | Required buckets (`skillradar-lake`, `skillradar-logs`) exist |
| `infra.spark.session` | Runtime | Spark session starts and `SELECT 1` succeeds |
| `infra.iceberg.configured` | Runtime | Spark config has Iceberg extensions and catalog |
| `infra.spark.s3a` | Runtime | Spark can access MinIO via Hadoop s3a filesystem |
| `infra.namespaces.exist` | Runtime | Required Iceberg namespaces (`sr_bronze`, etc.) exist |

**Check Collection Functions:**

```python
# Host checks (boto3-only, no Spark)
get_infra_checks_host(config) -> list[NamedCheck]

# Runtime checks (require Spark container)
get_infra_checks_runtime(config) -> list[NamedCheck]

# All checks (deprecated, use scoped versions)
get_infra_checks(config) -> list[NamedCheck]
```

### 5.2 Lakehouse Checks (lakehouse.py)

Generic Iceberg/lakehouse checks reusable across domains:

| Check | What It Validates |
|-------|-------------------|
| `check_namespace_exists` | Iceberg namespace is present in catalog |
| `check_table_exists` | Table can be described |
| `check_table_non_empty` | Table has `>= min_rows` records |
| `check_table_schema_contains` | Required columns exist in schema |
| `check_lineage_values` | Lineage columns (`dataset`, `version`, `lang`) have expected values |
| `check_partitioning` | (MVP: skip) Partitioning configuration |

**Example Usage:**

```python
from skill_radar.platform.validate.checks.lakehouse import (
    check_table_exists,
    check_table_non_empty,
    check_table_schema_contains,
)

def get_bronze_checks(spark, table_fqn, required_cols) -> list[NamedCheck]:
    return [
        NamedCheck(
            name=f"bronze.table.exists.{table_fqn}",
            description=f"Table {table_fqn} exists",
            fn=lambda: check_table_exists(spark, table_fqn),
        ),
        NamedCheck(
            name=f"bronze.table.non_empty.{table_fqn}",
            description=f"Table {table_fqn} has data",
            fn=lambda: check_table_non_empty(spark, table_fqn),
        ),
        # ...
    ]
```

### 5.3 Domain Checks (esco.py, etc.)

Domain-specific checks implement business logic for each data source:

#### ESCO Landing Checks

| Check | What It Validates |
|-------|-------------------|
| `landing.artifact.exists` | ZIP file exists in landing zone |
| `landing.manifest.exists` | `manifest.json` exists |
| `landing.manifest.schema_valid` | Manifest conforms to expected schema |
| `landing.manifest.checksum` | SHA256 matches artifact |
| `landing.manifest.version` | Schema version is supported |

#### ESCO Bronze Checks

| Check | What It Validates |
|-------|-------------------|
| `bronze.namespace.exists` | `sr_bronze` namespace exists |
| `bronze.table.exists.<entity>` | Entity table exists |
| `bronze.table.non_empty.<entity>` | Entity table has rows |
| `bronze.schema.correct.<entity>` | Schema matches contract |
| `bronze.lineage.correct.<entity>` | Lineage columns present with expected values |

---

## 6. Report Persistence (Sinks)

### 6.1 Local Filesystem

Reports are written to a structured directory hierarchy:

```
logs/
└── validation/
    ├── infra/
    │   └── 20260306_081542.a1b2c3d4.json
    ├── esco_landing/
    │   └── 20260306_083015.b2c3d4e5.json
    ├── esco_bronze/
    │   └── 20260306_090012.c3d4e5f6.json
    └── adzuna_bronze/
        └── 20260306_100000.d4e5f6g7.json
```

**Filename Format:** `<YYYYMMDD_HHMMSS>.<run_id>.json`

```python
def write_report_local(report: ValidationReport, *, log_dir: Path | None = None) -> Path:
```

### 6.2 S3/MinIO Upload

Reports can be uploaded to the logs bucket for centralized observability:

```
s3://skillradar-logs/
└── validation/
    ├── infra/
    │   └── 20260306_081542.a1b2c3d4.json
    └── ...
```

```python
def write_report_s3(report: ValidationReport, *, config: PlatformSettings) -> str | None:
```

**Control via CLI or environment:**

```bash
# Explicit upload
skill-radar validate infra --upload

# Environment variable
LOG_UPLOAD=1 skill-radar validate infra
```

### Finalize Report

```python
def finalize_report(
    report: ValidationReport,
    *,
    config: PlatformSettings | None = None,
    log_dir: Path | None = None,
    upload_s3: bool | None = None,
) -> tuple[Path, str | None]:
    """Write report locally and optionally upload to S3."""
```

---

## 7. CLI Integration

### 7.1 Validation Commands

```bash
# Infrastructure validation
skill-radar validate infra [--scope host|runtime|all] [--upload] [--quiet]

# ESCO validation
skill-radar validate esco-landing --version v1.2.0 --lang fr
skill-radar validate esco-bronze --version v1.2.0 --lang fr [--entities skills,occupations]
skill-radar validate esco-bronze-e2e --version v1.2.0 --lang fr [--fail-fast]

# Adzuna validation
skill-radar validate adzuna-bronze
skill-radar validate adzuna-silver
```

**Scope Parameter (infra only):**

| Scope | Checks Run | Execution Context |
|-------|------------|-------------------|
| `host` | MinIO reachable, buckets exist | Host (uv run) |
| `runtime` | Spark session, Iceberg config, S3A, namespaces | Spark container |
| `all` | Both host and runtime checks | Spark container |

### 7.2 Orchestration Commands (run.py)

The `run` command group combines **apply** (provisioning) with **validate** (verification):

```bash
# Provision and validate infrastructure
skill-radar run infra [--upload] [--quiet]

# Extract bronze and validate
skill-radar run esco-bronze --version v1.2.0 --lang fr [--fail-fast]
```

**Phased Execution:**

```
  ── Phase 1: Apply ──

  ✓ apply.bucket.skillradar-lake (Already exists)
  ✓ apply.bucket.skillradar-logs (Already exists)
  ✓ apply.namespace.sr_bronze (Already exists)

  ── Phase 2: Validate ──

  ✓ infra.minio.reachable
  ✓ infra.s3.buckets.exist
  ✓ infra.spark.session
  ...
```

---

## 8. Check Factory Pattern

The `create_check()` factory simplifies check result creation:

```python
def create_check(
    name: str,
    description: str,
    passed: bool,
    *,
    detail: str = "",
    metrics: dict[str, Any] | None = None,
    skip: bool = False,
    skip_reason: str = "",
    warn: bool = False,
    warn_reason: str = "",
) -> CheckResult:
```

**Usage Examples:**

```python
# Simple pass/fail
return create_check(
    name="infra.minio.reachable",
    description="MinIO/S3 endpoint reachable",
    passed=True,
    detail=f"Endpoint: {endpoint}",
)

# Failure with detail
return create_check(
    name="infra.s3.buckets.exist",
    description="Required S3 buckets exist",
    passed=False,
    detail=f"Missing: {', '.join(missing_buckets)}",
)

# Skip (not applicable)
return create_check(
    name="infra.spark.session",
    description="Spark session starts",
    passed=False,  # ignored when skip=True
    skip=True,
    skip_reason="pyspark not installed (run inside Spark container)",
)

# Warning (passed with caveat)
return create_check(
    name="infra.minio.reachable",
    description="MinIO/S3 endpoint reachable",
    passed=True,
    warn=True,
    warn_reason="Auth issue: InvalidAccessKeyId",
)
```

---

## 9. Runtime Context & Environment Awareness

The validation system is **environment-aware**, adapting behavior based on execution context:

### RuntimeContext Enum

```python
class RuntimeContext(Enum):
    HOST = "host"      # Running on developer machine (uv run)
    DOCKER = "docker"  # Running inside Docker container
```

### Endpoint Resolution

```python
def resolve_s3_endpoint(s3_config: S3Config, context: RuntimeContext) -> str:
    """Return correct S3 endpoint for the execution context."""
    if context == RuntimeContext.HOST:
        return s3_config.endpoint_host  # e.g., http://localhost:9000
    return s3_config.endpoint           # e.g., http://minio:9000
```

### Environment Variable Override

```bash
# Force host context (even inside container)
SKILLRADAR_RUNTIME_CONTEXT=host skill-radar validate infra

# Force docker context (testing on host)
SKILLRADAR_RUNTIME_CONTEXT=docker skill-radar validate infra
```

### Credential Loading

The Makefile provides `RUN_STEP_HOST` macro that sources `.env` before host commands:

```makefile
define RUN_STEP_HOST
    @set -a; source .env; set +a; \
    SKILLRADAR_RUNTIME_CONTEXT=host $(1)
endef

validate-infra:
    $(call RUN_STEP_HOST, uv run skill-radar validate infra --scope host)
```

---

## 10. Production Alignment Decisions

### 10.1 Single Source of Truth for Requirements

All infrastructure requirements are defined in `requirements.py`:

```python
def get_platform_requirements(config: PlatformSettings) -> PlatformRequirements:
    """The ONLY place that determines what infrastructure resources are required."""
```

Both **apply** (provisioning) and **validate** (verification) use this function, ensuring consistency.

### 10.2 Explicit Check Identity

The `NamedCheck` pattern ensures checks have stable, machine-readable names even when implemented as lambdas:

```python
NamedCheck(
    name="bronze.table.exists.esco_skills_raw",  # Stable identifier
    description="Table esco_skills_raw exists",   # Human-readable
    fn=lambda: check_table_exists(spark, table),  # Implementation
)
```

### 10.3 Structured Exit Codes

Exit codes are **layer-specific** and **stable**:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 10 | Infrastructure failure |
| 20 | Landing failure |
| 30 | Bronze failure |
| 40 | Silver failure |
| 50 | Unexpected error |

CI/CD pipelines can distinguish failure causes without parsing logs.

### 10.4 Idempotent Apply + Validate Pattern

The `run` commands follow a two-phase pattern:

1. **Apply** — Idempotent provisioning (create if not exists)
2. **Validate** — Read-only verification

This ensures:
- Safe re-runs (apply is idempotent)
- Clear separation of concerns
- Verification of apply success

### 10.5 JVM Resilience

Spark JVM crashes are detected and handled gracefully:
- Current check fails with error detail
- Remaining checks are skipped (not crashed)
- Report is finalized and persisted
- Exit code reflects failure

### 10.6 Observability

Every validation run produces:
- JSON report with timing and metrics
- Console output with ANSI formatting
- Optional S3 upload for centralized logging
- Run ID correlation with application logs

---

## 11. Testing Strategy

### Unit Tests

```
tests/unit/platform/
├── test_validation_models.py    # CheckResult, ValidationReport, ExitCode
├── test_validation_runner.py    # run_checks(), fail-fast, JVM detection
└── test_infra_checks.py         # Individual check functions
```

**Mocking Strategy:**
- boto3 clients mocked for S3 checks
- Spark sessions mocked or use local mode
- Filesystem mocked for sink tests

### Integration Tests

```
tests/integration/
├── domains/esco/
│   └── test_bronze_validate.py  # Full bronze validation with Iceberg
└── infra/
    └── test_iceberg_smoke.py    # Spark + Iceberg integration
```

### E2E Tests

```
tests/e2e/
└── test_esco_pipeline.py        # Full landing → bronze → validate flow
```

---

## 12. Module Map

```
src/skill_radar/platform/validate/
├── __init__.py              # Public API exports
├── models.py                # CheckResult, NamedCheck, ValidationReport, ExitCode
├── runner.py                # run_checks(), console output
├── sinks.py                 # write_report_local(), write_report_s3()
└── checks/
    ├── __init__.py          # Re-exports
    ├── infra.py             # Infrastructure checks (6 checks)
    ├── lakehouse.py         # Generic Iceberg checks (6 checks)
    └── esco.py              # ESCO domain checks (landing + bronze)

src/skill_radar/cli/
├── validate.py              # CLI validation commands
└── run.py                   # Orchestration commands (apply + validate)

src/skill_radar/platform/infra/
├── apply.py                 # Provisioning operations
└── requirements.py          # Single source of truth for requirements
```

---

## 13. Extending the Framework

### Adding a New Check

1. **Implement check function** in appropriate module:

```python
# In checks/my_domain.py
def check_my_condition(config: PlatformSettings) -> CheckResult:
    try:
        # Perform check logic
        passed = ...
        return create_check(
            name="my_domain.my_condition",
            description="My condition is met",
            passed=passed,
            detail="Additional info",
        )
    except Exception as exc:
        return create_check(
            name="my_domain.my_condition",
            description="My condition is met",
            passed=False,
            detail=str(exc)[:200],
        )
```

2. **Create check collection function**:

```python
def get_my_domain_checks(config: PlatformSettings) -> list[NamedCheck]:
    return [
        NamedCheck(
            name="my_domain.my_condition",
            description="My condition is met",
            fn=lambda: check_my_condition(config),
        ),
    ]
```

3. **Add CLI command** in `validate.py`:

```python
@validate_group.command("my-domain")
def validate_my_domain():
    config = load_platform_config()
    checks = get_my_domain_checks(config)
    report = run_checks(checks, validator_name="my_domain")
    # ...
```

### Adding a New Validator Scope

1. Define new `ExitCode` value (if needed)
2. Create check collection function
3. Add CLI command with appropriate options
4. Add Makefile target

---

## 14. Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `NoCredentialsError` | AWS credentials not set | Source `.env` or use `make validate-infra` |
| `EndpointConnectionError` | MinIO not running | Run `docker compose up -d` |
| `pyspark not installed` | Running in wrong context | Use `--scope host` or run inside Spark container |
| JVM crashes during validation | Memory/connectivity issue | Check Docker resources, retry |
| Checks skipping unexpectedly | Prior JVM crash | Check preceding check errors |

### Debug Mode

```bash
# Verbose logging
SKILLRADAR_LOG_LEVEL=DEBUG skill-radar validate infra

# Force context
SKILLRADAR_RUNTIME_CONTEXT=host skill-radar validate infra
```

### Report Inspection

```bash
# Find latest report
ls -lt logs/validation/infra/ | head -5

# View report
cat logs/validation/infra/20260306_*.json | jq .
```

---

## Summary

The Platform Validation System provides a **production-grade**, **framework-agnostic** validation infrastructure that ensures lakehouse health and data quality. Key strengths:

- **Composable checks** via `NamedCheck` pattern
- **Observable results** via structured JSON reports
- **Resilient execution** via JVM crash detection
- **CI/CD ready** via stable exit codes
- **Environment-aware** via runtime context detection
- **Extensible** via clean check factory patterns

This system forms the foundation for all pipeline validation in Skill Radar, ensuring that infrastructure, landing zones, and data layers meet quality expectations before downstream consumption.
