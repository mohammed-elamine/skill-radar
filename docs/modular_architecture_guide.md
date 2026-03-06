# Modular Architecture Guide

> How **Skill Radar** components, agents and orchestrators are designed to be
> modular, task-specific and robust — enabling clear separation of workload,
> a single source of truth, and delegation to context-aware agents.

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [Layered Architecture Overview](#2-layered-architecture-overview)
3. [CLI Layer — Thin Entry Points](#3-cli-layer--thin-entry-points)
4. [Domain Orchestrators — The Control Plane](#4-domain-orchestrators--the-control-plane)
5. [Platform Agents — Reusable Infrastructure Services](#5-platform-agents--reusable-infrastructure-services)
6. [Single Source of Truth Principle](#6-single-source-of-truth-principle)
7. [Pure Domain Logic — Zero I/O Functions](#7-pure-domain-logic--zero-io-functions)
8. [Validation Framework — Pluggable Checks](#8-validation-framework--pluggable-checks)
9. [Structured Observability](#9-structured-observability)
10. [Idempotent Infrastructure Provisioning](#10-idempotent-infrastructure-provisioning)
11. [Production Alignment](#11-production-alignment)

---

## 1. Design Philosophy

The platform follows three governing principles:

| Principle | Description |
|-----------|-------------|
| **Modularity** | Every component does one thing. Orchestrators sequence steps; agents own a capability (storage, layout, manifest); pure functions transform data. |
| **Delegation** | Orchestrators never embed infrastructure knowledge. They call platform agents that are context-aware, globally reusable, and environment-adaptive. |
| **Single source of truth** | Paths come from `LakeLayout`, configuration from `PlatformSettings`, schema contracts from `contract.yaml`, and run metadata from `RunContext`. No component invents its own copy. |

These principles are not aspirational guidelines — they are enforced through the code structure itself: modules are physically separated by responsibility and import boundaries prevent accidental coupling.

---

## 2. Layered Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                    CLI Layer                          │
│   skill-radar esco upload / bronze / silver           │
│   skill-radar adzuna bronze / silver                  │
│   skill-radar validate infra / esco-landing / ...     │
│   skill-radar run infra / esco-bronze                 │
│   skill-radar infra apply                             │
└───────────────────┬──────────────────────────────────┘
                    │ delegates to
┌───────────────────▼──────────────────────────────────┐
│              Domain Orchestrators                     │
│   run_intake()   run_bronze_extraction()              │
│   apply_infra()  run_checks()                         │
└───────────────────┬──────────────────────────────────┘
                    │ calls
┌───────────────────▼──────────────────────────────────┐
│              Platform Agents                          │
│   LakeLayout · S3Client · ManifestBuilder             │
│   RuntimeContext · ConfigLoader · validate.runner      │
└───────────────────┬──────────────────────────────────┘
                    │ uses
┌───────────────────▼──────────────────────────────────┐
│           Pure Domain Logic / Contracts               │
│   schema_mapping · validation · hashing               │
│   contract.yaml · PlatformSettings · RunContext       │
└──────────────────────────────────────────────────────┘
```

Each layer may only call the layer below it. The CLI never calls `boto3`
directly; an orchestrator never constructs an S3 path by concatenation;
a pure function never imports PySpark.

---

## 3. CLI Layer — Thin Entry Points

**Location:** `src/skill_radar/cli/`

Every CLI command follows the same three-line pattern:

```python
@esco.command()
@click.option(...)
def upload(version, lang, source):
    init_logging(job_name="esco_intake_upload")
    config = load_platform_config()
    run_intake(config=config, version=version, lang=lang, source=Path(source))
```

1. **Bootstrap logging** — a single `init_logging()` call that creates `RunContext`, wires console + file handlers.
2. **Load config** — `load_platform_config()` returns a validated `PlatformSettings` Pydantic model.
3. **Delegate** — one function call into the appropriate domain orchestrator.

The CLI knows *nothing* about storage paths, S3 endpoints, Spark sessions, or Iceberg tables. Its only job is to parse user arguments and hand off execution.

### Composite CLI commands

`cli/run.py` exposes multi-phase commands that chain orchestrators and validators:

```python
@run.command()
def esco_bronze(version, lang, source, validate_flag, ...):
    # Phase 1: Landing intake
    run_intake(config=config, version=version, lang=lang, source=Path(source))
    # Phase 2: Bronze extraction
    run_bronze_extraction(config=config, version=version, lang=lang)
    # Phase 3: Validation (optional)
    if validate_flag:
        report = run_checks(checks, context=f"esco-bronze-e2e-{version}-{lang}")
        finalize_report(report, config=config)
```

Even here the CLI is a thin sequencer — it calls fully self-contained orchestrators in order.

### CLI subgroup registration

```python
# cli/__init__.py
cli.add_command(esco)
cli.add_command(adzuna)
cli.add_command(infra)
cli.add_command(run_cli, name="run")
cli.add_command(validate_cli, name="validate")
```

New data sources get their own subgroup (e.g. `adzuna`) registered in a single line. The group structure mirrors the domain model: **source → stage → action**.

---

## 4. Domain Orchestrators — The Control Plane

Orchestrators are the only modules that *sequence* steps. They contain no infrastructure logic and no hardcoded values. Their docstrings enforce this contract explicitly:

> *"This module contains **no** direct path building, hardcoded config,
> or raw S3 calls — everything is delegated to platform agents."*
> — `landing/intake.py`

### ESCO Landing Intake (`run_intake`)

**Location:** `src/skill_radar/domains/esco/landing/intake.py`

```
Step 1  validate_artifact()           ← pure function
Step 2  sha256_file()                 ← pure function
Step 3  LakeLayout.landing_zip_key()  ← platform agent
Step 4  S3Client.object_exists()      ← platform agent (idempotency gate)
Step 5  S3Client.upload_file()        ← platform agent
Step 6  ManifestBuilder.build()       ← platform agent
Step 7  S3Client.upload_bytes()       ← platform agent
```

Seven steps, zero `boto3` imports, zero path concatenation, zero `f"s3a://..."`.

### ESCO Bronze Extraction (`run_bronze_extraction`)

**Location:** `src/skill_radar/domains/esco/bronze/extract.py`

```
Step 1  LakeLayout.landing_zip_key()            ← resolve source key
Step 2  S3Client download ZIP                    ← get artifact
Step 3  extract_csv_from_zip()                   ← pure function
Step 4  S3Client upload staged CSVs              ← write to staging
Step 5  Spark read from s3a:// path              ← uses LakeLayout prefix
Step 6  validate_required_columns()              ← pure contract validation
Step 7  contract-driven rename + newline helpers ← pure schema_mapping
Step 8  add lineage columns                      ← pure schema_mapping
Step 9  write to Iceberg                         ← LakeLayout.iceberg_table_fqn()
```

The orchestrator reads the ESCO contract to determine *which* CSV files to process and *what* column transformations to apply — the schema is never embedded in code.

### Adzuna Bronze Extraction

**Location:** `src/skill_radar/domains/adzuna/bronze/extract.py`

Follows the identical delegation pattern: fetch data → build Spark DataFrame → write to Iceberg via `LakeLayout`. Proves the pattern generalises across data sources.

### Why this matters

Because orchestrators contain only *sequencing*, they are:

- **Easy to read** — each step is a single method call with a descriptive name.
- **Easy to test** — swap agents with mocks and verify the call order.
- **Easy to extend** — adding a step means adding one line, not refactoring path logic.

---

## 5. Platform Agents — Reusable Infrastructure Services

Agents are globally available, stateless (or context-driven) services that encapsulate *one* infrastructure capability. Orchestrators from any domain can import and use them.

### `LakeLayout` — Path Authority

**Location:** `src/skill_radar/platform/lake/layout.py`

`LakeLayout` is the **single source of truth** for all storage paths in the lakehouse. It is initialised from `PlatformSettings` and exposes methods — never raw strings:

| Method | Purpose |
|--------|---------|
| `landing_zip_key(source, version, lang)` | S3 key for a landing artifact |
| `landing_manifest_key(source, version, lang)` | S3 key for the manifest JSON |
| `bronze_staging_prefix(source, entity)` | S3 prefix for staged CSVs |
| `iceberg_table_fqn(layer, domain, table)` | Fully qualified Iceberg table name |
| `iceberg_namespace(layer, domain)` | Iceberg namespace string |
| `logs_prefix(job_name, run_id)` | S3 prefix for log uploads |

No other module in the codebase constructs a storage path. If the layout convention changes (e.g. adding a date partition), the change is made in one place.

### `S3Client` — Storage Abstraction

**Location:** `src/skill_radar/platform/storage/s3_client.py`

Thin wrapper over `boto3` that exposes a minimal surface:

- `upload_file(bucket, key, path)` — upload local file
- `upload_bytes(bucket, key, data)` — upload in-memory bytes
- `object_exists(bucket, key)` — idempotency check
- `get_object_checksum(bucket, key)` — integrity verification

A `build_s3_client(config, endpoint, purpose)` factory configures timeouts per purpose:

```python
build_s3_client(s3_config, endpoint, purpose="default")       # 10s connect
build_s3_client(s3_config, endpoint, purpose="healthcheck")    # 2s connect
```

This means health checks fail fast without penalising production uploads.

### `ManifestBuilder` — Audit Trail

**Location:** `src/skill_radar/platform/manifest/builder.py`

Produces a versioned JSON manifest containing:

- `schema_version` — enables forward-compatible schema evolution
- Artifact metadata (size, checksum, filename)
- Source provenance (origin URL, version, language)
- Validation results (pre-upload checks)
- Audit trail (who, when, runtime context)

Manifests are the lakehouse's **immutable record** of what was ingested and under what conditions.

### `RuntimeContext` — Environment Awareness

**Location:** `src/skill_radar/platform/runtime/context.py`

A `StrEnum` with two values: `HOST` and `DOCKER`. Auto-detected by probing `/.dockerenv` and `/proc/1/cgroup`, overridable via `SKILLRADAR_RUNTIME_CONTEXT`.

This feeds into `resolve_s3_endpoint()` — on HOST, MinIO is at `localhost:9000`; in Docker, it's at `minio:9000`. The rest of the codebase never thinks about endpoints.

### `ConfigLoader` — Validated Configuration

**Location:** `src/skill_radar/config/loader.py`

```
defaults.yaml  →  env var overrides  →  PlatformSettings (Pydantic v2)
```

The loader reads a `defaults.yaml` file, applies any environment variable overrides (prefixed `SKILLRADAR_`), and returns a fully validated `PlatformSettings` model. Every downstream consumer receives a typed, validated object — never a raw dict.

---

## 6. Single Source of Truth Principle

One of the strongest design invariants is that **every category of platform knowledge has exactly one authoritative source**:

| Knowledge | Authority | Consumers |
|-----------|-----------|-----------|
| Storage paths | `LakeLayout` | Orchestrators, validators, log upload |
| Configuration values | `PlatformSettings` (via `load_platform_config()`) | Every module |
| ESCO schema contract | `contract.yaml` → `EscoContract` (Pydantic) | Bronze extraction, validation checks |
| Run metadata | `RunContext` (via `contextvars`) | Logging filters, formatters, manifest builder |
| Required infra resources | `get_platform_requirements(config)` | `apply_infra()`, `check_s3_buckets_exist()` |
| Exit codes | `ExitCode` enum | CLI, validation runner |

When the validation framework needs to know which buckets must exist, it calls `get_platform_requirements(config)` — the same function that `apply_infra()` uses to create them. There is no drift.

When the bronze extraction needs to know which columns a CSV must contain, it reads the contract YAML — the same contract that validation checks use to verify the output. There is no drift.

### Contract-Driven Schema

The ESCO contract (`domains/esco/contract/contract.yaml`) declares, per entity:

```yaml
entities:
  skills:
    required_columns: [conceptUri, preferredLabel, ...]
    renames:
      conceptUri: concept_uri
      preferredLabel: preferred_label
    newline_fields: [description, altLabels]
    derived_newline_helpers: true
```

This YAML is loaded once (`@lru_cache`) into a `EscoContract` Pydantic model. The bronze orchestrator iterates `contract.entities` to drive column validation, renaming, and newline normalisation — all from a single declarative file.

---

## 7. Pure Domain Logic — Zero I/O Functions

Below the agents and orchestrators sits a layer of pure functions with **no dependencies on Spark, boto3, or network**:

| Module | Functions | Dependencies |
|--------|-----------|--------------|
| `esco/bronze/schema_mapping.py` | `to_snake_case()`, `rename_mapping()`, `newline_raw_fields()`, `norm_column_name()`, `count_column_name()`, `LINEAGE_COLUMNS` | Python stdlib only |
| `esco/bronze/validation.py` | `validate_required_columns()`, `extract_csv_from_zip()` | `zipfile`, `csv` |
| `esco/landing/validation.py` | `validate_version()`, `validate_language()`, `validate_zip()`, `validate_artifact()` | `re`, `zipfile` |
| `utils/hashing.py` | `sha256_file()` | `hashlib` |
| `utils/assertions.py` | `require(condition, message)` | None |

### Why this matters

- **Testable** — unit tests run in < 1 ms with no fixtures, no Docker, no Spark.
- **Portable** — the same mapping logic applies whether the caller is a Spark job, a notebook, or a CLI command.
- **Auditable** — column rename rules are deterministic functions of the contract, not runtime state.

The `require()` guard is used throughout orchestrators as an assertion-style precondition check:

```python
require(zipfile, "Source ZIP does not exist")
require(version_match, f"Version {version} does not match pattern {pattern}")
```

It raises a clear `AssertionError` with context rather than allowing silent failures downstream.

---

## 8. Validation Framework — Pluggable Checks

**Location:** `src/skill_radar/platform/validate/`

The validation framework is itself a showcase of modular design. It is built from four composable parts:

### Check Model

```python
class NamedCheck(NamedTuple):
    name: str
    description: str
    fn: Callable[..., CheckResult]

class CheckResult:
    name: str
    description: str
    status: CheckStatus    # PASS | FAIL | WARN | SKIP
    detail: str
    elapsed_ms: float
```

A check is a plain function `() → CheckResult`. The `create_check()` factory standardises construction.

### Runner

`run_checks(checks: list[NamedCheck], context: str)` iterates the checks, captures timing, prints ANSI-coloured output, and aggregates results into a `ValidationReport`.

The runner knows nothing about *what* is being checked — it only knows how to execute a list of `NamedCheck` functions and collect results.

### Domain Check Libraries

| Module | Checks |
|--------|--------|
| `validate/checks/infra.py` | MinIO reachable, buckets exist, Spark session, Iceberg integration, S3A access, namespaces |
| `validate/checks/esco.py` | Landing artifact exists, manifest valid, checksum matches, bronze tables exist, schema correct, lineage values |

Each check function receives `PlatformSettings` (and possibly `SparkSession`) as arguments and returns a `CheckResult`. Checks are read-only — they never create or modify resources.

### Sinks

After checks run, the `ValidationReport` is persisted via pluggable sinks:

- `write_report_local(report)` → `logs/validation/<name>/<timestamp>.json`
- `write_report_s3(report, config)` → `s3://skillradar-logs/validation/...`
- `finalize_report(report, config)` → both (S3 controlled by `LOG_UPLOAD` env var)

### Composability in practice

CLI commands compose check lists dynamically:

```python
checks = build_infra_checks(config) + build_esco_landing_checks(config, version, lang)
report = run_checks(checks, context="esco-landing-v1.2.0-fr")
finalize_report(report, config=config)
sys.exit(ExitCode.from_report(report).value)
```

Adding a new check means writing one function and appending it to a list. The runner, sinks, and CLI require zero changes.

### Structured Exit Codes

```python
class ExitCode(IntEnum):
    OK                = 0
    INFRA_FAILURE     = 10
    LANDING_FAILURE   = 20
    BRONZE_FAILURE    = 30
    SILVER_FAILURE    = 40
    UNEXPECTED        = 50
```

`ExitCode.from_report(report)` inspects the report's check names to determine the appropriate exit code tier. CI/CD pipelines can branch on exit code ranges.

---

## 9. Structured Observability

**Location:** `src/skill_radar/platform/logging/`

### One-Shot Bootstrap

```python
init_logging(job_name="esco_intake_upload")
```

This single call:

1. Creates a `RunContext` via `create_initial_context(job_name)`.
2. Attaches a `ContextFilter` to every handler — injecting `run_id`, `job_name`, `env`, `dataset`, `version`, etc. into every log record.
3. Configures a `TextFormatter` for console output and a `JsonFormatter` for the file handler.

The bootstrap is idempotent — calling it twice has no effect.

### Progressive Context Enrichment

When the orchestrator learns more about the current run (e.g. after parsing arguments), it enriches the context:

```python
set_context(dataset="esco", version="1.2.0", lang="fr", dt="2026-02-21")
```

From that point on, every log line includes those fields — without the orchestrator touching any handler or formatter.

### `RunContext` as ContextVar

`RunContext` lives in a `ContextVar`, making it thread-safe and test-isolatable:

```python
@dataclass
class RunContext:
    run_id: str         # auto-generated UUID prefix
    job_name: str       # set at init
    env: str            # from SKILLRADAR_ENV or "local"
    started_at_utc: str # ISO timestamp
    dataset: str        # set mid-run
    version: str        # set mid-run
    lang: str           # set mid-run
    git_sha: str        # from GIT_SHA env var
    spark_app_id: str   # set when Spark starts
    host: str           # auto-detected hostname
    logfile: str        # set by bootstrap
```

Tests call `reset_context()` in teardown to avoid cross-test leakage.

### Dual Output

| Handler | Formatter | Purpose |
|---------|-----------|---------|
| Console (`stderr`) | `TextFormatter` | Human-readable, one-line-per-record |
| File (`logs/`) | `JsonFormatter` | Machine-parseable, one JSON object per line |

### Best-Effort Log Upload

`finalize_logging(upload=True)` flushes handlers and uploads the log file to S3:

```
s3://skillradar-logs/logs/<job_name>/dt=YYYY-MM-DD/run_id=<run_id>/app.log
```

Upload failures are logged as warnings and never mask the original job exit code. Observability is critical but never a blocker.

---

## 10. Idempotent Infrastructure Provisioning

**Location:** `src/skill_radar/platform/infra/apply.py`

`apply_infra(config)` creates all required platform resources:

```python
def apply_infra(config, *, context=None):
    # 1. Verify MinIO is reachable (fail-fast)
    check_s3_connectivity(s3_config, endpoint)

    # 2. Ensure buckets exist (idempotent)
    ensure_buckets(s3_config, endpoint, required=reqs.buckets)

    # 3. Ensure Iceberg namespaces exist (idempotent)
    ensure_namespaces(config, target_namespaces=reqs.namespaces)
```

Both `ensure_buckets()` and `ensure_namespaces()` use **check-then-create** logic:

```python
def ensure_buckets(s3_config, endpoint, *, required):
    for bucket in required:
        try:
            client.head_bucket(Bucket=bucket)       # already exists → skip
        except ClientError:
            client.create_bucket(Bucket=bucket)      # create idempotently
```

Running `skill-radar infra apply` ten times produces the same state as running it once. This is critical for CI/CD pipelines and developer onboarding.

The list of required resources comes from `get_platform_requirements(config)` — the same function used by validation checks — guaranteeing that what is provisioned is exactly what is verified.

---

## 11. Production Alignment

The modular design implemented above is not academic — it directly maps to production data platform practices:

### Separation of Concerns → Team Scalability

```
CLI team     → adds commands, parses args, wires phases
Domain team  → writes orchestrators, contracts, pure logic
Platform team → maintains agents (S3Client, LakeLayout, logging, validation)
```

Changes in one layer do not ripple into others. A new data source joins the platform by:

1. Adding a contract YAML.
2. Writing an orchestrator that calls existing agents.
3. Adding a CLI subgroup with commands.
4. Writing domain-specific validation checks.

Nothing in the platform layer changes.

### Contract-Driven Pipelines → Schema Governance

The contract YAML is a machine-readable schema agreement. It:

- Defines required columns, renames, and transformations **declaratively**.
- Is version-controlled alongside code.
- Is consumed at extraction time *and* validation time, ensuring consistency.

This mirrors production practices like schema registries and data contracts.

### Structured Exit Codes → CI/CD Integration

The `ExitCode` enum gives CI/CD pipelines a machine-readable signal of *where* a failure occurred. A pipeline can:

- Retry on `INFRA_FAILURE` (transient network issue).
- Alert the ESCO domain team on `BRONZE_FAILURE`.
- Page on-call for `UNEXPECTED`.

### Immutable Manifests → Data Lineage

Every landing ingestion produces a manifest recording the artifact checksum, source URL, validation results, and runtime context. This creates an auditable chain of custody from raw file to Iceberg table.

### Idempotent Operations → Safe Re-Runs

Both infrastructure provisioning and landing intake are idempotent. If a pipeline fails mid-way through, operators can safely re-run the entire job without creating duplicates or corrupting state.

### Environment-Adaptive Runtime → Portable Deployment

`RuntimeContext` auto-detection means the same CLI commands work identically on a developer laptop (`HOST → localhost:9000`) and inside a Docker container (`DOCKER → minio:9000`). No code changes, no branch-on-environment logic scattered through orchestrators.

---

## Summary

| Property | Mechanism |
|----------|-----------|
| **Modularity** | Physical separation into CLI / orchestrator / agent / pure logic layers |
| **Task Specificity** | Each module has a single, documented responsibility |
| **Robustness** | `require()` guards, typed configs, structured exit codes, idempotent operations |
| **Delegation** | Orchestrators call agents; agents call pure functions; no layer reaches down two levels |
| **Single Source of Truth** | `LakeLayout` (paths), `PlatformSettings` (config), `contract.yaml` (schema), `RunContext` (metadata) |
| **Production Alignment** | Contract-driven schemas, immutable manifests, structured observability, CI/CD-aware exit codes, environment-adaptive runtime |

The result is a data platform where adding a new source, stage, or check requires touching exactly the files that *should* change — and nothing else.
