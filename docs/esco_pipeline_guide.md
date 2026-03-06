# ESCO Pipeline Guide — Data Engineering Reference

> **Scope**: This document describes the design, architecture, and implementation
> of the **Landing**, **Bronze**, and **Silver** stages for the ESCO datasource
> within the Skill Radar platform. It targets data engineers who need to understand,
> operate, or extend the pipeline.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture Principles](#2-architecture-principles)
3. [Domain Contract System](#3-domain-contract-system)
4. [Package Structure](#4-package-structure)
5. [Landing Stage](#5-landing-stage)
6. [Bronze Stage](#6-bronze-stage)
7. [Silver Stage](#7-silver-stage)
8. [Platform Services](#8-platform-services)
9. [Data Flow — End-to-End](#9-data-flow--end-to-end)
10. [Operational Reference](#10-operational-reference)
11. [Design Decisions & Trade-offs](#11-design-decisions--trade-offs)

---

## 1. Overview

ESCO (*European Skills, Competences, Qualifications and Occupations*) is a
static, versioned taxonomy distributed as a ZIP archive containing multiple CSV
files. Skill Radar ingests ESCO data through three clearly separated stages:

| Stage       | Responsibility                              | Storage Target         |
|-------------|---------------------------------------------|------------------------|
| **Landing** | Validate, checksum, and persist the raw ZIP artifact into object storage (MinIO/S3). | `s3://skillradar-lake/data/landing/taxonomy/esco/…` |
| **Bronze**  | Extract CSVs from the landed ZIP, validate against the contract, normalise column names, add lineage, and write to Iceberg tables. | `sr.sr_bronze.esco_skills_raw`, `…occupations_raw`, `…relations_raw` |
| **Silver**  | Apply text normalisation, split multi-value fields into arrays, parse dates, extract UUIDs, deduplicate, and write typed Iceberg tables. | `sr.sr_silver.esco_skills`, `…occupations`, `…relations` |

Every operation across all stages is **contract-driven**, **idempotent**, and
**auditable** — there are no hardcoded paths, magic column names, or silent
fallbacks anywhere in the pipeline.

---

## 2. Architecture Principles

The following principles guided every design choice in the ESCO pipeline:

### 2.1 Contract-First Development

The ESCO domain contract (`contract.yaml`) is the **single source of truth**
for every expectation about the data: which entities exist, what columns they
must contain, what values are allowed, how vendor columns map to Bronze names,
and which fields carry newline-separated labels. Both the Landing validator
and the Bronze extractor read the same contract — there is no drift between
validation and processing logic.

### 2.2 Separation of Concerns (Control Plane vs. Data Plane)

Modules are organised into clear roles:

- **Orchestrators** (`landing.intake`, `bronze.extract`, `silver.format`) coordinate the
  workflow but contain no path building, no raw S3 calls, and no schema logic.
- **Platform agents** (`LakeLayout`, `S3Client`, `ManifestBuilder`,
  `RuntimeContext`) provide reusable infrastructure services.
- **Domain logic** (`landing.validation`, `bronze.validation`,
  `bronze.schema_mapping`) contains pure functions that are unit-testable
  without Spark, S3, or any I/O.
- **Spark transforms** (`platform.spark.transforms`) provides reusable Column
  and DataFrame utilities for Silver processing.

### 2.3 Centralised Path Building

Every S3 key and Iceberg table name is computed by `LakeLayout`. No module
constructs storage paths itself. This eliminates path inconsistencies and
makes the entire lake navigable from a single class.

### 2.4 Idempotency and Reproducibility

- **Landing**: The intake checks whether an artifact already exists before
  uploading. Re-uploading the same version + lang returns a clear conflict
  error unless `--force` is specified.
- **Bronze**: Iceberg partition-level overwrite (`overwritePartitions()`) on
  `(version, lang)` ensures that re-running the same extraction replaces
  exactly the affected partitions without duplicating data.
- **Silver**: Same partition-level overwrite strategy ensures idempotent
  re-runs without data duplication.

### 2.5 Distributed-Safe I/O

Spark executors cannot reliably read from the Spark driver's local filesystem.
The Bronze extractor therefore **stages** extracted CSVs to S3 before pointing
Spark at `s3a://` paths — guaranteeing correct behaviour on multi-node clusters.

### 2.6 Strict Logging Context

Every process entry-point must call `init_logging(job_name=...)`, which creates
a `RunContext` propagated to every log record via `contextvars`. If a module
emits a log without an initialised context, a `RuntimeError` is raised
immediately — there is no silent fallback.

---

## 3. Domain Contract System

### 3.1 Contract YAML

The file `src/skill_radar/domains/esco/contract/contract.yaml` declares the
full schema expectations for the ESCO dataset:

```yaml
dataset: esco

artifact:
  type: zip
  content: classification
  file_type: csv

supported_languages: [fr, en]
version_pattern: "^v\\d+\\.\\d+\\.\\d+$"

entities:
  - name: skills
    filename_pattern: "skills_{lang}.csv"
    required_columns:
      - conceptUri
      - preferredLabel
      - altLabels
      - hiddenLabels
      - description
      - skillType
      - reuseLevel
    renames:
      conceptUri: concept_uri
      preferredLabel: preferred_label
      altLabels: alt_labels_raw
      hiddenLabels: hidden_labels_raw
      # ...
    newline_fields: [altLabels, hiddenLabels]
    derived_newline_helpers: true

  - name: occupations
    # ... (same pattern)

  - name: relations
    filename_pattern: "occupationSkillRelations_{lang}.csv"
    required_columns: [occupationUri, occupationLabel, relationType, ...]
    renames: { ... }
    derived_newline_helpers: false
```

Key observations:

| Field                      | Purpose |
|----------------------------|---------|
| `filename_pattern`         | Resolves the CSV filename inside the ZIP for a given `{lang}`. |
| `required_columns`         | Validated at both Landing (header check) and Bronze (Spark DataFrame). |
| `renames`                  | Explicit vendor → bronze column mapping (no guesswork). |
| `newline_fields`           | Declares which vendor columns contain `\n`-separated labels. |
| `derived_newline_helpers`  | Controls whether `_norm` / `_count` helper columns are materialised. |

### 3.2 Pydantic Models

The YAML is parsed into strongly-typed Pydantic v2 models
(`EscoContract`, `ContractEntity`, `ColumnSpec`, etc.) by `loader.py`.
The loader uses `@lru_cache` so the contract is parsed exactly once per process.

### 3.3 Allowed-Value Constraints

`ColumnSpec` supports an optional `allowed_values` list. When present:

- Landing validation checks column presence only (header-level).
- Bronze validation performs a **Spark aggregation** to detect every row
  violating the constraint and raises `BronzeValidationError` with exact
  counts of invalid values per column.

---

## 4. Package Structure

```
src/skill_radar/domains/esco/
├── __init__.py
├── contract/                  # Domain contract layer
│   ├── __init__.py
│   ├── contract.yaml          # The single source of truth
│   ├── loader.py              # YAML → Pydantic (cached)
│   └── models.py              # EscoContract, ContractEntity, ColumnSpec, …
├── landing/                   # Landing stage
│   ├── __init__.py
│   ├── intake.py              # Orchestrator: validate → checksum → upload → manifest
│   └── validation.py          # Pure validators (ZIP structure, columns, version, lang)
├── bronze/                    # Bronze stage
│   ├── __init__.py
│   ├── errors.py              # BronzeValidationError
│   ├── extract.py             # Orchestrator: download → stage → Spark read → validate → write
│   ├── iceberg.py             # Iceberg namespace + table write helpers
│   ├── schema_mapping.py      # Contract-driven renames, snake_case, newline helpers
│   └── validation.py          # Pure validators (required columns, ZIP extraction)
└── silver/                    # Silver stage
    ├── __init__.py
    └── format.py              # Orchestrator: Bronze read → transform → dedupe → write
```

Supporting platform packages:

```
src/skill_radar/
├── config/                    # PlatformSettings, loader, Pydantic models
├── platform/
│   ├── lake/                  # LakeLayout, LakeLayer/Domain/Source enums
│   ├── logging/               # init_logging, RunContext, ContextFilter, formatters
│   ├── manifest/              # ManifestBuilder (JSON manifest for landed artifacts)
│   ├── runtime/               # RuntimeContext detection (host vs Docker), endpoint resolution
│   ├── spark/                 # Spark transforms (normalize, split, dedupe, parse)
│   ├── storage/               # S3Client (boto3 wrapper), exceptions
│   └── validate/              # Validation framework (landing + bronze + silver checks)
└── utils/
    ├── assertions.py          # require() guard
    └── hashing.py             # sha256_file (chunked, memory-safe)
```

---

## 5. Landing Stage

### 5.1 Purpose

The Landing stage takes an externally-acquired ESCO ZIP file and persists it
into object storage with full validation, checksumming, and manifest creation.
After Landing, the artifact is considered "received and verified" — ready for
Bronze extraction.

### 5.2 Workflow

```
 ┌─────────────────────────────────────────────────────────────────┐
 │  CLI: skill-radar esco upload --version v1.2.1 --lang fr       │
 │  Or:  make upload-esco VERSION=v1.2.1 LANG=fr                  │
 └──────────────┬──────────────────────────────────────────────────┘
                │
                ▼
 1. Resolve ZIP file (--file flag or dropzone search)
                │
                ▼
 2. Load PlatformSettings + EscoContract
                │
                ▼
 3. Validate artifact
    ├── Version matches pattern (^v\d+\.\d+\.\d+$)
    ├── Language is in supported_languages
    ├── File is a valid ZIP archive
    ├── Each contract entity CSV is present inside the ZIP
    └── Each required column exists in each CSV header
                │
                ▼
 4. Compute SHA-256 checksum (chunked, memory-safe)
                │
                ▼
 5. Build storage keys via LakeLayout
    ├── artifact_key: data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/esco.zip
    └── manifest_key: data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/manifest.json
                │
                ▼
 6. Idempotency check (does artifact_key already exist in S3?)
    ├── Yes + no --force → return conflict error
    └── No or --force   → proceed
                │
                ▼
 7. Upload ZIP to S3 via S3Client
                │
                ▼
 8. Build manifest (ManifestBuilder) → upload manifest JSON to S3
                │
                ▼
 9. Return IntakeResult (success, keys, checksum, validation details)
```

### 5.3 Validation Detail

Validation is implemented as a chain of pure functions in `landing/validation.py`:

| Check                           | Function             | Scope |
|---------------------------------|----------------------|-------|
| Version format                  | `validate_version`   | Regex match against `contract.version_pattern` |
| Language supported              | `validate_language`  | Membership in `contract.supported_languages` |
| Valid ZIP archive               | `validate_zip`       | `zipfile.is_zipfile()` |
| Entity CSV present              | `validate_zip`       | Filename in `zf.namelist()` |
| Required columns in CSV header  | `validate_zip`       | CSV header parsed from ZIP stream |

Each check produces a `Check(name, passed, message)` dataclass. The aggregate
`ValidationResult` short-circuits: if version or language fails, the ZIP is
never opened.

### 5.4 Manifest

After a successful upload, a JSON manifest is persisted alongside the artifact:

```json
{
  "schema_version": "1.0.0",
  "dataset": "esco",
  "artifact": {
    "type": "zip",
    "version": "v1.2.1",
    "language": "fr",
    "checksum": { "algorithm": "sha256", "value": "a1b2c3..." },
    "sha256": "a1b2c3...",
    "size_bytes": 12345678,
    "storage": {
      "bucket": "skillradar-lake",
      "key": "data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/esco.zip"
    }
  },
  "source": {
    "provider": "ESCO",
    "acquisition_method": "manual_download",
    "provider_url": "https://esco.ec.europa.eu"
  },
  "validation": {
    "status": "passed",
    "checks": [ ... ]
  },
  "audit": {
    "uploaded_at_utc": "2026-03-06T...",
    "uploaded_by": "cli",
    "environment": "local"
  }
}
```

The manifest provides a complete audit trail: who uploaded, when, what
checksum, what validation checks ran, and where the artifact is stored.

---

## 6. Bronze Stage

### 6.1 Purpose

The Bronze stage reads the landed ZIP from object storage, extracts each
entity CSV, validates and transforms it according to the contract, adds
lineage metadata, and writes the result to partitioned Iceberg tables.

### 6.2 Workflow

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │  spark-submit jobs/esco/bronze_esco_to_iceberg.py                   │
 │      --version v1.2.1 --lang fr                                     │
 │  Or: skill-radar esco bronze --version v1.2.1 --lang fr             │
 │  Or: make bronze-esco VERSION=v1.2.1 LANG=fr                        │
 └────────────────┬────────────────────────────────────────────────────┘
                  │
                  ▼
  1. Load PlatformSettings + EscoContract + RunContext
                  │
                  ▼
  2. Resolve landing keys via LakeLayout
     ├── zip_key:      data/landing/taxonomy/esco/artifact/version=…/lang=…/esco.zip
     └── manifest_key: data/landing/taxonomy/esco/artifact/version=…/lang=…/manifest.json
                  │
                  ▼
  3. Load artifact SHA-256 from manifest (boto3, driver-side)
                  │
                  ▼
  4. Download ZIP from S3 to driver temp directory
     (fallback: compute SHA-256 from downloaded file if manifest unavailable)
                  │
                  ▼
  5. Compute staging prefix via LakeLayout
     data/bronze/taxonomy/esco/staging/version=…/lang=…/run_id=…
                  │
                  ▼
  6. Ensure Iceberg namespace: CREATE NAMESPACE IF NOT EXISTS sr.sr_bronze
                  │
                  ▼
  7. For each contract entity (skills, occupations, relations):
     │
     ├── a) Extract CSV from ZIP → local temp
     ├── b) Upload CSV to S3 staging (distributed-safe)
     ├── c) Spark reads from s3a://…/staging/…/entity=skills/skills_fr.csv
     ├── d) Validate required columns against contract
     ├── e) Validate allowed values (Spark aggregation)
     ├── f) Rename columns (contract-driven mapping)
     ├── g) Add newline helper columns (_norm, _count) if configured
     └── h) Add lineage columns (9 columns) + write to Iceberg
                  │
                  ▼
  8. Return BronzeExtractionResult with per-entity outcomes
```

### 6.3 Schema Mapping

Schema mapping is entirely driven by the contract and implemented in
`bronze/schema_mapping.py` — a **pure-Python module with zero Spark dependency**
that can be fully tested in isolation.

#### Column Renaming

For each vendor column in the CSV, the mapping is resolved as:

1. If `contract_entity.renames` has an explicit entry → use it.
2. Otherwise → apply `to_snake_case()` (camelCase → snake_case).

#### Lineage Columns

Every Bronze row carries 9 lineage columns:

| Column             | Value |
|--------------------|-------|
| `dataset`          | `"esco"` |
| `entity`           | `"skills"`, `"occupations"`, or `"relations"` |
| `version`          | Artifact version (e.g. `"v1.2.1"`) |
| `lang`             | Language code (e.g. `"fr"`) |
| `source_zip_key`   | Full S3 key of the landed ZIP |
| `manifest_key`     | Full S3 key of the manifest JSON |
| `artifact_sha256`  | SHA-256 of the ZIP artifact |
| `ingested_at_utc`  | Spark `current_timestamp()` at write time |
| `run_id`           | Unique identifier for this pipeline run |

### 6.4 Iceberg Table Management

Bronze tables are managed in `bronze/iceberg.py`:

- **Namespace**: `sr.sr_bronze` — created with `CREATE NAMESPACE IF NOT EXISTS`.
- **Table naming**: `sr.sr_bronze.esco_{entity}_raw` (suffix `_raw` distinguishes
  Bronze from Silver tables).
- **Partitioning**: `(version, lang)` — enables efficient reads and
  partition-level overwrite for idempotent re-runs.
- **Table properties**: Iceberg format-version 2, Parquet write format.

---

## 7. Silver Stage

### 7.1 Purpose

The Silver stage reads from Bronze Iceberg tables and applies domain-specific
transformations to produce typed, normalised, and deduplicated tables suitable
for analytics and downstream Gold processing. Silver is the **canonical**
representation of ESCO data within the lakehouse.

### 7.2 Design Philosophy

Silver transformations follow a **pure-function approach**:

1. **Text normalisation**: Trim whitespace and collapse multiple spaces.
2. **Label array splitting**: Convert newline-separated strings to sorted,
   deduplicated arrays.
3. **Type parsing**: Parse date strings to proper `DATE` types.
4. **UUID extraction**: Extract UUIDs from ESCO URIs for efficient joins.
5. **Deterministic deduplication**: Use window functions with stable hash
   tie-breakers for reproducible results.

### 7.3 Workflow

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │  skill-radar esco silver --version v1.2.1 --lang fr                 │
 │  Or: make silver-esco VERSION=v1.2.1 ESCO_LANG=fr                   │
 └────────────────┬────────────────────────────────────────────────────┘
                  │
                  ▼
  1. Load PlatformSettings + RunContext
                  │
                  ▼
  2. Ensure Silver namespace: CREATE NAMESPACE IF NOT EXISTS sr.sr_silver
                  │
                  ▼
  3. For each entity (skills, occupations, relations):
     │
     ├── a) Read Bronze table filtered by (version, lang)
     │      spark.read.table("sr.sr_bronze.esco_skills_raw")
     │          .where(version == "v1.2.1" AND lang == "fr")
     │
     ├── b) Apply entity-specific transformation:
     │      • Text normalisation (trim + collapse whitespace)
     │      • Split newline labels → sorted array
     │      • Parse modified_date → DATE type
     │      • Extract concept_uri_uuid from URI
     │      • Compute alt_labels_count, hidden_labels_count
     │
     ├── c) Deduplicate by canonical key:
     │      • skills: (concept_uri, version, lang)
     │      • occupations: (concept_uri, version, lang)
     │      • relations: (occupation_uri, skill_uri, relation_type, version, lang)
     │
     └── d) Write to Silver Iceberg table (partition overwrite)
                  │
                  ▼
  4. Return SilverFormatResult with per-entity outcomes
```

### 7.4 Entity Transformations

Each entity has a dedicated transformation function that applies the
appropriate processing logic.

#### Skills Transformation

| Bronze Column       | Silver Column        | Transformation |
|---------------------|---------------------|----------------|
| `concept_uri`       | `concept_uri`       | Unchanged |
| —                   | `concept_uri_uuid`   | Extract UUID from URI suffix |
| `skill_type`        | `skill_type`        | Unchanged |
| `reuse_level`       | `reuse_level`       | Unchanged |
| `preferred_label`   | `preferred_label`   | `normalize_text_col()` |
| `description`       | `description`       | `normalize_text_col()` |
| `alt_labels_raw`    | `alt_labels`        | `split_newline_labels()` → Array |
| `hidden_labels_raw` | `hidden_labels`     | `split_newline_labels()` → Array |
| —                   | `alt_labels_count`  | `size(alt_labels)` |
| —                   | `hidden_labels_count` | `size(hidden_labels)` |
| `modified_date`     | `modified_date`     | `parse_date_col()` → DATE |
| *lineage columns*   | *unchanged*         | Preserved for traceability |

#### Occupations Transformation

Similar to skills with the addition of ISCO group and code fields where present.

#### Relations Transformation

| Bronze Column       | Silver Column           | Transformation |
|---------------------|------------------------|----------------|
| `occupation_uri`    | `occupation_uri`       | Unchanged |
| —                   | `occupation_uri_uuid`  | Extract UUID from URI |
| `skill_uri`         | `skill_uri`            | Unchanged |
| —                   | `skill_uri_uuid`       | Extract UUID from URI |
| `occupation_label`  | `occupation_label`     | `normalize_text_col()` |
| `skill_label`       | `skill_label`          | `normalize_text_col()` |
| `relation_type`     | `relation_type`        | `lower(normalize_text_col())` |
| `skill_type`        | `skill_type`           | `normalize_text_col()` |
| *lineage columns*   | *unchanged*            | Preserved |

### 7.5 Spark Transforms Library

Silver transformations leverage reusable utilities from
`platform.spark.transforms`:

| Function                  | Purpose |
|---------------------------|---------|
| `normalize_text_col()`    | Trim + collapse whitespace |
| `split_newline_labels()`  | Split `\n`-separated strings to sorted, distinct array |
| `extract_uri_uuid()`      | Extract UUID (36 chars) from URI suffix |
| `parse_date_col()`        | Parse date string to `DATE` type (handles ISO-8601) |
| `dedupe_by_key()`         | Deterministic deduplication with stable hash tie-breaker |
| `stable_row_hash()`       | SHA-256 of concatenated column values |

#### Label Splitting Example

```python
# Input: "Python\nPython 3\n  Python programming  \nPython"
# Output: ["Python", "Python 3", "Python programming"]  (sorted, deduplicated, trimmed)

split_newline_labels("alt_labels_raw").alias("alt_labels")
```

The function:
1. Normalises `\r\n` and `\r` to `\n`
2. Splits by `\n`
3. Trims each element
4. Filters empty strings
5. Removes duplicates (`array_distinct`)
6. Sorts alphabetically for stable ordering

### 7.6 Deduplication Strategy

Silver ensures exactly one row per canonical key using window-based
deduplication:

```python
# Window partition by the canonical key columns
window = Window.partitionBy("concept_uri", "version", "lang") \
    .orderBy(
        F.col("modified_date").desc_nulls_last(),
        F.col("ingested_at_utc").desc_nulls_last(),
        F.col("__row_hash__").desc()  # Stable tie-breaker
    )

df.withColumn("__row_num__", F.row_number().over(window)) \
  .where(F.col("__row_num__") == 1)
```

The ordering preference:
1. Most recent `modified_date` (from ESCO source)
2. Most recent `ingested_at_utc` (Bronze ingestion timestamp)
3. Deterministic hash tie-breaker (ensures reproducible results)

### 7.7 Iceberg Table Management

Silver tables are managed similarly to Bronze:

- **Namespace**: `sr.sr_silver`
- **Table naming**: `sr.sr_silver.esco_skills`, `sr.sr_silver.esco_occupations`,
  `sr.sr_silver.esco_relations` (no `_raw` suffix)
- **Partitioning**: `(version, lang)`
- **Write mode**: Partition overwrite for idempotent re-runs

### 7.8 Validation

Silver validation runs via `skill-radar validate esco-silver --version ... --lang ...`
and checks:

| Check                      | Description |
|----------------------------|-------------|
| `silver.table.exists.*`    | Silver tables exist in Iceberg catalog |
| `silver.table.non_empty.*` | Tables have rows for the specified partition |
| `silver.schema.contains.*` | Expected columns are present |
| `silver.uniqueness.*`      | No duplicate canonical keys |

---

## 8. Platform Services

### 8.1 LakeLayout

The `LakeLayout` class is the **single source of truth** for all storage paths.
It reads layer names, root prefix, and Iceberg catalog configuration from
`PlatformSettings`.

Key methods:

| Method                      | Returns |
|-----------------------------|---------|
| `landing_zip_key(…)`        | `data/landing/{domain}/{source}/artifact/version={v}/lang={l}/{source}.zip` |
| `landing_manifest_key(…)`   | `data/landing/{domain}/{source}/artifact/version={v}/lang={l}/manifest.json` |
| `bronze_staging_prefix(…)`  | `data/bronze/{domain}/{source}/staging/version={v}/lang={l}/run_id={r}` |
| `iceberg_table_fqn(…)`      | `sr.sr_bronze.esco_skills_raw` or `sr.sr_silver.esco_skills` |
| `iceberg_namespace(…)`      | `sr.sr_bronze` or `sr.sr_silver` |

### 8.2 RuntimeContext

The `platform.runtime` package detects whether the process is running on the
**host** (developer laptop) or inside a **Docker container**, and resolves the
correct S3 endpoint:

| Context | Endpoint             |
|---------|----------------------|
| Host    | `http://localhost:9000` |
| Docker  | `http://minio:9000`  |

### 8.3 S3Client

A thin abstraction over boto3:

- `upload_file()` / `upload_bytes()` — with idempotency guard
- `object_exists()` — HEAD request
- `build_s3_client()` — factory with configurable timeouts

### 8.4 Logging

The logging system provides structured, contextual logging:

- **`init_logging(job_name)`** — one-shot bootstrap with `RunContext`
- **`set_context(**fields)`** — progressively enriches the context mid-run
- **`finalize_logging(upload=…)`** — flushes handlers, optionally uploads logs

Every log line carries `run_id`, `job_name`, `env`, `dataset`, `version`,
`lang`, `spark_app_id`, and `host`.

### 8.5 Configuration

`PlatformSettings` (Pydantic v2) centralises all platform configuration:

```
PlatformSettings
├── platform.environment       ("local" | "staging" | "production")
├── storage
│   ├── s3 (bucket, endpoint_host, endpoint_docker, region, secure)
│   └── iceberg (catalog_name, namespace_prefix, warehouse, required_namespaces)
├── lake
│   ├── root_prefix            ("data")
│   └── layers (landing, bronze, silver, gold)
├── manifest.schema_version    ("1.0.0")
└── logging (level, log_dir, console_format, file_format, logs_bucket)
```

---

## 9. Data Flow — End-to-End

```
                              ESCO Portal
                                  │
                          (manual download)
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │  data/incoming/esco/     │   Dropzone (host filesystem)
                    │  └── esco.zip            │
                    └────────────┬────────────┘
                                 │
                    CLI: skill-radar esco upload
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │            LANDING STAGE                  │
              │  1. Validate (ZIP, columns, version)     │
              │  2. SHA-256 checksum                     │
              │  3. Upload ZIP → MinIO                   │
              │  4. Build + upload manifest.json          │
              └──────────────────┬───────────────────────┘
                                 │
    s3://skillradar-lake/data/landing/taxonomy/esco/artifact/
    └── version=v1.2.1/lang=fr/
        ├── esco.zip
        └── manifest.json
                                 │
                    skill-radar esco bronze
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │            BRONZE STAGE                   │
              │  1. Download ZIP from S3                 │
              │  2. Extract + stage CSVs to S3           │
              │  3. Spark read → validate → transform    │
              │  4. Add lineage columns                  │
              │  5. Write → Iceberg (partition overwrite)│
              └──────────────────┬───────────────────────┘
                                 │
    Iceberg tables (sr.sr_bronze):
    ├── esco_skills_raw       (partitioned by version, lang)
    ├── esco_occupations_raw  (partitioned by version, lang)
    └── esco_relations_raw    (partitioned by version, lang)
                                 │
                    skill-radar esco silver
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │            SILVER STAGE                   │
              │  1. Read Bronze tables (filtered)        │
              │  2. Normalise text fields                │
              │  3. Split labels → arrays                │
              │  4. Parse dates, extract UUIDs           │
              │  5. Deduplicate by canonical key         │
              │  6. Write → Iceberg (partition overwrite)│
              └──────────────────┬───────────────────────┘
                                 │
    Iceberg tables (sr.sr_silver):
    ├── esco_skills           (partitioned by version, lang)
    ├── esco_occupations      (partitioned by version, lang)
    └── esco_relations        (partitioned by version, lang)
                                 │
                                 ▼
                         (Gold stage — future milestone)
```

---

## 10. Operational Reference

### 10.1 Landing an Artifact

```bash
# Upload ESCO ZIP to landing zone
make upload-esco VERSION=v1.2.1 LANG=fr

# Or via CLI directly
uv run skill-radar esco upload --version v1.2.1 --lang fr

# Dry-run (validate only)
uv run skill-radar esco upload --version v1.2.1 --lang fr --dry-run

# Force overwrite
uv run skill-radar esco upload --version v1.2.1 --lang fr --force
```

### 10.2 Running Bronze Extraction

```bash
# Via Makefile (recommended)
make bronze-esco VERSION=v1.2.1 ESCO_LANG=fr

# Via CLI (inside Spark container)
docker compose exec -T spark bash -lc \
  "uv run skill-radar esco bronze --version v1.2.1 --lang fr"

# Process subset of entities
make bronze-esco VERSION=v1.2.1 ESCO_LANG=fr ENTITIES=skills,occupations
```

### 10.3 Running Silver Formatting

```bash
# Via Makefile (recommended)
make silver-esco VERSION=v1.2.1 ESCO_LANG=fr

# Via CLI (inside Spark container)
docker compose exec -T spark bash -lc \
  "uv run skill-radar esco silver --version v1.2.1 --lang fr"

# Process subset of entities
make silver-esco VERSION=v1.2.1 ESCO_LANG=fr ENTITIES=skills
```

### 10.4 Full Pipeline with Validation

```bash
# Run full pipeline: Bronze → Validate → Silver → Validate
make run-esco-silver VERSION=v1.2.1 ESCO_LANG=fr
```

### 10.5 Validation Commands

```bash
# Validate Bronze tables
make validate-esco-bronze VERSION=v1.2.1 ESCO_LANG=fr

# Validate Silver tables
make validate-esco-silver VERSION=v1.2.1 ESCO_LANG=fr
```

### 10.6 Querying Results

```sql
-- Bronze table query
SELECT count(*) FROM sr.sr_bronze.esco_skills_raw
WHERE version = 'v1.2.1' AND lang = 'fr';

-- Silver table query
SELECT concept_uri, preferred_label, alt_labels, alt_labels_count
FROM sr.sr_silver.esco_skills
WHERE version = 'v1.2.1' AND lang = 'fr'
LIMIT 10;

-- Check deduplication effectiveness
SELECT count(*) as bronze_count FROM sr.sr_bronze.esco_skills_raw
WHERE version = 'v1.2.1' AND lang = 'fr';

SELECT count(*) as silver_count FROM sr.sr_silver.esco_skills
WHERE version = 'v1.2.1' AND lang = 'fr';
```

### 10.7 Lake Structure After Full Pipeline

```
s3://skillradar-lake/
├── data/
│   ├── landing/
│   │   └── taxonomy/esco/artifact/
│   │       └── version=v1.2.1/lang=fr/
│   │           ├── esco.zip
│   │           └── manifest.json
│   ├── bronze/
│   │   └── taxonomy/esco/staging/
│   │       └── version=v1.2.1/lang=fr/run_id=abc123/
│   │           └── entity=.../...
└── warehouse/
    ├── sr_bronze.db/
    │   ├── esco_skills_raw/
    │   ├── esco_occupations_raw/
    │   └── esco_relations_raw/
    └── sr_silver.db/
        ├── esco_skills/
        ├── esco_occupations/
        └── esco_relations/
```

---

## 11. Design Decisions & Trade-offs

### 11.1 Why Three Separate Stages?

| Stage   | Persistence Level | Schema Stability | Recovery |
|---------|-------------------|------------------|----------|
| Landing | Raw artifact      | None (ZIP)       | Re-download from source |
| Bronze  | Raw columns + lineage | Semi-stable   | Re-run from Landing |
| Silver  | Typed + normalised | Stable         | Re-run from Bronze |

Separating stages provides:
- **Auditability**: Raw artifacts are always preserved
- **Replayability**: Any stage can be re-run without re-downloading
- **Decoupled failure domains**: A Silver failure doesn't lose Bronze data

### 11.2 Why Deterministic Deduplication?

Non-deterministic deduplication (e.g., `dropDuplicates()` or `DISTINCT`)
produces unpredictable results when there are ties. Using window functions
with a stable hash tie-breaker ensures:

- **Reproducible results**: Same input always produces same output
- **Testability**: Expected outcomes can be asserted in tests
- **Audit trail**: The "winning" row is deterministically selected

### 11.3 Why Partition-Level Overwrite?

Alternatives considered:
- `MERGE INTO`: More complex, requires primary key definition at table level
- `INSERT OVERWRITE`: Full table overwrite is too coarse
- `DELETE + INSERT`: Non-atomic, risks data loss on failure

Partition-level overwrite (`overwritePartitions()`) provides:
- **Atomicity**: Either the partition is fully replaced or unchanged
- **Efficiency**: Only touches affected partitions
- **Simplicity**: No complex merge logic

### 11.4 Why Pure-Function Transforms?

The Spark transforms in `platform.spark.transforms` are defined as pure
functions that return Column expressions. This enables:

- **Composability**: Transforms can be chained and combined
- **Testability**: Each function can be tested in isolation
- **Reusability**: Same transforms work across different datasets

### 11.5 Why Arrays Instead of Exploded Rows?

For label fields (alt_labels, hidden_labels), we chose to store as arrays
rather than exploding to separate rows:

**Array storage (chosen)**:
- Preserves original row count (easier row-count validation)
- Efficient for most read patterns (WHERE concept_uri = ...)
- Smaller storage footprint
- Simpler deduplication logic

**Exploded rows (rejected)**:
- Would require separate label tables
- More complex join patterns
- Higher storage overhead
- Complicates lineage tracking

Arrays can be exploded at query time if needed:
```sql
SELECT concept_uri, explode(alt_labels) as label
FROM sr.sr_silver.esco_skills
```

### 11.6 Why Contract-Driven Column Mapping?

Hardcoding column mappings in Python creates:
- Scattered, hard-to-review column definitions
- Risk of drift between modules
- Requires code changes for schema updates

Contract-driven mapping via `contract.yaml` provides:
- Single source of truth for all column mappings
- Non-engineers can review and modify schemas
- Automated validation against the same contract

---

*Last updated: 2026-03-06 — Milestone 3 (Silver ESCO Formatting)*
