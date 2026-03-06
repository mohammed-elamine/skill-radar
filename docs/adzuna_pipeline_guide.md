# Adzuna Pipeline — Bronze & Silver Data Engineering Guide

> **Audience:** Data engineers, reviewers, and future contributors.
> **Scope:** Design rationale, architecture decisions, module contracts, data flows, and operational runbooks for the Adzuna Bronze and Silver stages within the Skill Radar lakehouse.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Data Source Profile](#2-data-source-profile)
3. [Architecture Overview](#3-architecture-overview)
4. [Contract-Driven Design](#4-contract-driven-design)
5. [Bronze Layer — Raw API Capture](#5-bronze-layer--raw-api-capture)
   - 5.1 [Design Principles](#51-design-principles)
   - 5.2 [API Client](#52-api-client)
   - 5.3 [Row Mapping](#53-row-mapping)
   - 5.4 [Iceberg Table Layout](#54-iceberg-table-layout)
   - 5.5 [Request Log Table](#55-request-log-table)
   - 5.6 [Idempotency & Append Semantics](#56-idempotency--append-semantics)
6. [Silver Layer — Normalization & Deduplication](#6-silver-layer--normalization--deduplication)
   - 6.1 [Design Principles](#61-design-principles)
   - 6.2 [Transform Pipeline](#62-transform-pipeline)
   - 6.3 [Deduplication Strategy](#63-deduplication-strategy)
   - 6.4 [Iceberg Table Layout](#64-iceberg-table-layout)
   - 6.5 [Partition Overwrite for Idempotent Reruns](#65-partition-overwrite-for-idempotent-reruns)
7. [Configuration & Credential Management](#7-configuration--credential-management)
8. [Validation Framework](#8-validation-framework)
9. [Observability & Lineage](#9-observability--lineage)
10. [CLI & Makefile Interface](#10-cli--makefile-interface)
11. [Testing Strategy](#11-testing-strategy)
12. [Production Alignment Decisions](#12-production-alignment-decisions)
13. [Module Map](#13-module-map)
14. [Future Roadmap](#14-future-roadmap)

---

## 1. Executive Summary

Skill Radar ingests live job-market data from the **Adzuna Search API** to power downstream skill analytics. The pipeline follows a **medallion architecture** (Bronze → Silver → Gold) on top of **Apache Iceberg** tables stored in **MinIO** (S3-compatible object storage), orchestrated through **Apache Spark**.

The Adzuna pipeline was designed with explicit production requirements:

- **Raw fidelity in Bronze** — every byte the API returns is preserved.
- **Typed, deduplicated Silver** — a clean analytical dataset ready for Gold.
- **Contract-driven** — a YAML contract governs extraction scope, field expectations, and validation rules.
- **Fully instrumented** — every run produces structured logs, lineage metadata, and run summaries.
- **Idempotent** — safe reruns at both layers without data corruption.
- **Modular** — country-extensible, preset-configurable, with separation between HTTP I/O, schema mapping, and persistence.

---

## 2. Data Source Profile

| Attribute | Value |
|-----------|-------|
| **Provider** | [Adzuna](https://developer.adzuna.com/) |
| **Acquisition method** | REST API (JSON) |
| **Authentication** | `app_id` + `app_key` (header/query parameter) |
| **Refresh cadence** | Daily |
| **Initial scope** | France (`fr`) |
| **Rate limits** | Per-application; managed via pagination caps and backoff |
| **Data model** | Job postings with company, location, salary, category metadata |

The Adzuna API returns paginated JSON results. Each result object contains a flat-ish structure with nested `company`, `location`, and `category` sub-objects. Some fields (salary, coordinates, contract type) are nullable or API-predicted.

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                   Adzuna REST API                        │
│              (paginated JSON responses)                  │
└─────────────────────┬────────────────────────────────────┘
                      │  HTTP GET with retry + backoff
                      ▼
┌──────────────────────────────────────────────────────────┐
│              AdzunaClient  (pure Python)                 │
│  • search_jobs()         → single page                   │
│  • iter_search_pages()   → lazy page iterator            │
│  • search_all()          → aggregated SearchResult       │
│  • Bounded exponential backoff                           │
│  • HTTP error classification (auth/client/server)        │
└─────────────────────┬────────────────────────────────────┘
                      │  SearchResult (dataclass)
                      ▼
┌──────────────────────────────────────────────────────────┐
│          Bronze Extraction  (Spark + Iceberg)            │
│  • _map_job_to_bronze_row()  — flat dict with lineage    │
│  • _write_bronze_append()    — Iceberg append            │
│  • Partitioned by (ingestion_date, country)              │
│  • Request-log sidecar table                             │
│                                                          │
│  Tables:                                                 │
│    sr.sr_bronze.adzuna_jobs_raw                          │
│    sr.sr_bronze.adzuna_request_log_raw                   │
└─────────────────────┬────────────────────────────────────┘
                      │  Spark SQL read from Bronze
                      ▼
┌──────────────────────────────────────────────────────────┐
│         Silver Formatting  (Spark + Iceberg)             │
│  • _transform_bronze_to_silver()  — type parsing, UDFs   │
│  • _deduplicate_jobs()            — window dedup          │
│  • _write_silver_table()          — partition overwrite   │
│  • Partitioned by (country, ingestion_date)              │
│                                                          │
│  Table:                                                  │
│    sr.sr_silver.adzuna_jobs                              │
└──────────────────────────────────────────────────────────┘
```

The pipeline intentionally separates **HTTP I/O** (the `AdzunaClient`, with zero Spark dependency) from **persistence** (Bronze/Silver modules, which require `SparkSession`). This separation enables:

- Unit testing the API client with mock HTTP fixtures.
- Replacing the HTTP layer (e.g., async, batch file ingest) without touching persistence.
- Running Silver independently from Bronze (backfill, reprocessing).

---

## 4. Contract-Driven Design

The Adzuna domain contract (`src/skill_radar/domains/adzuna/contract/contract.yaml`) is the **authoritative specification** for the datasource. It declares:

```yaml
dataset: adzuna

source:
  provider: Adzuna
  acquisition_method: api
  provider_url: https://developer.adzuna.com

supported_countries:
  - code: fr
    api_country_code: fr
    label: France

refresh_cadence: daily

extraction_defaults:
  results_per_page: 50
  max_pages_per_run: 20
  request_timeout_seconds: 30
  max_retries: 3
  backoff_seconds: 2

extraction_presets:
  default_fr:
    country: fr
    max_days_old: 7
    sort_by: date

field_expectations:
  job_identifiers: [id, adref]
  contract_time_values: [full_time, part_time]
  contract_type_values: [permanent, contract]
```

### Why a contract?

| Concern | What the contract solves |
|---------|--------------------------|
| **Scope governance** | Adding a new country is a YAML change, not a code change. |
| **Extraction reproducibility** | Presets pin search parameters (filters, sort, recency window). |
| **Validation alignment** | Field expectations feed directly into validation checks. |
| **Documentation as code** | The contract is the single source of truth for what the pipeline does. |

The contract is parsed at startup into strongly-typed **Pydantic models** (`AdzunaContract`, `ExtractionPreset`, `ExtractionDefaults`, etc.) with runtime validation. A `@lru_cache` ensures the YAML is loaded exactly once per process.

---

## 5. Bronze Layer — Raw API Capture

### 5.1 Design Principles

1. **Preserve raw fidelity.** Every API response field is captured verbatim in `raw_payload_json`. Structured columns are extracted for queryability, but the JSON payload is the ground truth.

2. **Append-only.** Bronze never deletes or overwrites. Each extraction run appends new rows. Re-running the pipeline on the same day produces duplicate rows — this is intentional. Deduplication is a Silver responsibility.

3. **Immutable lineage.** Every row carries its extraction context: `run_id`, `extracted_at_utc`, `ingestion_date`, `country`, `preset`, `search_key`, `page`, `api_result_position`. Future auditors can reconstruct exactly which API call produced which row.

4. **Partitioned for efficient reads.** Tables are partitioned by `(ingestion_date, country)`, enabling Silver to read a single day-country slice without scanning the full table.

### 5.2 API Client

`AdzunaClient` (`src/skill_radar/domains/adzuna/api/client.py`) is a **synchronous, dependency-minimal** HTTP client:

- **Zero Spark dependency.** Pure `requests.Session`, easily testable with `unittest.mock`.
- **Bounded exponential backoff.** Transient 5xx errors and network failures are retried up to `max_retries` times with `backoff * attempt` delay. Auth errors (401/403) and client errors (4xx) are raised immediately — retrying would be pointless.
- **Structured error hierarchy.** Four exception classes (`AdzunaAuthError`, `AdzunaClientError`, `AdzunaServerError`, `AdzunaResponseError`) enable callers to handle failures granularly.
- **Secrets never logged.** `request_params` in `PageResult` scrub `app_id` and `app_key` before being captured in lineage metadata.
- **Pagination-aware.** `iter_search_pages()` yields `PageResult` objects until the API returns fewer results than `results_per_page` (last page signal) or `max_pages` is reached.

```python
# Simplified flow
client = AdzunaClient(app_id=..., app_key=..., results_per_page=50)
result: SearchResult = client.search_all("fr", "default_fr", max_pages=20)
# result.all_results → list[dict]   (raw API JSON objects)
# result.page_results → list[PageResult]   (per-page lineage)
```

### 5.3 Row Mapping

`_map_job_to_bronze_row()` converts each API JSON object into a flat dictionary with 31 columns:

| Category | Columns | Notes |
|----------|---------|-------|
| **Identity** | `job_id`, `adref` | Stringified from API `id` field |
| **Content** | `title`, `description`, `created_at_raw`, `redirect_url` | Raw text, no normalization |
| **Geolocation** | `latitude`, `longitude`, `location_display_name`, `location_area_json` | Area serialized as JSON array |
| **Compensation** | `salary_min`, `salary_max`, `salary_is_predicted_raw` | Kept as raw strings/floats |
| **Contract** | `contract_time_raw`, `contract_type_raw` | Raw API enum values |
| **Taxonomy** | `category_tag`, `category_label` | Adzuna's job category |
| **Company** | `company_display_name`, `company_canonical_name` | Nested extraction |
| **Raw payload** | `raw_payload_json` | Full API JSON for replay |
| **Lineage** | `source_system`, `country`, `preset`, `search_key`, `search_params_json`, `page`, `results_per_page`, `api_result_position`, `extracted_at_utc`, `ingestion_date`, `run_id` | 11 lineage columns |

All nested objects (`location`, `category`, `company`) are defensively accessed with `.get()` and `or {}` guards to handle null/missing sub-objects without exceptions.

### 5.4 Iceberg Table Layout

```
sr.sr_bronze.adzuna_jobs_raw
├── Partitioned by: ingestion_date (string), country (string)
├── Format: Parquet (Iceberg format-version 2)
├── Write mode: Append
└── Table properties:
    ├── format-version = 2
    └── write.format.default = parquet
```

The fully-qualified name (FQN) is generated by `LakeLayout.adzuna_bronze_jobs_raw_fqn()`, which delegates to the generic `iceberg_table_fqn("bronze", "adzuna", "jobs", raw=True)`. This ensures naming consistency across all datasets.

### 5.5 Request Log Table

Every extraction run also writes to `sr.sr_bronze.adzuna_request_log_raw` — a sidecar observability table with 14 columns:

| Column | Purpose |
|--------|---------|
| `page` | API page number |
| `request_params_json` | Query parameters (secrets scrubbed) |
| `http_status` | HTTP response code |
| `response_count` | Number of results on this page |
| `request_started_at_utc` / `request_finished_at_utc` | Timing brackets |
| `duration_ms` | Latency per page |
| `success` | Boolean |
| `error_message` | Error detail if failed |
| `run_id`, `ingestion_date`, `country`, `preset`, `source_system` | Lineage |

This table enables:
- **API performance monitoring** (latency per page, degradation trends).
- **Cost attribution** (pages fetched per run).
- **Debugging** (replay exact query parameters for a failed page).

### 5.6 Idempotency & Append Semantics

Bronze is append-only by design. If `adzuna-bronze` runs twice on the same day:
- Two sets of rows appear in the same `ingestion_date` partition.
- Each set carries a distinct `run_id` and `extracted_at_utc`.
- Silver resolves this by deduplicating on `(country, job_id)` and keeping the latest.

This approach avoids the complexity of upsert/merge at the Bronze level while preserving full extraction history.

---

## 6. Silver Layer — Normalization & Deduplication

### 6.1 Design Principles

1. **One fact table.** Silver produces a single `adzuna_jobs` table — a clean, typed, deduplicated dataset for downstream analytics and Gold joins.

2. **Typed, not interpreted.** Timestamps are parsed, nulls are properly typed, booleans are derived from raw string values. But no NLP, no skill extraction, no enrichment — that belongs in Gold.

3. **Deterministic deduplication.** `(country, job_id)` is the business key. When duplicates exist, the latest Bronze record (by `bronze_extracted_at_utc`) wins. Null `job_id` rows are preserved without dedup (they may carry partial data useful for coverage analysis).

4. **Idempotent reruns.** Partition overwrite semantics: re-running Silver for the same `(country, ingestion_date)` replaces the partition atomically without affecting other partitions.

### 6.2 Transform Pipeline

`_transform_bronze_to_silver()` applies the following transformations in order:

| Step | Transformation | Input Column(s) | Output Column(s) |
|------|---------------|------------------|-------------------|
| 1 | Parse JSON array | `location_area_json` | `location_area` (array<string>) |
| 2 | Derive location hierarchy | `location_area` | `location_country`, `location_region`, `location_subregion` |
| 3 | Parse timestamp | `created_at_raw` | `posted_at_utc` (timestamp), `posted_date` (string) |
| 4 | Compute salary mean | `salary_min`, `salary_max` | `salary_mean` (double) |
| 5 | Parse boolean | `salary_is_predicted_raw` | `salary_is_predicted` (boolean) |
| 6 | Derive contract flags | `contract_time_raw`, `contract_type_raw` | `is_full_time`, `is_part_time`, `is_permanent`, `is_contract` |
| 7 | Normalize text | `title`, `description`, `company_display_name`, `location_display_name` | `title_normalized`, `description_normalized`, `company_normalized`, `location_normalized` |
| 8 | Add Silver lineage | — | `silver_run_id`, `formatted_at_utc`, `bronze_extracted_at_utc`, `bronze_run_id` |

**Location hierarchy derivation** uses the Adzuna area array convention `[country, region, city]`:
```
["France", "Île-de-France", "Paris"]
  → location_country = "France"
  → location_region  = "Île-de-France"
  → location_subregion = "Paris"
```

**Text normalization** applies `TRIM → collapse whitespace → lowercase` to produce searchable helper columns without destroying the original values.

**Boolean parsing** handles Adzuna's inconsistent representations (`"0"`, `"1"`, `"true"`, `"false"`, `null`) via a Spark expression.

### 6.3 Deduplication Strategy

```python
window = Window.partitionBy("country", "job_id") \
               .orderBy(col("bronze_extracted_at_utc").desc_nulls_last(),
                        col("adref").desc_nulls_last())

# Keep row_number == 1 (latest extraction)
```

Key design choices:
- **Null `job_id` rows are excluded from dedup** and preserved as-is. This avoids silently dropping rows that might represent incomplete but valuable data.
- **`adref` as tiebreaker.** When two rows share the same `job_id` and `extracted_at_utc`, the latest `adref` breaks the tie deterministically.
- **Separate DataFrames.** The implementation explicitly splits null-ID vs valid-ID rows, deduplicates only the valid set, and unions them back. This avoids Spark's null-handling quirks in window functions.

### 6.4 Iceberg Table Layout

```
sr.sr_silver.adzuna_jobs
├── Partitioned by: country (string), ingestion_date (string)
├── Format: Parquet (Iceberg format-version 2)
├── Write mode: Partition overwrite (dynamic)
├── 41 columns (see schema below)
└── Table properties:
    ├── format-version = 2
    └── write.format.default = parquet
```

**Silver schema** (41 columns):

| Group | Columns |
|-------|---------|
| Identity | `job_id`, `adref`, `source_system`, `country` |
| Content | `job_title`, `job_description`, `posted_at_utc`, `posted_date`, `job_url` |
| Company | `company_name`, `company_canonical_name` |
| Category | `category_tag`, `category_label` |
| Location | `location_display_name`, `location_area`, `location_country`, `location_region`, `location_subregion`, `latitude`, `longitude` |
| Compensation | `salary_min`, `salary_max`, `salary_mean`, `salary_is_predicted` |
| Contract | `contract_time`, `contract_type`, `is_full_time`, `is_part_time`, `is_permanent`, `is_contract` |
| Normalization | `title_normalized`, `description_normalized`, `company_normalized`, `location_normalized` |
| Lineage | `bronze_extracted_at_utc`, `bronze_run_id`, `search_key`, `search_params_json`, `silver_run_id`, `formatted_at_utc`, `ingestion_date` |

### 6.5 Partition Overwrite for Idempotent Reruns

Silver uses Iceberg's **partition overwrite** (`writeTo().overwritePartitions()`) instead of full table overwrite. This means:

- Re-running Silver for `country=fr, ingestion_date=2026-03-06` replaces only that partition.
- Data for other countries or dates is untouched.
- Failed partial writes are rolled back atomically (Iceberg ACID guarantees).

This is critical for production because:
- Daily schedules can safely retry without duplicating data.
- Backfills for a specific date don't corrupt the rest of the table.
- Hot partitions can be refreshed independently.

---

## 7. Configuration & Credential Management

Configuration follows a **layered override** pattern:

```
contract.yaml  →  defaults.yaml  →  environment variables  →  CLI flags
(base truth)      (platform defaults)  (runtime overrides)     (user overrides)
```

### Platform configuration (`config/defaults.yaml`)

```yaml
adzuna:
  base_url: "https://api.adzuna.com/v1/api"
  default_country: fr
  results_per_page: 50
  max_pages_per_run: 20
  request_timeout_seconds: 30
  max_retries: 3
  backoff_seconds: 2
  default_preset: default_fr
```

### Environment variable overrides

Every platform config field can be overridden via environment variable:

| Variable | Config field | Example |
|----------|-------------|---------|
| `SKILLRADAR_ADZUNA_BASE_URL` | `adzuna.base_url` | Custom proxy URL |
| `SKILLRADAR_ADZUNA_DEFAULT_COUNTRY` | `adzuna.default_country` | `gb` |
| `SKILLRADAR_ADZUNA_RPP` | `adzuna.results_per_page` | `25` |
| `SKILLRADAR_ADZUNA_MAX_PAGES` | `adzuna.max_pages_per_run` | `10` |
| `SKILLRADAR_ADZUNA_TIMEOUT` | `adzuna.request_timeout_seconds` | `60` |
| `SKILLRADAR_ADZUNA_RETRIES` | `adzuna.max_retries` | `5` |
| `SKILLRADAR_ADZUNA_BACKOFF` | `adzuna.backoff_seconds` | `5` |
| `SKILLRADAR_ADZUNA_DEFAULT_PRESET` | `adzuna.default_preset` | `custom_gb` |

### Credential isolation

API credentials are **never stored in config files or YAML**. They are resolved at runtime from environment variables:

| Variable | Required | Purpose |
|----------|----------|---------|
| `ADZUNA_APP_ID` | Yes | Adzuna application identifier |
| `ADZUNA_APP_KEY` | Yes | Adzuna application secret key |

Missing credentials raise `KeyError` immediately at pipeline start — no silent fallback. Credentials are loaded through `Settings.from_env()` and never appear in logs, lineage metadata, or run summaries.

---

## 8. Validation Framework

Adzuna validation integrates with the platform's existing `NamedCheck → run_checks() → finalize_report()` framework.

### Bronze checks (11 checks)

| Check | What it verifies |
|-------|------------------|
| `namespace_exists` | `sr_bronze` Iceberg namespace is present |
| `jobs_raw.table_exists` | Bronze jobs table exists |
| `jobs_raw.non_empty` | Table has at least one row |
| `jobs_raw.schema` | All 31 required columns are present |
| `jobs_raw.job_id_population` | ≥ 95% of rows have a non-empty `job_id` |
| `jobs_raw.raw_payload_parseable` | Sampled `raw_payload_json` parses as valid JSON |
| `jobs_raw.lineage` | Lineage columns (`source_system`, `country`, `run_id`, `ingestion_date`) present |
| `jobs_raw.page_metadata` | `page >= 1` and `results_per_page > 0` |
| `request_log.table_exists` | Request log table exists |
| `request_log.non_empty` | Request log has at least one row |
| `request_log.schema` | All 14 log columns present |

### Silver checks (9 checks)

| Check | What it verifies |
|-------|------------------|
| `namespace_exists` | `sr_silver` Iceberg namespace is present |
| `table_exists` | Silver jobs table exists |
| `non_empty` | Table has at least one row |
| `schema` | All 41 required columns are present |
| `key_population` | ≥ 95% of rows have `job_id`, `job_title`, and `country` populated |
| `salary_consistency` | `salary_min <= salary_max` when both are non-null |
| `coordinate_sanity` | `latitude ∈ [-90, 90]`, `longitude ∈ [-180, 180]` |
| `no_duplicates` | No duplicate `(country, job_id)` pairs |
| `lineage` | Lineage columns present |

### Running validation

```bash
# Bronze
make validate-adzuna-bronze
skill-radar validate adzuna-bronze

# Silver
make validate-adzuna-silver
skill-radar validate adzuna-silver
```

---

## 9. Observability & Lineage

Every pipeline run produces a JSON run summary written to the `logs/validation/` directory:

```
logs/validation/adzuna_bronze/2026-03-06_a1b2c3d4e5f6.json
logs/validation/adzuna_silver/2026-03-06_a1b2c3d4e5f6.json
```

### Bronze run summary fields

```json
{
  "run_id": "a1b2c3d4e5f6",
  "country": "fr",
  "preset": "default_fr",
  "pages_fetched": 20,
  "rows_ingested": 985,
  "rows_written": 985,
  "target_table": "sr.sr_bronze.adzuna_jobs_raw",
  "request_log_table": "sr.sr_bronze.adzuna_request_log_raw",
  "request_log_rows": 20,
  "extracted_at_utc": "2026-03-06T08:15:42+00:00",
  "ingestion_date": "2026-03-06",
  "success": true,
  "error": ""
}
```

### Silver run summary fields

```json
{
  "run_id": "a1b2c3d4e5f6",
  "country": "fr",
  "ingestion_date": "2026-03-06",
  "input_row_count": 1970,
  "output_row_count": 985,
  "duplicates_removed": 985,
  "target_table": "sr.sr_silver.adzuna_jobs",
  "bronze_table": "sr.sr_bronze.adzuna_jobs_raw",
  "formatted_at_utc": "2026-03-06T08:20:15+00:00",
  "success": true,
  "error": ""
}
```

### Row-level lineage

Every Bronze row carries:
- `run_id` — ties it to a specific extraction run.
- `extracted_at_utc` — when the API call completed.
- `search_key` — deterministic search fingerprint (e.g. `country=fr|preset=default_fr`).
- `search_params_json` — exact query parameters (secrets scrubbed).
- `page`, `api_result_position` — exact position within the API response.

Every Silver row carries the above (inherited from Bronze) plus:
- `silver_run_id` — formatting run identifier.
- `formatted_at_utc` — when Silver formatting completed.
- `bronze_extracted_at_utc` — preserved for cross-layer traceability.
- `bronze_run_id` — which Bronze run produced this row.

This enables full **forward and backward lineage tracing** from a Silver row back to the exact API page and position that produced it.

---

## 10. CLI & Makefile Interface

### CLI commands

```bash
# Bronze extraction
skill-radar adzuna bronze [--preset NAME] [--country CC] [--max-pages N] [--results-per-page N] [--quiet]

# Silver formatting
skill-radar adzuna silver [--country CC] [--ingestion-date YYYY-MM-DD] [--quiet]

# Validation
skill-radar validate adzuna-bronze
skill-radar validate adzuna-silver
```

### Makefile targets

| Target | Description | Runs inside |
|--------|-------------|-------------|
| `make adzuna-bronze` | Extract from API → Bronze Iceberg | Spark container |
| `make adzuna-silver` | Transform Bronze → Silver Iceberg | Spark container |
| `make validate-adzuna-bronze` | Run Bronze validation checks | Spark container |
| `make validate-adzuna-silver` | Run Silver validation checks | Spark container |
| `make run-adzuna` | Full pipeline: bronze → validate → silver → validate | Spark container |

### Makefile variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `ADZUNA_COUNTRY` | `fr` | Country code |
| `ADZUNA_PRESET` | `default_fr` | Extraction preset |
| `ADZUNA_MAX_PAGES` | (from config) | Override page limit |
| `ADZUNA_RESULTS_PER_PAGE` | (from config) | Override results per page |
| `ADZUNA_INGESTION_DATE` | (today) | Override Silver date scope |

```bash
# Example: extract UK jobs, max 5 pages
make adzuna-bronze ADZUNA_COUNTRY=gb ADZUNA_MAX_PAGES=5

# Example: reprocess Silver for a specific date
make adzuna-silver ADZUNA_COUNTRY=fr ADZUNA_INGESTION_DATE=2026-03-01

# Example: full pipeline
make run-adzuna
```

---

## 11. Testing Strategy

### Unit tests (68 tests, no Spark dependency)

| Module | Tests | What is covered |
|--------|-------|-----------------|
| `test_contract.py` | 10 | Contract loading, YAML parsing, preset lookup, field expectations |
| `test_api_client.py` | 18 | Parameter construction, pagination, error handling, retry, lineage scrubbing |
| `test_bronze_mapping.py` | 17 | Row mapping (all fields), null handling, `_safe_float`, request log mapping |
| `test_silver_helpers.py` | 11 | `_safe_parse_json_array`, `_location_hierarchy` edge cases |
| `test_layout_and_config.py` | 7 | AdzunaConfig defaults, LakeLayout FQN generation |
| `test_errors.py` | 5 | Error hierarchy, inheritance, message preservation |

### Test fixtures

```
tests/fixtures/adzuna/
├── valid_search_response.json      # 3 jobs, all fields populated
├── null_fields_response.json       # 2 jobs, sparse/null fields
└── duplicate_jobs_response.json    # 3 jobs, 2 share same job_id
```

### Testing philosophy

- **Unit tests are Spark-free.** All row-mapping, JSON parsing, config loading, and HTTP client logic is tested with pure Python. This keeps the test suite fast (~0.2s for all 68 tests).
- **Mocking over integration.** API calls are mocked with `unittest.mock.patch` on the `requests.Session.get` method. No HTTP server is instantiated.
- **Fixtures are realistic.** JSON fixtures mirror real Adzuna API responses with French job postings, Unicode characters, and edge cases (null salaries, missing companies).
- **Integration tests** (not yet implemented) will run inside the Spark container with `pytest.mark.integration`, following the established ESCO pattern.

---

## 12. Production Alignment Decisions

This section documents key architectural choices and why they align with production requirements.

### 12.1 LakeLayout as single source of truth

Every Iceberg FQN in the codebase is generated by `LakeLayout`. No module constructs table names from strings. This means:

- Renaming a namespace prefix is a single config change.
- A typo in a table name is caught at one point, not scattered across modules.
- Cross-dataset consistency (ESCO and Adzuna follow the same naming pattern) is enforced structurally.

### 12.2 Iceberg format-version 2

All tables are created with `format-version = 2`, which enables:
- Row-level deletes (for future GDPR compliance).
- Equality deletes (for efficient merge operations in Gold).
- Improved statistics for query planning.

### 12.3 Separation of HTTP and persistence layers

The `AdzunaClient` returns Python dataclasses (`SearchResult`, `PageResult`), not Spark DataFrames. Bronze converts these to `list[dict]` before calling `spark.createDataFrame()`. This boundary enables:

- Testing the client in pure Python.
- Swapping the HTTP layer (e.g., `httpx`, async) without touching Spark code.
- Running the client standalone for debugging or data exploration.

### 12.4 Contract vs. config distinction

- **Contract** (`contract.yaml`) defines the domain: what countries are supported, what fields the API returns, what presets exist. It changes rarely and is versioned with the code.
- **Config** (`defaults.yaml` + env vars) defines runtime behavior: timeouts, page limits, URLs. It changes per environment (dev / staging / prod).

Pydantic validates both at load time. A malformed contract or config raises an exception before any API call is made.

### 12.5 Defensive null handling

The API client and row-mapping layer use defensive patterns everywhere:
- `job.get("location", {}) or {}` — handles both missing keys and explicit `null`.
- `_safe_float()` — converts to float or returns `None`, never raises.
- JSON serialization uses `ensure_ascii=False` to preserve French characters.
- Silver's `_safe_parse_json_array()` returns `[]` for any unparseable input.

### 12.6 Credential security

- Credentials are loaded from environment variables via `Settings.from_env()`.
- Missing credentials raise `KeyError` immediately — no silent empty-string fallback.
- `PageResult.request_params` explicitly scrub `app_id` and `app_key` before going into lineage columns.
- No credential value appears in logs, run summaries, or Iceberg table data.

### 12.7 Error classification and retry

The 4-class error hierarchy enables production alerting patterns:

| Error class | HTTP codes | Retry? | Alert level |
|-------------|-----------|--------|-------------|
| `AdzunaAuthError` | 401, 403 | No | Critical (credential rotation needed) |
| `AdzunaClientError` | 400, 404, 422 | No | Warning (check query parameters) |
| `AdzunaServerError` | 500+ / network | Yes (bounded backoff) | Info (transient) → Error (all retries exhausted) |
| `AdzunaResponseError` | 200 + bad body | No | Error (API contract violation) |

### 12.8 Structured logging and context

All logging uses Python's `logging` module with structured context injection via `init_logging()` / `set_context()`. Run IDs propagate from the CLI layer through Bronze and Silver, enabling log correlation across pipeline stages.

---

## 13. Module Map

```
src/skill_radar/
├── domains/adzuna/
│   ├── __init__.py
│   ├── contract/
│   │   ├── contract.yaml           # Domain contract (YAML)
│   │   ├── models.py               # Pydantic models for contract
│   │   ├── loader.py               # Cached YAML → Pydantic loader
│   │   └── __init__.py             # Public API re-exports
│   ├── api/
│   │   ├── client.py               # HTTP client (search_jobs, pagination, retry)
│   │   ├── errors.py               # 4-class exception hierarchy
│   │   └── __init__.py
│   ├── bronze/
│   │   ├── extract.py              # API → Bronze Iceberg (row mapping + write)
│   │   └── __init__.py
│   └── silver/
│       ├── format.py               # Bronze → Silver Iceberg (transforms + dedup)
│       └── __init__.py
├── cli/
│   ├── adzuna.py                   # Click commands: adzuna bronze, adzuna silver
│   └── validate.py                 # Click commands: validate adzuna-bronze/silver
├── config/
│   ├── defaults.yaml               # Platform defaults (incl. adzuna section)
│   ├── models.py                   # Pydantic: AdzunaConfig + PlatformSettings
│   ├── loader.py                   # YAML + env override loader
│   └── adzuna.py                   # Credentials loader (Settings.from_env)
└── platform/
    ├── lake/layout.py              # LakeLayout (FQN generation)
    └── validate/checks/adzuna.py   # Bronze + Silver validation checks

tests/
├── fixtures/adzuna/
│   ├── valid_search_response.json
│   ├── null_fields_response.json
│   └── duplicate_jobs_response.json
└── unit/domains/adzuna/
    ├── test_contract.py
    ├── test_api_client.py
    ├── test_bronze_mapping.py
    ├── test_silver_helpers.py
    ├── test_layout_and_config.py
    └── test_errors.py
```

---

## 14. Future Roadmap

| Item | Layer | Description |
|------|-------|-------------|
| **Multi-country support** | All | Add `gb`, `de` presets to contract; pipeline already supports it via `--country` flag |
| **Integration tests** | Test | End-to-end Spark tests inside Docker (following ESCO `test_bronze_extraction.py` pattern) |
| **Airflow DAG** | Orchestration | Daily scheduled `adzuna-bronze → validate → silver → validate` chain with retry and alerting |
| **Incremental Silver** | Silver | Read only new Bronze rows since last Silver run (using Iceberg snapshots or watermarks) |
| **Gold layer** | Gold | Skill extraction from `job_description`, ESCO skill matching, demand aggregations |
| **Schema evolution** | Platform | Handle API field additions gracefully via Iceberg schema evolution |
| **S3 run summary upload** | Observability | Push run summaries to `skillradar-logs` bucket for centralized monitoring |
| **Data retention policy** | Bronze | Configurable TTL for Bronze partitions (e.g., keep 90 days of raw history) |
