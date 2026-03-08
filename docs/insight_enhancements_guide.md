# Insight Enhancements — Gold Analytics & ML Segmentation

> **Scope**: Three new Gold analytical datasets, one ML feature (KMeans
> clustering), full Elasticsearch/Kibana integration, and pipeline
> hardening.<br>
> **Engine**: PySpark 3.5 + Spark MLlib + Apache Iceberg<br>
> **Philosophy**: Small-win, high-ROI improvements that reuse existing Gold
> tables — no new ingestion, no new Bronze/Silver processing.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Dataset A — Emerging-Skill Signals](#3-dataset-a--emerging-skill-signals)
4. [Dataset B — Occupation Market Daily](#4-dataset-b--occupation-market-daily)
5. [Dataset C — Skill Demand Segments (ML)](#5-dataset-c--skill-demand-segments-ml)
6. [Orchestrator Integration](#6-orchestrator-integration)
7. [Configuration Reference](#7-configuration-reference)
8. [Search Serving Layer](#8-search-serving-layer)
9. [Kibana Dashboard — Emerging Signals](#9-kibana-dashboard--emerging-signals)
10. [Validation & Observability](#10-validation--observability)
11. [Pipeline Hardening](#11-pipeline-hardening)
12. [Module Map](#12-module-map)
13. [Testing](#13-testing)

---

## 1. Executive Summary

This enhancement adds **three new Gold-layer analytical datasets** derived
entirely from existing Gold tables (zero new data sources required):

| Dataset | Grain | ML? | Purpose |
|---|---|---|---|
| `gold_skill_emerging_daily` | skill × date × country | No | Momentum, acceleration, novelty scores to surface rising skills |
| `gold_occupation_market_daily` | occupation × date × country | No | Occupation-level demand metrics (job count, skill breadth, match quality) |
| `gold_skill_demand_segments_daily` | skill × date × country | **Yes** | KMeans clustering into 4 market segments (niche → dominant) |

All three are:
- Written to Iceberg (partitioned by `country`, `ingestion_date`)
- Exported to Elasticsearch with dedicated index mappings
- Surfaced in a new Kibana dashboard ("Emerging Skills & Market Signals")
- Wrapped in **soft-fail** semantics so failures never break the core pipeline
- Fully validated through the existing validation framework

---

## 2. Architecture Overview

```
 ┌──────────────────────────────────────────────────────────────┐
 │                   Existing Gold Tables                       │
 │  skill_demand_daily · salary_by_skill_daily ·                │
 │  occupation_skill_graph · job_skill_matches ·                │
 │  job_occupation_matches                                      │
 └───────┬──────────────────┬───────────────────┬───────────────┘
         │                  │                   │
   ┌─────▼──────┐   ┌──────▼───────┐   ┌───────▼───────┐
   │  Phase 5   │   │  Phase 6     │   │  Phase 7      │
   │ Emerging   │   │ Occ Market   │   │ KMeans        │
   │ Signals    │   │ Analytics    │   │ Segments      │
   │ (PySpark)  │   │ (PySpark)    │   │ (Spark MLlib) │
   └─────┬──────┘   └──────┬───────┘   └───────┬───────┘
         │                  │                   │
   ┌─────▼──────────────────▼───────────────────▼───────┐
   │               Iceberg (sr.sr_gold)                  │
   │  gold_skill_emerging_daily                          │
   │  gold_occupation_market_daily                       │
   │  gold_skill_demand_segments_daily                   │
   └─────┬──────────────────┬───────────────────┬───────┘
         │                  │                   │
   ┌─────▼──────────────────▼───────────────────▼───────┐
   │           Elasticsearch indices                     │
   │  skillradar-skill-emerging-daily-{country}-{date}   │
   │  skillradar-occupation-market-daily-{country}-{date}│
   │  skillradar-skill-demand-segments-daily-…           │
   └─────┬──────────────────────────────────────────────┘
         │
   ┌─────▼──────────────────────────────────────────────┐
   │  Kibana — Dashboard D: "Emerging Skills &          │
   │           Market Signals"                           │
   │  • KPI card  • Bar chart  • Line chart  • Table    │
   └────────────────────────────────────────────────────┘
```

**Key design choice**: Phases 5–7 run *after* the core analytics (Phases 1–4)
inside the same Spark session.  Each is wrapped in a soft-fail `try/except`
so that a failure in any enrichment phase logs a warning and continues
without aborting the pipeline.

---

## 3. Dataset A — Emerging-Skill Signals

**Module**: `src/skill_radar/domains/gold/analytics/emerging_skills.py`
**Function**: `compute_skill_emerging_daily(skill_demand_df, *, ingestion_date, weights)`
**Iceberg table**: `sr.sr_gold.gold_skill_emerging_daily`

### Concept

Surfaces skills that are gaining traction by computing three signal scores:

| Score | Definition | Range |
|---|---|---|
| **Momentum** | `percent_rank()` of `jobs_count` within the same `(country, date)` partition — higher demand → higher score | [0, 1] |
| **Acceleration** | Day-over-day change ratio `(today - yesterday) / max(yesterday, 1)`, clamped to [0, 1]. Falls back to 0.0 when no prior day exists. | [0, 1] |
| **Novelty** | `1.0 / count(distinct ingestion_date)` for that skill across all available history — a first-time observation gets 1.0. | (0, 1] |
| **Composite** | Configurable weighted sum: `w_m × momentum + w_a × accel + w_n × novelty`, clamped to [0, 1] | [0, 1] |

### Implementation Details

1. **Input**: The full `skill_demand_daily` table (all dates for the target
   country), enabling historical comparisons.
2. **Momentum**: Uses a `Window.partitionBy("country", "ingestion_date")`
   with `orderBy(jobs_count.asc())` for `percent_rank()`.
3. **Acceleration**: A self-join between the target date and the previous day
   on `(country, esco_skill_concept_uri)`.
4. **Novelty**: A `countDistinct("ingestion_date")` grouped by skill URI,
   then `1.0 / count`.
5. **Composite**: Weighted sum with default weights `(0.4, 0.3, 0.3)`,
   configurable per environment.
6. **Output**: Single-date partition with lineage columns (`gold_meta_run_id`,
   `gold_meta_generated_at_utc`, `esco_version`, `esco_lang`).

### Schema

```
ingestion_date            DATE
country                   STRING
esco_skill_concept_uri    STRING
esco_skill_preferred_label STRING
jobs_count                LONG
momentum_score            DOUBLE
acceleration_score        DOUBLE
novelty_score             DOUBLE
emerging_composite_score  DOUBLE
gold_meta_run_id          STRING
gold_meta_generated_at_utc STRING
esco_version              STRING
esco_lang                 STRING
```

---

## 4. Dataset B — Occupation Market Daily

**Module**: `src/skill_radar/domains/gold/analytics/occupation_market.py`
**Function**: `compute_occupation_market_daily(occ_matches_df, skill_matches_df)`
**Iceberg table**: `sr.sr_gold.gold_occupation_market_daily`

### Concept

Provides an occupation-level view of daily market conditions by cross-joining
Gold job-occupation and job-skill matches:

| Metric | Definition |
|---|---|
| `total_jobs_count` | Distinct jobs matched to this occupation |
| `unique_skills_count` | Number of distinct ESCO skills co-occurring with the occupation's jobs |
| `avg_match_score` | Mean match score across all job-occupation pairs |

### Implementation Details

1. **Step 1** — Group `job_occupation_matches` by `(ingestion_date, country,
   esco_occupation_concept_uri)` → `countDistinct("job_id")` and
   `avg("match_score")`.
2. **Step 2** — Join the occupation's jobs with `job_skill_matches` on
   `job_id` to count unique skill URIs (`countDistinct`).
3. **Step 3** — Left-join skill counts back into occupation aggregates,
   `fillna(0)` for occupations with no skill matches.

### Schema

```
ingestion_date                    DATE
country                           STRING
esco_occupation_concept_uri       STRING
esco_occupation_preferred_label   STRING
total_jobs_count                  LONG
unique_skills_count               LONG
avg_match_score                   DOUBLE
gold_meta_run_id                  STRING
gold_meta_generated_at_utc        STRING
esco_version                      STRING
esco_lang                         STRING
```

---

## 5. Dataset C — Skill Demand Segments (ML)

**Module**: `src/skill_radar/domains/gold/analytics/skill_segments.py`
**Function**: `compute_skill_demand_segments(skill_demand_df, *, ingestion_date, config)`
**Iceberg table**: `sr.sr_gold.gold_skill_demand_segments_daily`
**ML Library**: PySpark MLlib (`pyspark.ml.clustering.KMeans`,
`pyspark.ml.feature.VectorAssembler`)

### Concept

Clusters skills into **4 market segments** based on demand intensity signals,
using unsupervised KMeans:

| Segment | Label | Interpretation |
|---|---|---|
| 0 | `niche` | Low demand, few companies/locations |
| 1 | `growing` | Moderate demand, expanding presence |
| 2 | `established` | Solid demand, broad market |
| 3 | `dominant` | Highest demand, ubiquitous across market |

### Implementation Details

1. **Features**: `VectorAssembler` combines three columns from
   `skill_demand_daily`:
   - `jobs_count` — raw job count
   - `unique_companies_count` — employer diversity
   - `unique_locations_count` — geographic spread
2. **Clustering**: `KMeans(k=4, seed=42, maxIter=20)` with Euclidean distance.
   All parameters are configurable via `MLSegmentsConfig`.
3. **Cluster labelling**: After fitting, the 4 cluster centres are sorted by
   L2 norm (ascending).  The sorted rank (0–3) is mapped to segment names.
   This ensures a stable interpretation regardless of the random
   initialization.
4. **Fallback**: When fewer than `min_rows` (default: 20) skills exist in
   the partition, every row is assigned `segment_id=-1`,
   `segment_label="unclustered"` — no model is fitted.
5. **Output**: One row per skill with `segment_id` (int) and
   `segment_label` (str), plus the original demand features.

### Schema

```
ingestion_date              DATE
country                     STRING
esco_skill_concept_uri      STRING
esco_skill_preferred_label  STRING
jobs_count                  LONG
unique_companies_count      LONG
unique_locations_count      LONG
segment_id                  INT
segment_label               STRING
gold_meta_run_id            STRING
gold_meta_generated_at_utc  STRING
esco_version                STRING
esco_lang                   STRING
```

### Why KMeans?

| Consideration | Decision |
|---|---|
| Simplicity | KMeans is the simplest unsupervised ML model; easy to explain, debug, and tune |
| Spark-native | `pyspark.ml.clustering.KMeans` runs distributed on the cluster — no data export needed |
| Deterministic (seed) | Fixed seed (42) + norm-sorted labelling → reproducible segments |
| Configurable | `k`, `maxIter`, `seed`, `features`, and `segment_names` are all in YAML/env config |
| Graceful fallback | Partitions too small for meaningful clustering skip the model entirely |

---

## 6. Orchestrator Integration

**Module**: `src/skill_radar/domains/gold/analytics/orchestrator.py`

The three new datasets are integrated as Phases 5–7 of the Gold analytics
orchestrator, appended after the core phases (1–4):

| Phase | Dataset | Input | Soft-fail? |
|---|---|---|---|
| 1 | `gold_skill_demand_daily` | Silver jobs + ESCO skills | No (core) |
| 2 | `gold_salary_by_skill_daily` | Silver jobs + ESCO skills | No (core) |
| 3 | `gold_occupation_skill_graph` | Gold matches | No (core) |
| 4 | (reserved) | — | — |
| **5** | **`gold_skill_emerging_daily`** | Phase 1 output | **Yes** |
| **6** | **`gold_occupation_market_daily`** | Gold occ + skill matches | **Yes** |
| **7** | **`gold_skill_demand_segments_daily`** | Phase 1 output | **Yes** |

### Soft-Fail Pattern

```python
# Phase 5 ── emerging-skill signals (soft-fail)
try:
    emerging_df = compute_skill_emerging_daily(...)
    write_iceberg_table(emerging_df, ...)
    result.skill_emerging_rows = emerging_df.count()
except Exception:
    logger.warning("Phase 5 (emerging-skill signals) failed", exc_info=True)
```

Each enrichment phase:
- Is wrapped in `try/except Exception`
- Logs a warning with full traceback on failure
- Sets the row count to 0 (the result model tracks per-phase counts)
- **Does not abort** the pipeline — subsequent phases still run

This ensures the 3 core datasets (demand, salary, graph) are always produced,
and the enrichment datasets are best-effort.

---

## 7. Configuration Reference

### `EmergingScoreConfig`

Located in `src/skill_radar/config/models.py`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `w_momentum` | float | 0.4 | Weight for momentum score in composite |
| `w_acceleration` | float | 0.3 | Weight for acceleration score in composite |
| `w_novelty` | float | 0.3 | Weight for novelty score in composite |

### `MLSegmentsConfig`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `k` | int | 4 | Number of KMeans clusters |
| `min_rows` | int | 20 | Minimum rows to fit a model (below → "unclustered") |
| `seed` | int | 42 | Random seed for reproducibility |
| `max_iter` | int | 20 | Maximum KMeans iterations |
| `features` | list[str] | `[jobs_count, unique_companies_count, unique_locations_count]` | Feature columns for clustering |
| `segment_names` | dict[str, str] | `{0: niche, 1: growing, 2: established, 3: dominant}` | Human-readable labels per cluster rank |

### `GoldAnalyticsConfig`

Aggregates both sub-configs under `gold.analytics` in the platform config.

### Override via environment variables

```bash
SKILLRADAR_GOLD_EMERGING_W_MOMENTUM=0.5
SKILLRADAR_GOLD_ML_SEGMENTS_K=5
SKILLRADAR_GOLD_ML_SEGMENTS_MIN_ROWS=10
```

---

## 8. Search Serving Layer

All three datasets are fully integrated into the Elasticsearch serving
pipeline.

### 8.1 Dataset Registry

In `src/skill_radar/domains/search/datasets.py`, `SERVED_DATASETS` now
contains **8 entries** and `PRIMARY_DATASETS` contains **6**:

```python
PRIMARY_DATASETS = [
    "skill_demand_daily",
    "salary_by_skill_daily",
    "occupation_skill_graph",
    "skill_emerging_daily",        # ← new
    "occupation_market_daily",     # ← new
    "skill_demand_segments_daily", # ← new
]
```

### 8.2 Elasticsearch Index Mappings

Each dataset has a dedicated mapping in
`src/skill_radar/platform/search/mappings.py`:

| Dataset | Key Mapped Fields | Types |
|---|---|---|
| `skill_emerging_daily` | `momentum_score`, `acceleration_score`, `novelty_score`, `emerging_composite_score` | `double` |
| `occupation_market_daily` | `total_jobs_count`, `unique_skills_count`, `avg_match_score` | `long`, `long`, `double` |
| `skill_demand_segments_daily` | `segment_id`, `segment_label`, `jobs_count`, `unique_companies_count`, `unique_locations_count` | `long`, `keyword`, `long` … |

### 8.3 Document Builders

In `src/skill_radar/domains/search/documents.py`:

- `build_skill_emerging_documents(rows)` — 4 score fields + lineage
- `build_occupation_market_documents(rows)` — 3 market metrics + lineage
- `build_skill_demand_segments_documents(rows)` — segment + demand features + lineage

All produce deterministic `doc_id` via SHA-256 content hashing.

### 8.4 Export Resilience

The `run search` CLI command distinguishes **core** and **enrichment**
datasets:

- Core datasets (`skill_demand_daily`, `salary_by_skill_daily`,
  `occupation_skill_graph`) — failure aborts the pipeline.
- Enrichment datasets (the 3 new ones) — export failures are downgraded
  to warnings and do not block the pipeline.

Validation checks run only for datasets that were actually exported.

---

## 9. Kibana Dashboard — Emerging Signals

**Dashboard**: "Skill Radar / Emerging Skills & Market Signals"
**ID**: `skillradar-dash-emerging-signals`
**Builder**: `src/skill_radar/platform/search/kibana_builders.py`
→ `_build_emerging_signals()`

### Visualizations

| # | Type | Title | Description |
|---|---|---|---|
| 1 | KPI (lnsMetric) | Total Emerging Records | Count of docs in the data view |
| 2 | Horizontal Bar (lnsXY) | Top 20 Emerging Skills | Skills ranked by `emerging_composite_score` |
| 3 | Line (lnsXY) | Composite Score Trend | `emerging_composite_score` over time |
| 4 | Table (lnsDatatable) | Skill-Level Scores | 50-row table: momentum, acceleration, novelty, composite per skill |

### Data Views

Three new Kibana data views are registered:

| ID | Title | Index Pattern |
|---|---|---|
| `skillradar-dv-skill-emerging-daily` | Skill Radar — Emerging Skills Daily | `skillradar-skill-emerging-daily-*` |
| `skillradar-dv-occupation-market-daily` | Skill Radar — Occupation Market Daily | `skillradar-occupation-market-daily-*` |
| `skillradar-dv-skill-demand-segments-daily` | Skill Radar — Skill Demand Segments Daily | `skillradar-skill-demand-segments-daily-*` |

---

## 10. Validation & Observability

### Per-Dataset Validation Checks

Each new dataset is validated in Elasticsearch with the same 4-check pattern
used for existing datasets:

1. **Index exists** — alias resolves
2. **Mapping fields** — expected field names and types match
3. **Doc count** — documents exist for the `(date, country)` partition
4. **Required fields populated** — sample of 10 docs has all key fields non-null

Key fields per dataset (`_KEY_FIELDS` in
`src/skill_radar/platform/validate/checks/search.py`):

| Dataset | Required Fields |
|---|---|
| `skill_emerging_daily` | `doc_id`, `ingestion_date`, `country`, `esco_skill_concept_uri` |
| `occupation_market_daily` | `doc_id`, `ingestion_date`, `country`, `esco_occupation_concept_uri` |
| `skill_demand_segments_daily` | `doc_id`, `ingestion_date`, `country`, `esco_skill_concept_uri` |

### Kibana Asset Checks

Data-view and dashboard existence checks are **non-blocking** in the
automated pipeline context.  When Kibana is unreachable or assets are not
yet applied, these checks are downgraded from FAIL → WARN with the note
"non-blocking in pipeline".  Apply them separately via:

```bash
skill-radar search dashboard apply --kibana-url http://kibana:5601
```

### Orchestrator Observability

The `GoldAnalyticsResult` model tracks row counts for each phase:

```python
result.skill_emerging_rows       # Phase 5
result.occupation_market_rows    # Phase 6
result.skill_demand_segments_rows  # Phase 7
```

Final log line emits all six counts:
```
Gold analytics complete: demand=98 salary=9 graph=2798 emerging=98 occ_market=1517 segments=98
```

---

## 11. Pipeline Hardening

Several robustness improvements were implemented alongside the new datasets:

### 11.1 Soft-Fail Enrichment Phases

Phases 5–7 are non-critical.  Any exception is caught, logged as a warning,
and the pipeline continues.  This prevents an ML library issue (e.g., missing
`numpy`) from blocking the core Gold output.

### 11.2 `--kibana-url` CLI Option

The `skill-radar run search` command now accepts `--kibana-url` to override
the Kibana endpoint.  This is critical for Docker containers where Kibana
resolves as `http://kibana:5601` instead of `http://localhost:5601`.

The Airflow DAG passes this automatically:
```python
f"--kibana-url {SEARCH_KIBANA_URL_DOCKER}"
```

### 11.3 Core vs. Enrichment Export Semantics

The search export distinguishes between core and enrichment datasets.
Only core dataset failures abort the pipeline; enrichment failures are
downgraded to warnings.

### 11.4 Kibana Validation Downgrading

Kibana asset checks (data views, dashboards) are non-blocking in the
`run search` pipeline.  FAIL results are automatically downgraded to WARN
because dashboards are applied out-of-band via `search dashboard apply`.

### 11.5 Kibana Reachability Tolerance

When Kibana is unreachable during validation, the reachability check
returns WARN (not FAIL).  All downstream Kibana asset checks are
short-circuited to WARN as well, avoiding a cascade of false failures.

---

## 12. Module Map

```
src/skill_radar/
├── config/
│   └── models.py                   # EmergingScoreConfig, MLSegmentsConfig
├── domains/
│   ├── gold/
│   │   ├── analytics/
│   │   │   ├── emerging_skills.py  # compute_skill_emerging_daily()
│   │   │   ├── occupation_market.py# compute_occupation_market_daily()
│   │   │   ├── skill_segments.py   # compute_skill_demand_segments()
│   │   │   └── orchestrator.py     # Phases 5–7 integration
│   │   ├── matching/
│   │   │   └── models.py           # GoldAnalyticsResult (row counts)
│   │   └── schema.py               # Schema constants for 3 new tables
│   └── search/
│       ├── datasets.py             # ServedDataset entries + PRIMARY_DATASETS
│       ├── documents.py            # 3 new document builders
│       ├── kibana_metadata.py      # DashboardDatasetMeta for 3 datasets
│       └── kibana_builders.py      # Dashboard D builder
├── platform/
│   ├── search/
│   │   ├── mappings.py             # 3 new ES index mappings
│   │   └── kibana_assets.py        # Expected asset inventory
│   └── validate/
│       └── checks/search.py        # _KEY_FIELDS + Kibana WARN logic
├── cli/
│   └── run.py                      # --kibana-url, core/enrichment semantics
dags/
├── adzuna_daily_pipeline.py        # --kibana-url in search_unit command
└── _shared/
    └── config.py                   # SEARCH_KIBANA_URL_DOCKER
```

---

## 13. Testing

### Unit Tests

The three analytical functions are tested at the module level:

- **Schema validation** — `tests/unit/domains/gold/` tests verify required
  columns and composite keys for all new tables.
- **Dataset registry** — `tests/unit/domains/search/test_datasets.py`
  confirms `PRIMARY_DATASETS` has 6 entries and all are subsets of
  `ALL_DATASET_NAMES` (8 entries).
- **Document builders** — `tests/unit/domains/search/test_documents.py`
  validates `doc_id` generation and field mapping for all new datasets.
- **ES mappings** — `tests/unit/platform/search/test_mappings.py` ensures
  mapping keys exist and field types are correct.
- **Kibana metadata** — `tests/unit/domains/search/test_kibana_metadata.py`
  validates data-view titles, field counts, and sort fields.
- **Kibana builders** — `tests/unit/platform/search/test_kibana_builders.py`
  (110 tests) validates the Lens serialization format, panel layout, and
  NDJSON structure.
- **Search validation** — `tests/unit/platform/validate/test_search_checks.py`
  covers the WARN-downgrade logic for unreachable Kibana.

### Running

```bash
make check     # lint + format + mypy + unit tests
make ci        # full CI pipeline
```
