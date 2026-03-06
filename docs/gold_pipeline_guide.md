# Gold Pipeline — Data Engineering Guide

> **Layer**: Gold (consumption-ready analytics)<br>
> **Engine**: PySpark 3.5 + Apache Iceberg (format-version 2, Parquet)<br>
> **Namespace**: `sr.sr_gold`<br>
> **Output tables**: 5<br>

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Data Source Profile](#2-data-source-profile)
3. [Architecture Overview](#3-architecture-overview)
4. [Design Principles](#4-design-principles)
5. [Matching Layer — Skill Matching](#5-matching-layer--skill-matching)
6. [Matching Layer — Occupation Matching](#6-matching-layer--occupation-matching)
7. [Matching Layer — Orchestrator](#7-matching-layer--orchestrator)
8. [Analytics Layer — KPIs and Graph](#8-analytics-layer--kpis-and-graph)
9. [Iceberg Table Layouts](#9-iceberg-table-layouts)
10. [Validation Framework](#10-validation-framework)
11. [Observability and Lineage](#11-observability-and-lineage)
12. [CLI and Makefile Interface](#12-cli-and-makefile-interface)
13. [Testing Strategy](#13-testing-strategy)
14. [Production Alignment Decisions](#14-production-alignment-decisions)
15. [Module Map](#15-module-map)
16. [Future Roadmap](#16-future-roadmap)

---

## 1. Executive Summary

The **Gold layer** is the final analytical stage in the Skill Radar lakehouse
architecture. It fuses cleaned job-market data (Adzuna Silver) with a
standardised European skills taxonomy (ESCO Silver) to produce five
consumption-ready Iceberg tables that power downstream dashboards, APIs, and
research queries.

| Aspect | Detail |
|---|---|
| **Inputs** | Adzuna Silver jobs + ESCO Silver skills, occupations, relations |
| **Outputs** | 5 Gold Iceberg tables in `sr.sr_gold` |
| **Matching strategy** | Deterministic, dictionary-driven, no ML |
| **Write mode** | Partition overwrite → idempotent reruns |
| **Validation** | 35+ automated checks across all 5 tables |
| **CLI** | `skill-radar gold matching`, `gold analytics`, `gold pipeline`, `run gold`, `validate gold` |
| **Tests** | 68 unit tests (5 files), 0 regressions |

The pipeline is split into two sequential stages:

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                        Gold Pipeline Flow                          │
  │                                                                    │
  │   Phase 1: Matching                 Phase 2: Analytics             │
  │   ─────────────────                 ──────────────────             │
  │   ESCO Silver ─┐                   Gold match tables ─┐           │
  │                 ├→ Skill Matching                      ├→ KPIs     │
  │   Adzuna Silver─┤  Occupation Matching   Adzuna Silver─┤  Graph    │
  │                 ├→ Deduplication                       ├→ Salary   │
  │                 ↓                                      ↓           │
  │          gold_job_skill_matches           gold_skill_demand_daily  │
  │          gold_job_occupation_matches      gold_salary_by_skill     │
  │                                          gold_occupation_skill_graph│
  └─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Source Profile

The Gold layer consumes exclusively from the Silver layer. No raw or
external data is read directly.

### Input Tables

| Source | Table FQN | Description | Key Columns |
|---|---|---|---|
| Adzuna Silver | `sr.sr_silver.adzuna_jobs` | Cleaned, deduplicated job postings | `job_id`, `title_normalized`, `description_normalized`, `salary_*`, `company_name` |
| ESCO Silver | `sr.sr_silver.esco_skills` | Standardised skill taxonomy | `concept_uri`, `preferred_label`, `alt_labels[]`, `hidden_labels[]`, `skill_type` |
| ESCO Silver | `sr.sr_silver.esco_occupations` | Standardised occupation taxonomy | `concept_uri`, `preferred_label`, `concept_uri_uuid` |
| ESCO Silver | `sr.sr_silver.esco_relations` | Skill-to-occupation mapping | `occupation_uri`, `skill_uri`, `relation_type` |

### Partition Filtering

All reads are partition-scoped to avoid full table scans:

- **Adzuna Silver**: `WHERE country = :country AND ingestion_date = :date`
- **ESCO Silver**: `WHERE version = :esco_version AND lang = :esco_lang`

### Output Tables

| Table | Grain | Phase |
|---|---|---|
| `gold_job_skill_matches` | (job_id, skill_uri) per partition | Matching |
| `gold_job_occupation_matches` | (job_id, occupation_uri, method) per partition | Matching |
| `gold_skill_demand_daily` | (skill_uri) per partition | Analytics |
| `gold_salary_by_skill_daily` | (skill_uri) per partition | Analytics |
| `gold_occupation_skill_graph` | (occupation_uri, skill_uri) per partition | Analytics |

---

## 3. Architecture Overview

### Layered Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Lakehouse Layers                                  │
│                                                                              │
│  Landing          Bronze              Silver             Gold                │
│  (raw files)      (raw Iceberg)       (clean Iceberg)    (analytics)         │
│                                                                              │
│  Adzuna ZIP ──→ adzuna_jobs_raw ──→ adzuna_jobs ─────┐                       │
│                                                      ├──→ Matching ──→ KPIs  │
│  ESCO CSVs ──→ esco_*_raw ──────→ esco_skills ───────┘                       │
│                                   esco_occupations                           │
│                                   esco_relations                             │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Gold Internal Architecture

```
                    ┌──────────────────────────────────────────┐
                    │            Gold Matching Layer           │
                    │                                          │
                    │  ┌─────────────────────────────────────┐ │
                    │  │     build_skill_label_dictionary()  │ │
                    │  │  ESCO Silver → flat label dictionary│ │
                    │  └──────────────┬──────────────────────┘ │
                    │                 │                        │
                    │  ┌──────────────▼──────────────────────┐ │
                    │  │     match_jobs_to_skills()          │ │
                    │  │  broadcast cross-join + \\b regex   │ │
                    │  │  → raw skill match rows             │ │
                    │  └──────────────┬──────────────────────┘ │
                    │                 │                        │
                    │  ┌──────────────▼──────────────────────┐ │
                    │  │     deduplicate_skill_matches()     │ │
                    │  │  Window: highest score wins         │ │
                    │  │  + merge title_hit / description_hit│ │
                    │  └──────────────┬──────────────────────┘ │
                    │                 │                        │
                    │  ┌──────────────▼──────────────────────┐ │
                    │  │ match_jobs_to_occupations_by_title()│ │
                    │  │  equi-join on normalized label      │ │
                    │  └──────────────┬──────────────────────┘ │
                    │                 │                        │
                    │  ┌──────────────▼──────────────────────┐ │
                    │  │  match_jobs_to_occupations_by_      │ │
                    │  │  relations()                        │ │
                    │  │  skill_matches → ESCO relations →   │ │
                    │  │  groupBy → score formula            │ │
                    │  └──────────────┬──────────────────────┘ │
                    │                 │                        │
                    │  ┌──────────────▼──────────────────────┐ │
                    │  │     combine_occupation_matches()    │ │
                    │  │  unionByName + dedup                │ │
                    │  └──────────────┬──────────────────────┘ │
                    │                 ▼                        │
                    │  ┌───────────────────────────────────┐   │
                    │  │  _write_gold_table()              │   │
                    │  │  Iceberg v2 / partition overwrite │   │
                    │  └───────────────────────────────────┘   │
                    └──────────────────────────────────────────┘

                    ┌──────────────────────────────────────────┐
                    │           Gold Analytics Layer           │
                    │                                          │
                    │  Gold matches + Adzuna Silver + ESCO     │
                    │                                          │
                    │  ├→ compute_skill_demand_daily()         │
                    │  ├→ compute_salary_by_skill_daily()      │
                    │  └→ compute_occupation_skill_graph()     │
                    │                                          │
                    │  → 3 Iceberg tables (partition overwrite)│
                    └──────────────────────────────────────────┘
```

### Component Interactions

```
  CLI (click)
    │
    ├── gold matching ──→ matching/orchestrator.run_gold_matching()
    │                       ├── skill_matching.build_skill_label_dictionary()
    │                       ├── skill_matching.match_jobs_to_skills()
    │                       ├── skill_matching.deduplicate_skill_matches()
    │                       ├── occupation_matching.match_jobs_to_occupations_by_title()
    │                       ├── occupation_matching.match_jobs_to_occupations_by_relations()
    │                       ├── occupation_matching.combine_occupation_matches()
    │                       └── _write_gold_table() × 2
    │
    ├── gold analytics ─→ analytics/orchestrator.run_gold_analytics()
    │                       ├── skill_kpis.compute_skill_demand_daily()
    │                       ├── salary_kpis.compute_salary_by_skill_daily()
    │                       ├── occupation_kpis.compute_occupation_skill_graph()
    │                       └── _write_gold_table() × 3
    │
    ├── gold pipeline ──→ run_gold_matching() → run_gold_analytics()
    │
    ├── validate gold ──→ gold.get_gold_checks() → run_checks()
    │
    └── run gold ───────→ matching → analytics → validate (3-phase)
```

---

## 4. Design Principles

The Gold layer was built to satisfy the same production-alignment
invariants as the rest of the Skill Radar lakehouse. Every decision below
has a concrete implementation in the codebase.

### 4.1 Deterministic Matching — No ML

We deliberately chose **exact dictionary matching** over statistical or
ML-based NER/entity linking. This is a conscious trade-off:

| Criterion | Our approach (dictionary) | Alternative (ML NER) |
|---|---|---|
| **Reproducibility** | Bit-for-bit across reruns | Non-deterministic |
| **Explainability** | Every match traces to a label + score | Black box |
| **Dependency footprint** | PySpark only | spaCy / HuggingFace / GPU |
| **Accuracy on long-tail** | Lower | Higher |
| **Maintenance burden** | ESCO version pin | Model retraining |

The dictionary approach is production-safe for an initial release and can
be extended with fuzzy or ML methods later (see §16 Roadmap).

### 4.2 Separation of Concerns — Matching vs. Analytics

The Gold layer is split into two independent packages:

- **`matching/`**: reads Silver tables, produces match fact tables.
- **`analytics/`**: reads Gold match tables + Silver dimensions, produces
  KPI and graph tables.

This ensures that analytics can be recomputed without re-running the
expensive matching step, and that each stage can be tested in isolation.

### 4.3 Scoring as Code — No Magic Numbers

All scoring parameters are defined once in `models.py` and referenced
everywhere:

```python
SKILL_MATCH_SCORES: dict[tuple[str, str], float] = {
    ("preferred", "title"):       1.00,
    ("alt",       "title"):       0.95,
    ("hidden",    "title"):       0.65,
    ("preferred", "description"): 0.75,
    ("alt",       "description"): 0.70,
    ("hidden",    "description"): 0.45,
}

OCCUPATION_RELATION_BASE_SCORE  = 0.5
OCCUPATION_RELATION_SKILL_WEIGHT = 0.1
OCCUPATION_RELATION_MAX_SCORE   = 0.95
```

The score policy reflects **ESCO label semantics**:

- **preferred ≈ alt >> hidden** — Alt labels (non-preferred terms / NPTs)
  are legitimate labour-market synonyms, abbreviations, and spelling
  variants. They score close to preferred. Hidden labels exist for
  indexing and text-mining recall but may be outdated, misspelled, or
  politically incorrect — they are materially penalised.
- **title > description** — A mention in the job title carries stronger
  signal than one buried in the description.

A single `models.py` module is the **single source of truth** for all
match methods, score scales, and result dataclasses. This prevents
score drift and makes auditing trivial.

### 4.4 Idempotent Writes via Partition Overwrite

Every Gold table uses Iceberg's `overwritePartitions()` write mode.
Re-running the same (ingestion_date, country) partition produces
identical results without duplicating rows. This is critical for
retry safety in scheduled pipelines.

### 4.5 Contract-Free Design (Gold ≠ Bronze)

Unlike Bronze (which uses YAML contracts for external API ingestion),
Gold has no contract layer. Its inputs are already validated Silver
tables with stable schemas. Gold enforces quality through:

1. Typed column selections on read.
2. Deterministic transforms.
3. 35+ post-write validation checks.

---

## 5. Matching Layer — Skill Matching

**Module**: `domains/gold/matching/skill_matching.py`<br>
**Method tag**: `exact_dictionary_v1`

### 5.1 Label Dictionary Construction

The first step explodes ESCO Silver skills into a flat label dictionary:

```
                    ┌──────────────────────────────┐
                    │   ESCO Silver: esco_skills   │
                    │                              │
                    │  concept_uri                 │
                    │  preferred_label             │
                    │  alt_labels[]                │
                    │  hidden_labels[]             │
                    └──────────────┬───────────────┘
                                   │
                         explode_outer × 3 types
                                   │
                    ┌──────────────▼───────────────┐
                    │   Flat Label Dictionary      │
                    │                              │
                    │  concept_uri                 │
                    │  label_value                 │
                    │  label_normalized            │
                    │  label_type (preferred|alt|  │
                    │              hidden)         │
                    └──────────────────────────────┘
```

Implementation details:

1. **Preferred labels**: one row per skill (always present).
2. **Alt labels**: `explode_outer(alt_labels)` → one row per alternative form.
3. **Hidden labels**: `explode_outer(hidden_labels)` → one row per hidden form.
4. **Normalization**: `lower(regexp_replace(trim(label), '\\s+', ' '))`.
5. **Deduplication**: `dropDuplicates(["concept_uri", "label_normalized", "label_type"])`.
6. **Null filtering**: discard rows where `label_normalized` is null or empty.

### 5.2 Phrase-Safe Regex Matching

For each label in the dictionary, a **word-boundary-safe regex** pattern
is generated:

```
\b(?:machine learning|intelligence artificielle|python)\b
```

- Labels are escaped via `re.escape()` to handle special characters.
- Labels are sorted by length descending so longer phrases match first
  (avoiding false substring matches).
- The `\b` anchors prevent partial-word hits.

### 5.3 Broadcast Cross-Join Strategy

```
  Adzuna Silver jobs (partition-scoped)
          │
          │  crossJoin + broadcast
          │
  Label dictionary (broadcast-safe — thousands of rows)
          │
          ├── title_normalized.rlike(label_pattern) → title matches
          └── description_normalized.rlike(label_pattern) → description matches
```

- The label dictionary is broadcast-joined (`F.broadcast(labels)`)
  because ESCO labels fit in driver memory (typically 10–30 K rows).
- Two separate cross-joins execute: one for `title_normalized`, one for
  `description_normalized`.
- Results are unioned via `unionByName`.

### 5.4 Deterministic Scoring Matrix

Each raw match row receives a score from the 6-cell matrix:

| `label_type` | `text_source = title` | `text_source = description` |
|---|---|---|
| **preferred** | 1.00 | 0.75 |
| **alt** | 0.95 | 0.70 |
| **hidden** | 0.65 | 0.45 |

**Rationale** — the scoring policy encodes two orthogonal axes derived
from the ESCO data model:

1. **Label trustworthiness** (`preferred ≈ alt >> hidden`):
   - *Preferred labels* are the canonical, authoritative term.
   - *Alt labels* (non-preferred terms / NPTs) are legitimate
     labour-market synonyms, abbreviations, spelling variants, and
     declensions. They remain strong evidence and score close to
     preferred.
   - *Hidden labels* are kept for search/text-mining recall but may be
     outdated, misspelled, or politically incorrect. They are
     intentionally hidden from end users and therefore receive a
     materially lower score.

2. **Text source signal** (`title > description`):
   A skill mentioned in the job title is a stronger hiring signal than
   one appearing only in the description body.

The score expression is implemented as a chain of `F.when().otherwise()`
clauses referencing the constant dictionary — no hard-coded literals in
the Spark plan.

### 5.5 Deduplication

Raw matches can produce multiple rows per (job, skill) — e.g., the same
skill matched in both title and description, or via different label forms.
Deduplication is applied with a PySpark Window:

```python
Window.partitionBy(
    "job_id", "country", "ingestion_date",
    "esco_skill_concept_uri", "match_method",
).orderBy(
    col("match_score").desc(),
    # title > description
    when(col("matched_text_source") == "title", lit(0)).otherwise(lit(1)).asc(),
    # preferred > alt > hidden
    when(col("matched_label_type") == "preferred", lit(0))
    .when(col("matched_label_type") == "alt", lit(1))
    .otherwise(lit(2)).asc(),
)
```

After dedup, `title_hit` and `description_hit` are **merged** via a
separate `groupBy` aggregation so that the final row indicates whether
the skill was found in the title, description, or both.

---

## 6. Matching Layer — Occupation Matching

**Module**: `domains/gold/matching/occupation_matching.py`<br>

Occupation matching uses **two independent signals** that are combined and
deduplicated.

### 6.1 Signal 1 — Title Exact Match (`title_exact_v1`)

```
  Adzuna Silver jobs
       │
       │  title_normalized == occupation preferred_label (normalized)
       │
  ESCO Silver occupations (broadcast)
       │
       ▼
  Match rows: score = 1.0, method = title_exact_v1
```

- A normalised equi-join between `title_normalized` and the ESCO
  occupation `preferred_label` (both lowercased, whitespace-collapsed).
- Score is always **1.0** because an exact title match is the strongest
  signal available.
- The occupation dimension is broadcast-joined (small cardinality).

### 6.2 Signal 2 — Relation Inference (`relation_score_v1`)

```
  Gold skill matches (from §5)
       │
       │  JOIN esco_relations ON skill_uri
       │
       ▼
  (job, occupation) pairs with supporting skill counts
       │
       │  score = min(0.5 + count × 0.1, 0.95)
       │
       ▼
  Match rows: method = relation_score_v1
```

- For each job, the skill matches produced in §5 are joined with the
  ESCO `occupation_uri → skill_uri` relations table.
- A `groupBy(job_id, occupation_uri)` counts the distinct supporting
  skills.
- The score follows a linear formula capped at 0.95:

$$
\text{score} = \min(0.5 + \text{count} \times 0.1,\; 0.95)
$$

| Supporting skills | Score |
|---|---|
| 1 | 0.60 |
| 2 | 0.70 |
| 3 | 0.80 |
| 4 | 0.90 |
| 5+ | 0.95 (capped) |

The cap at 0.95 ensures relation-inferred matches never outscore a
direct title match (1.0), preserving signal hierarchy.

### 6.3 Combining and Deduplication

```python
combined = title_matches.unionByName(relation_matches)

Window.partitionBy(
    "job_id", "country", "ingestion_date",
    "esco_occupation_concept_uri", "match_method",
).orderBy(
    col("match_score").desc(),
    col("supporting_skill_match_count").desc(),
)
```

Both signals are unioned and deduplicated per
`(job, occupation, method)`. If the same occupation is matched by **both**
methods, both rows are kept (distinct `match_method` values), providing
downstream consumers with provenance transparency.

---

## 7. Matching Layer — Orchestrator

**Module**: `domains/gold/matching/orchestrator.py`<br>
**Function**: `run_gold_matching()`

The matching orchestrator sequences the full matching pipeline within a
single SparkSession:

```
  1. Load platform config + resolve run_id
  2. Create Gold namespace (CREATE NAMESPACE IF NOT EXISTS)
  3. Read inputs:
     - ESCO Silver skills    (filtered by version + lang)
     - ESCO Silver occupations
     - ESCO Silver relations
     - Adzuna Silver jobs    (filtered by country + date, optional job_limit)
  4. build_skill_label_dictionary()
  5. match_jobs_to_skills() → deduplicate_skill_matches()
  6. match_jobs_to_occupations_by_title()
  7. match_jobs_to_occupations_by_relations() (using skill matches from 5)
  8. combine_occupation_matches()
  9. Add lineage columns (gold_run_id, gold_generated_at_utc, esco_version, esco_lang)
 10. _write_gold_table() → gold_job_skill_matches
 11. _write_gold_table() → gold_job_occupation_matches
```

### Write Helper

```python
def _write_gold_table(df, table_fqn, spark, partition_cols=("ingestion_date", "country")):
    if not spark.catalog.tableExists(table_fqn):
        df.writeTo(table_fqn).using("iceberg")
          .tableProperty("format-version", "2")
          .tableProperty("write.format.default", "parquet")
          .partitionedBy(*partition_cols)
          .create()
    else:
        df.writeTo(table_fqn).overwritePartitions()
```

On first run, the table is created with Iceberg v2 metadata and Parquet
encoding. On subsequent runs, only the targeted partition is overwritten.

### Result Reporting

The orchestrator returns a `GoldMatchingResult` dataclass with:

| Field | Purpose |
|---|---|
| `run_id` | Lineage correlation |
| `ingestion_date`, `country` | Target partition |
| `esco_version`, `esco_lang` | Taxonomy version pin |
| `adzuna_jobs_count` | Input job count |
| `skill_dictionary_size` | Dictionary rows |
| `job_skill_matches_count` | Output skill match count |
| `job_occupation_matches_count` | Output occupation match count |
| `success` | Boolean |
| `error` | Error message (empty on success) |

---

## 8. Analytics Layer — KPIs and Graph

**Package**: `domains/gold/analytics/`<br>
**Orchestrator**: `run_gold_analytics()`

The analytics layer reads the matching outputs and Adzuna Silver job
facts to produce three consumption-ready tables. All computations are
pure PySpark aggregations — no UDFs, no external dependencies.

### 8.1 Skill Demand Daily

**Module**: `analytics/skill_kpis.py`<br>
**Function**: `compute_skill_demand_daily()`<br>
**Grain**: one row per `(ingestion_date, country, skill_uri)`<br>

Joins Gold skill matches with Adzuna Silver job facts and aggregates:

| Output Column | Aggregation |
|---|---|
| `jobs_count` | `countDistinct("job_id")` |
| `unique_companies_count` | `countDistinct("company_name")` |
| `unique_locations_count` | `countDistinct("location_display_name")` |
| `title_match_jobs_count` | `sum(when(title_hit == True, 1))` |
| `description_match_jobs_count` | `sum(when(description_hit == True, 1))` |
| `avg_salary_min` | `avg("salary_min")` |
| `avg_salary_max` | `avg("salary_max")` |
| `avg_salary_mean` | `avg("salary_mean")` |
| `first_seen_posted_date` | `min("posted_date")` |
| `last_seen_posted_date` | `max("posted_date")` |

Also carries through ESCO dimension columns: `esco_skill_type`,
`reuse_level`, `preferred_label`.

### 8.2 Salary by Skill Daily

**Module**: `analytics/salary_kpis.py`<br>
**Function**: `compute_salary_by_skill_daily()`<br>
**Grain**: one row per `(ingestion_date, country, skill_uri)`<br>

Similar to skill demand, but **restricted to jobs with non-null salary
data** (at least one of `salary_min`, `salary_max`, `salary_mean` is
not null):

| Output Column | Aggregation |
|---|---|
| `salary_jobs_count` | `countDistinct("job_id")` |
| `avg_salary_min` | `avg("salary_min")` |
| `avg_salary_max` | `avg("salary_max")` |
| `avg_salary_mean` | `avg("salary_mean")` |
| `min_salary_min` | `min("salary_min")` |
| `max_salary_max` | `max("salary_max")` |

### 8.3 Occupation-Skill Graph

**Module**: `analytics/occupation_kpis.py`<br>
**Function**: `compute_occupation_skill_graph()`<br>
**Grain**: one row per `(ingestion_date, country, occupation_uri, skill_uri, relation_type)`<br>

Builds a bipartite graph between occupations and skills, enriched with
market evidence:

```
  ESCO relations (backbone)
       │
       │  LEFT JOIN evidence from Gold matches
       │
  For each (occupation, skill) pair:
    How many distinct jobs matched BOTH the occupation AND the skill?
       │
       ▼
  matched_jobs_count (0 if no evidence)
```

- The ESCO relation table is the backbone (all relation pairs are represented).
- Evidence is computed by inner-joining `occ_matches` with `skill_matches`
  on `job_id`, then counting distinct jobs per `(occupation, skill)`.
- Pairs with no market evidence receive `matched_jobs_count = 0`.
- Labels are enriched from the ESCO skill and occupation dimension tables.

### 8.4 Analytics Orchestrator

`run_gold_analytics()` sequences:

1. Read Gold match tables (filtered by partition).
2. Read Adzuna Silver jobs (for salary/company enrichment).
3. Read ESCO Silver dimensions (for label/relation enrichment).
4. Compute all three analytics outputs.
5. Add lineage columns to each.
6. Write to Gold Iceberg tables with partition overwrite.

Returns `GoldAnalyticsResult` with row counts per output table.

---

## 9. Iceberg Table Layouts

All Gold tables share these properties:

| Property | Value |
|---|---|
| **Format version** | 2 |
| **File format** | Parquet |
| **Partitioning** | `(ingestion_date, country)` |
| **Write mode** | Partition overwrite (idempotent) |
| **Namespace** | `sr.sr_gold` |
| **Naming** | `gold_<entity>` (no `_raw` suffix) |

### 9.1 `gold_job_skill_matches`

| Column | Type | Description |
|---|---|---|
| `source_system` | string | Data source identifier |
| `country` | string | Partition key |
| `ingestion_date` | string | Partition key (YYYY-MM-DD) |
| `job_id` | string | Adzuna job identifier |
| `adref` | string | Adzuna ad reference |
| `posted_date` | string | Original posting date |
| `esco_skill_concept_uri` | string | ESCO skill URI |
| `esco_skill_concept_uri_uuid` | string | ESCO skill UUID |
| `esco_skill_preferred_label` | string | Canonical skill name |
| `esco_skill_type` | string | Skill type (skill/knowledge) |
| `esco_reuse_level` | string | ESCO reuse level |
| `matched_label` | string | Label that triggered the match |
| `matched_label_type` | string | `preferred` / `alt` / `hidden` |
| `matched_text_source` | string | `title` / `description` |
| `match_method` | string | `exact_dictionary_v1` |
| `match_score` | double | [0.5, 1.0] |
| `title_hit` | boolean | Skill appeared in title |
| `description_hit` | boolean | Skill appeared in description |
| `gold_run_id` | string | Pipeline run identifier |
| `gold_generated_at_utc` | timestamp | Pipeline execution time |
| `adzuna_silver_run_id` | string | Upstream lineage |
| `esco_version` | string | ESCO taxonomy version |
| `esco_lang` | string | ESCO language code |

**Business key**: `(job_id, esco_skill_concept_uri, country, ingestion_date)`

### 9.2 `gold_job_occupation_matches`

| Column | Type | Description |
|---|---|---|
| `source_system` | string | Data source identifier |
| `country` | string | Partition key |
| `ingestion_date` | string | Partition key |
| `job_id` | string | Adzuna job identifier |
| `adref` | string | Adzuna ad reference |
| `posted_date` | string | Original posting date |
| `esco_occupation_concept_uri` | string | ESCO occupation URI |
| `esco_occupation_concept_uri_uuid` | string | ESCO occupation UUID |
| `esco_occupation_preferred_label` | string | Canonical occupation name |
| `match_method` | string | `title_exact_v1` / `relation_score_v1` |
| `match_score` | double | [0.5, 1.0] |
| `matched_text_source` | string | `title` / `relation` |
| `supporting_skill_match_count` | integer | Skills supporting inference |
| `gold_run_id` | string | Pipeline run identifier |
| `gold_generated_at_utc` | timestamp | Pipeline execution time |
| `adzuna_silver_run_id` | string | Upstream lineage |
| `esco_version` | string | ESCO taxonomy version |
| `esco_lang` | string | ESCO language code |

**Business key**: `(job_id, esco_occupation_concept_uri, match_method, country, ingestion_date)`

### 9.3 `gold_skill_demand_daily`

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | string | Partition key |
| `country` | string | Partition key |
| `esco_skill_concept_uri` | string | ESCO skill URI |
| `esco_skill_concept_uri_uuid` | string | ESCO skill UUID |
| `esco_skill_preferred_label` | string | Canonical skill name |
| `esco_skill_type` | string | Skill type |
| `reuse_level` | string | ESCO reuse level |
| `jobs_count` | long | Distinct jobs mentioning this skill |
| `unique_companies_count` | long | Distinct hiring companies |
| `unique_locations_count` | long | Distinct job locations |
| `title_match_jobs_count` | long | Jobs matched via title |
| `description_match_jobs_count` | long | Jobs matched via description |
| `avg_salary_min` | double | Average of salary_min across jobs |
| `avg_salary_max` | double | Average of salary_max across jobs |
| `avg_salary_mean` | double | Average of salary_mean across jobs |
| `first_seen_posted_date` | string | Earliest posting date |
| `last_seen_posted_date` | string | Latest posting date |
| `gold_run_id` | string | Pipeline run identifier |
| `gold_generated_at_utc` | timestamp | Pipeline execution time |
| `esco_version` | string | ESCO taxonomy version |
| `esco_lang` | string | ESCO language code |

**Business key**: `(esco_skill_concept_uri, country, ingestion_date)`

### 9.4 `gold_salary_by_skill_daily`

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | string | Partition key |
| `country` | string | Partition key |
| `esco_skill_concept_uri` | string | ESCO skill URI |
| `esco_skill_concept_uri_uuid` | string | ESCO skill UUID |
| `esco_skill_preferred_label` | string | Canonical skill name |
| `salary_jobs_count` | long | Jobs with salary data |
| `avg_salary_min` | double | Average of salary_min |
| `avg_salary_max` | double | Average of salary_max |
| `avg_salary_mean` | double | Average of salary_mean |
| `min_salary_min` | double | Minimum of salary_min |
| `max_salary_max` | double | Maximum of salary_max |
| `gold_run_id` | string | Pipeline run identifier |
| `gold_generated_at_utc` | timestamp | Pipeline execution time |
| `esco_version` | string | ESCO taxonomy version |
| `esco_lang` | string | ESCO language code |

**Business key**: `(esco_skill_concept_uri, country, ingestion_date)`

### 9.5 `gold_occupation_skill_graph`

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | string | Partition key |
| `country` | string | Partition key |
| `esco_occupation_concept_uri` | string | ESCO occupation URI |
| `esco_occupation_concept_uri_uuid` | string | ESCO occupation UUID |
| `esco_occupation_preferred_label` | string | Canonical occupation name |
| `esco_skill_concept_uri` | string | ESCO skill URI |
| `esco_skill_concept_uri_uuid` | string | ESCO skill UUID |
| `esco_skill_preferred_label` | string | Canonical skill name |
| `relation_type` | string | ESCO relation type |
| `matched_jobs_count` | long | Jobs matching both entities |
| `gold_run_id` | string | Pipeline run identifier |
| `gold_generated_at_utc` | timestamp | Pipeline execution time |
| `esco_version` | string | ESCO taxonomy version |
| `esco_lang` | string | ESCO language code |

**Business key**: `(esco_occupation_concept_uri, esco_skill_concept_uri, country, ingestion_date)`

---

## 10. Validation Framework

**Module**: `platform/validate/checks/gold.py`<br>
**Factory**: `get_gold_checks(spark, config, ingestion_date, country, esco_version, esco_lang)`<br>
**Exit code**: `ExitCode.GOLD_FAILURE = 45`<br>

The Gold validation suite reuses the platform's generic check
infrastructure (`NamedCheck` → `run_checks()` → `ValidationReport` →
`finalize_report()`) and adds domain-specific assertions.

### 10.1 Check Categories

| Category | Description | Implementation |
|---|---|---|
| **Structural** | Namespace exists, table exists, non-empty | Reuse from `lakehouse` checks |
| **Schema** | Required columns present per table | `check_table_schema_contains()` |
| **Partition** | Target (date, country) partition non-empty | `_check_partition_non_empty()` |
| **Score range** | `match_score` ∈ [0, 1] | `_check_score_range()` |
| **Uniqueness** | No duplicate business keys | `_check_no_duplicate_keys()` |
| **Lineage** | Gold lineage columns present | `_check_lineage_present()` |
| **Salary coherence** | `min_salary_min ≤ avg_salary_mean ≤ max_salary_max` | `_check_salary_coherence()` |
| **Positive counts** | Count columns ≥ 0 | `_check_positive_counts()` |
| **Graph integrity** | `co_occurrence_count ≥ 1` | `_check_co_occurrence_positive()` |

### 10.2 Checks per Table

#### `gold_job_skill_matches` (7 checks)

| Check Name | Assertion |
|---|---|
| `gold.skill_matches.table_exists` | Iceberg table exists |
| `gold.skill_matches.non_empty` | Table has > 0 rows |
| `gold.skill_matches.schema` | 15 required columns present |
| `gold.skill_matches.partition_non_empty` | Target partition has rows |
| `gold.skill_matches.score_range` | `match_score` ∈ [0, 1] |
| `gold.skill_matches.no_duplicates` | Unique `(job_id, skill_uri, country, ingestion_date)` |
| `gold.skill_matches.lineage` | Lineage columns present |

#### `gold_job_occupation_matches` (7 checks)

| Check Name | Assertion |
|---|---|
| `gold.occ_matches.table_exists` | Iceberg table exists |
| `gold.occ_matches.non_empty` | Table has > 0 rows |
| `gold.occ_matches.schema` | 11 required columns present |
| `gold.occ_matches.partition_non_empty` | Target partition has rows |
| `gold.occ_matches.score_range` | `match_score` ∈ [0, 1] |
| `gold.occ_matches.no_duplicates` | Unique `(job_id, occupation_uri, method, country, ingestion_date)` |
| `gold.occ_matches.lineage` | Lineage columns present |

#### `gold_skill_demand_daily` (7 checks)

| Check Name | Assertion |
|---|---|
| `gold.demand.table_exists` | Iceberg table exists |
| `gold.demand.non_empty` | Table has > 0 rows |
| `gold.demand.schema` | 13 required columns present |
| `gold.demand.partition_non_empty` | Target partition has rows |
| `gold.demand.positive_counts` | `job_count`, `distinct_companies`, `distinct_locations`, `title_hit_count`, `description_hit_count` ≥ 0 |
| `gold.demand.no_duplicates` | Unique `(skill_uri, country, ingestion_date)` |
| `gold.demand.lineage` | Lineage columns present |

#### `gold_salary_by_skill_daily` (8 checks)

| Check Name | Assertion |
|---|---|
| `gold.salary.table_exists` | Iceberg table exists |
| `gold.salary.non_empty` | Table has > 0 rows |
| `gold.salary.schema` | 12 required columns present |
| `gold.salary.partition_non_empty` | Target partition has rows |
| `gold.salary.salary_coherence` | `min_salary_min ≤ avg_salary_mean ≤ max_salary_max` |
| `gold.salary.positive_counts` | `jobs_with_salary` ≥ 0 |
| `gold.salary.no_duplicates` | Unique `(skill_uri, country, ingestion_date)` |
| `gold.salary.lineage` | Lineage columns present |

#### `gold_occupation_skill_graph` (7 checks)

| Check Name | Assertion |
|---|---|
| `gold.graph.table_exists` | Iceberg table exists |
| `gold.graph.non_empty` | Table has > 0 rows |
| `gold.graph.schema` | 11 required columns present |
| `gold.graph.partition_non_empty` | Target partition has rows |
| `gold.graph.co_occurrence_positive` | `co_occurrence_count ≥ 1` |
| `gold.graph.no_duplicates` | Unique `(occupation_uri, skill_uri, country, ingestion_date)` |
| `gold.graph.lineage` | Lineage columns present |

**Plus**: 1 namespace check (`gold.namespace_exists`).
**Total**: 37 checks.

---

## 11. Observability and Lineage

### 11.1 Lineage Columns

Every Gold table carries four lineage columns:

| Column | Purpose |
|---|---|
| `gold_run_id` | UUID-hex identifier correlating rows to a pipeline execution |
| `gold_generated_at_utc` | ISO 8601 timestamp of generation |
| `esco_version` | ESCO taxonomy version used for matching |
| `esco_lang` | ESCO language used for matching |

Match tables additionally carry `adzuna_silver_run_id` to trace the
upstream Silver pipeline that produced the job data.

### 11.2 Structured Logging

All orchestrators and CLI commands use the platform's structured logging
pipeline:

```
  init_logging("gold_matching", enable_file=True)
       │
  set_context(dataset="gold", spark_app_id=...)
       │
  domain orchestrator (logger.info / logger.exception)
       │
  finalize_logging()
```

Logs are written to `logs/` with structured context (run_id, dataset,
spark_app_id) and are safe for aggregation in centralised log systems.

### 11.3 Result Dataclasses

Each orchestrator returns a typed result dataclass (`GoldMatchingResult`,
`GoldAnalyticsResult`, `GoldPipelineResult`) with a `summary_dict()`
method for JSON serialization. The CLI prints structured summaries:

```
  Gold matching complete.
    Country              : fr
    Ingestion date       : 2025-01-15
    ESCO version         : v1.2.1
    ESCO lang            : fr
    Adzuna jobs          : 4212
    Skill dictionary     : 28 740
    Job-skill matches    : 187 320
    Job-occ matches      : 3 891
    Run ID               : a1b2c3d4e5f6
```

---

## 12. CLI and Makefile Interface

### 12.1 CLI Commands

All Gold CLI commands follow the platform's standard pattern:
`init_logging → load config → SparkSession (with JVM mitigations) → orchestrator → finalize_logging`.

| Command | Description | Key Options |
|---|---|---|
| `skill-radar gold matching` | Run skill + occupation matching | `--ingestion-date`, `--country`, `--esco-version`, `--esco-lang`, `--job-limit`, `--quiet` |
| `skill-radar gold analytics` | Run KPI + graph computation | `--ingestion-date`, `--country`, `--esco-version`, `--esco-lang`, `--quiet` |
| `skill-radar gold pipeline` | Run matching → analytics | All matching options |
| `skill-radar validate gold` | Run 37 validation checks | `--ingestion-date`, `--country`, `--esco-version`, `--esco-lang` |
| `skill-radar run gold` | Full pipeline: matching → analytics → validate | All matching options |

#### JVM Crash Mitigations

All Gold Spark sessions disable three features known to cause native
crashes in containerised JVM environments:

```python
SparkSession.builder.appName("gold_matching")
    .config("spark.sql.codegen.wholeStage", "false")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
```

### 12.2 Makefile Targets

| Target | Description |
|---|---|
| `make gold-matching` | Matching phase only |
| `make gold-analytics` | Analytics phase only |
| `make validate-gold` | Validation phase only |
| `make run-gold` | Full pipeline (matching → analytics → validate) |

### 12.3 Makefile Variables

| Variable | Default | Description |
|---|---|---|
| `GOLD_COUNTRY` | `fr` | Target country partition |
| `GOLD_INGESTION_DATE` | *(required)* | Target date partition (YYYY-MM-DD) |
| `GOLD_ESCO_VERSION` | `v1.2.1` | ESCO taxonomy version |
| `GOLD_ESCO_LANG` | `fr` | ESCO language code |
| `GOLD_JOB_LIMIT` | *(empty)* | Debug: limit Adzuna job count |

### 12.4 Usage Examples

```bash
# Full pipeline
make run-gold GOLD_COUNTRY=fr GOLD_INGESTION_DATE=2025-01-15 \
  GOLD_ESCO_VERSION=v1.2.1 GOLD_ESCO_LANG=fr

# Matching only with job limit (debugging)
make gold-matching GOLD_COUNTRY=fr GOLD_INGESTION_DATE=2025-01-15 \
  GOLD_ESCO_VERSION=v1.2.1 GOLD_ESCO_LANG=fr GOLD_JOB_LIMIT=100

# Direct CLI (inside container)
skill-radar run gold --ingestion-date 2025-01-15 --country fr \
  --esco-version v1.2.1 --esco-lang fr
```

---

## 13. Testing Strategy

### 13.1 Test Philosophy

Gold tests follow the project's established convention: **unit tests
validate logic in isolation** without requiring a SparkSession or Iceberg
catalog. Integration and e2e tests (run in Docker) cover the full
pipeline with real Spark execution.

### 13.2 Test Files

| File | Tests | Scope |
|---|---|---|
| `test_models.py` | 24 | Scoring config integrity, `compute_occupation_relation_score()` determinism/capping/monotonicity, result dataclass defaults/serialization |
| `test_skill_matching.py` | 17 | `normalize_label()` edge cases, `build_match_regex_pattern()` word boundary safety, multi-label patterns, special character escaping |
| `test_layout.py` | 9 | All Gold + ESCO Silver FQN helpers, uniqueness across tables, format validation |
| `test_validation_checks.py` | 13 | Factory returns `NamedCheck` instances, check count ≥ 30, unique names, `gold.` prefix, required column lists, `ExitCode.GOLD_FAILURE == 45` |
| `test_cli.py` | 5 | `gold_group` registered in main, subcommands exist, help output, `validate gold` registered, `run gold` registered |
| **Total** | **68** | |

### 13.3 Key Testing Patterns

- **Score matrix completeness**: every `(label_type, text_source)` pair
  is tested to verify the `SKILL_MATCH_SCORES` dictionary covers all
  expected cells.
- **Boundary conditions**: `compute_occupation_relation_score()` is
  tested at 0, 1, 5, 10, 100 supporting skills to verify capping and
  monotonicity.
- **Regex safety**: patterns with special characters (`c++`,
  `node.js`, `(parentheses)`) are tested to verify `re.escape()`
  handles them correctly.
- **FQN uniqueness**: all Gold table FQNs are collected into a set and
  verified to be distinct (prevents copy-paste naming errors).
- **Validation check cardinality**: the factory is called with mock
  objects and verified to return ≥ 30 `NamedCheck` instances with unique
  names.

---

## 14. Production Alignment Decisions

This section documents the engineering decisions that make the Gold layer
production-ready rather than a proof-of-concept.

### 14.1 LakeLayout as Single Source of Truth

All 5 Gold table FQNs are constructed through `LakeLayout` convenience
methods (e.g., `gold_job_skill_matches_fqn()`). No module constructs
table names by hand. This means:

- Renaming a table is a single-line change.
- The catalog/namespace prefix is configurable via `PlatformSettings`.
- FQN consistency is enforced by unit tests.

### 14.2 Iceberg Format-Version 2

All Gold tables use Iceberg v2 with Parquet encoding:

- **Row-level deletes**: v2 supports position and equality deletes for
  future incremental updates.
- **Partition evolution**: v2 allows partition spec changes without
  rewriting data.
- **Hidden partitioning**: dates and other transform partitions are
  natively supported.

### 14.3 Partition Overwrite for Idempotency

Gold writes use `df.writeTo(fqn).overwritePartitions()`. This guarantees
that re-running the same (date, country) partition replaces the previous
output atomically. No deduplication of historical runs is needed. This is
the fundamental building block for retry-safe scheduled pipelines.

### 14.4 Scoring as a Static Configuration Object

Every score literal is defined in `models.py` and imported by the Spark
transform modules. This:

- Prevents drift between score definition and score usage.
- Enables unit testing of the score scale independent of Spark.
- Allows future A/B testing by swapping the config module.

### 14.5 Broadcast Joins for Dimension Tables

ESCO dimension tables (skills: ~13K rows, occupations: ~3K rows,
relations: ~30K rows) are broadcast-joined using `F.broadcast()`. This
avoids shuffle joins and keeps the Spark plan efficient for daily
job-partition sizes (typically 1K–50K rows).

### 14.6 Defensive Null Handling

- Skill matching filters out null/empty labels before dictionary
  construction.
- Salary KPIs explicitly filter to `salary_mean IS NOT NULL` before
  aggregation.
- Occupation graph uses `F.coalesce(matched_jobs_count, lit(0))` for
  unmatched relation pairs.
- Validation checks verify non-negative counts and salary coherence.

### 14.7 Error Classification with Exit Codes

The validation framework assigns a dedicated `ExitCode.GOLD_FAILURE = 45`
(distinct from `SILVER_FAILURE = 43` and `BRONZE_FAILURE`). This allows
CI/CD and orchestrators (Airflow, Prefect) to distinguish Gold-specific
failures from other pipeline stages and route alerts accordingly.

### 14.8 Structured Result Reporting

Both orchestrators return typed dataclasses (`GoldMatchingResult`,
`GoldAnalyticsResult`) with `summary_dict()` methods that serialize to
JSON-compatible dicts. This eliminates ad-hoc string parsing in
downstream automation and enables structured alerting.

### 14.9 JVM Crash Mitigations

Gold SparkSessions disable `wholeStageCodegen`, `vectorizedReader`, and
`adaptiveQueryExecution` — three features known to cause native SIGSEGV
crashes in Mac ARM and Docker Alpine environments. This is a
production-safety measure documented in the codebase and discoverable
via the CLI source.

### 14.10 Phased Execution with Early Exit

The `run gold` command executes three phases (matching → analytics →
validation) with early-exit on failure. If matching fails, analytics is
skipped. If analytics fails, validation is skipped. This prevents
cascading errors and wasted compute.

---

## 15. Module Map

```
src/skill_radar/
├── domains/
│   └── gold/
│       ├── __init__.py
│       ├── matching/
│       │   ├── __init__.py
│       │   ├── models.py                   # Score config, constants, result dataclasses
│       │   ├── skill_matching.py           # Dictionary build, regex matching, dedup
│       │   ├── occupation_matching.py      # Title + relation matching, combine
│       │   └── orchestrator.py             # Sequences matching pipeline, Iceberg write
│       └── analytics/
│           ├── __init__.py
│           ├── skill_kpis.py               # Daily skill demand aggregation
│           ├── salary_kpis.py              # Salary-by-skill aggregation
│           ├── occupation_kpis.py          # Occupation-skill graph builder
│           └── orchestrator.py             # Sequences analytics pipeline, Iceberg write
├── cli/
│   ├── gold.py                             # gold matching | analytics | pipeline commands
│   ├── run.py                              # run gold (matching → analytics → validate)
│   └── validate.py                         # validate gold (37 checks)
└── platform/
    ├── lake/
    │   └── layout.py                       # Gold FQN helpers (5 tables + 3 ESCO Silver)
    └── validate/
        ├── models.py                       # ExitCode.GOLD_FAILURE = 45
        └── checks/
            └── gold.py                     # 37 validation checks, check helpers

tests/unit/domains/gold/
├── test_models.py                          # 24 tests — scoring, dataclasses
├── test_skill_matching.py                  # 17 tests — normalize, regex, patterns
├── test_layout.py                          #  9 tests — FQN helpers, uniqueness
├── test_validation_checks.py              # 13 tests — check factory, exit codes
└── test_cli.py                            #  5 tests — CLI registration, help
```

---

## 16. Future Roadmap

| Enhancement | Priority | Description |
|---|---|---|
| **Fuzzy skill matching** | High | Add Levenshtein / Jaro-Winkler similarity as a second match method to capture abbreviations and misspellings |
| **Embedding-based matching** | Medium | Use sentence embeddings (e.g., CamemBERT for French) for semantic similarity matching as a third signal |
| **Incremental processing** | High | Replace full-partition recompute with Iceberg merge-on-read for append-only daily ingestion |
| **Time-series analytics** | Medium | Add rolling-window KPIs (7-day, 30-day moving averages) on top of daily snapshots |
| **Multi-language support** | Medium | Extend matching to handle cross-language skill extraction (e.g., English jobs with French ESCO) |
| **Graph analytics** | Low | Compute PageRank / centrality measures on the occupation-skill graph |
| **Data quality SLAs** | High | Add threshold-based checks (e.g., match rate ≥ X%) beyond current existence/range checks |
| **Spark UI observability** | Medium | Emit Spark metrics to Prometheus for job duration/shuffle monitoring |
| **Materialized views** | Low | Create Iceberg views for common dashboard queries |
| **Upstream lineage tracking** | Medium | Write a `gold_pipeline_log` table with per-run metadata and check results |

---

*Guide version: 1.0.0 — Generated alongside the Gold layer implementation.*
