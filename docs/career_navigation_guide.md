# Career Navigation — Gold Analytics Extension

> **Scope**: Four new Gold analytical datasets enabling occupation and skill
> exploration, pairwise occupation similarity, and career transition guidance.
> Includes Elasticsearch/Kibana integration and a new dashboard.<br>
> **Engine**: PySpark 3.5 + Apache Iceberg<br>
> **Philosophy**: Reuses existing Gold tables and ESCO Silver dimensions —
> no new ingestion, no new Bronze/Silver processing.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Dataset A — Occupation Profile Daily](#3-dataset-a--occupation-profile-daily)
4. [Dataset B — Skill Profile Daily](#4-dataset-b--skill-profile-daily)
5. [Dataset C — Occupation Similarity Daily](#5-dataset-c--occupation-similarity-daily)
6. [Dataset D — Occupation Transition Daily](#6-dataset-d--occupation-transition-daily)
7. [Orchestrator Integration](#7-orchestrator-integration)
8. [Configuration Reference](#8-configuration-reference)
9. [Search Serving Layer](#9-search-serving-layer)
10. [Kibana Dashboard — Career Navigation Explorer](#10-kibana-dashboard--career-navigation-explorer)
11. [Validation & Observability](#11-validation--observability)
12. [Module Map](#12-module-map)
13. [Testing](#13-testing)

---

## 1. Executive Summary

This extension adds **four new Gold-layer analytical datasets** that enable
career exploration and navigation features:

| Dataset | Grain | Purpose |
|---|---|---|
| `gold_occupation_profile_daily` | occupation × date × country | Canonical occupation card with demand, salary, and skills |
| `gold_skill_profile_daily` | skill × date × country | Canonical skill card with demand, salary, and occupations |
| `gold_occupation_similarity_daily` | source × target occ × date × country | Pairwise occupation similarity via weighted Jaccard |
| `gold_occupation_transition_daily` | from × to occ × date × country | Transition difficulty, shared/missing skills, market context |

All four datasets:
- Are written to Iceberg (partitioned by `country`, `ingestion_date`)
- Are exported to Elasticsearch with dedicated index mappings
- Are surfaced in a new Kibana dashboard ("Career Navigation Explorer")
- Use **soft-fail** semantics — failures never break the core pipeline
- Are fully validated through the existing validation framework

---

## 2. Architecture Overview

```
 ┌──────────────────────────────────────────────────────────────┐
 │                   Existing Gold Tables                       │
 │  job_skill_matches · job_occupation_matches ·                │
 │  skill_demand_daily · salary_by_skill_daily ·                │
 │  occupation_skill_graph                                      │
 └───────┬──────────────────┬───────────────────┬───────────────┘
         │                  │                   │
 ┌───────▼──────┐   ┌──────▼───────┐   ┌───────▼───────────────┐
 │  ESCO Silver │   │  Adzuna      │   │  Existing Gold tables │
 │  occupations │   │  Silver jobs │   │  (Phases 1-7 outputs) │
 │  skills      │   └──────┬───────┘   └───────┬───────────────┘
 │  relations   │          │                   │
 └───────┬──────┘          │                   │
         │                 │                   │
 ┌───────▼─────────────────▼───────────────────▼───────────────┐
 │              Career Navigation Analytics                     │
 │                                                              │
 │  Phase  8: occupation_profile_daily   (occ card)            │
 │  Phase  9: skill_profile_daily        (skill card)          │
 │  Phase 10: occupation_similarity      (weighted Jaccard)    │
 │  Phase 11: occupation_transition      (difficulty + gaps)   │
 └──────────────────────────┬──────────────────────────────────┘
                            │
                    ┌───────▼───────┐
                    │  Elasticsearch │
                    │  4 new indices │
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │    Kibana      │
                    │  Dashboard E   │
                    └───────────────┘
```

### Data Flow Dependencies

- **Phase 8** (occupation profiles) reads Gold matches, salary, and Silver
  dimensions → produces the occupation card table
- **Phase 9** (skill profiles) reads Gold demand, salary, matches, and Silver
  skills → produces the skill card table
- **Phase 10** (occupation similarity) reads only ESCO Silver relations and
  occupations → produces pairwise similarity scores
- **Phase 11** (occupation transitions) reads the similarity pairs from
  Phase 10 plus occupation profiles from Phase 8 for salary/jobs context →
  produces transition guidance

---

## 3. Dataset A — Occupation Profile Daily

### Purpose

Builds a canonical **occupation card** for each ESCO occupation observed in
the market data. Aggregates demand metrics, salary context, top skills, and
top companies into a single searchable row.

### Schema

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | string | Partition key |
| `country` | string | Partition key |
| `esco_occupation_concept_uri` | string | ESCO occupation URI |
| `esco_occupation_concept_uri_uuid` | string | Short UUID for indexing |
| `esco_occupation_preferred_label` | string | Human-readable occupation name |
| `occupation_search_text` | string | Normalized all-label text for search |
| `matched_jobs_count` | long | Total matched job postings |
| `distinct_companies_count` | long | Unique hiring companies |
| `distinct_locations_count` | long | Unique job locations |
| `avg_salary_mean` | double | Weighted average salary |
| `top_essential_skills_json` | string | JSON array of top essential skills |
| `top_optional_skills_json` | string | JSON array of top optional skills |
| `top_companies_json` | string | JSON array of top hiring companies |
| `related_skills_count` | long | Total ESCO skill relations |
| `gold_pipeline_version` | string | Lineage |
| `gold_computed_at` | string | Lineage |

### Key

`(esco_occupation_concept_uri, country, ingestion_date)`

### Module

`src/skill_radar/domains/gold/analytics/occupation_profiles.py`

---

## 4. Dataset B — Skill Profile Daily

### Purpose

Builds a canonical **skill card** for each ESCO skill observed in
the market. Aggregates demand, salary, top occupations, and top companies.

### Schema

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | string | Partition key |
| `country` | string | Partition key |
| `esco_skill_concept_uri` | string | ESCO skill URI |
| `esco_skill_concept_uri_uuid` | string | Short UUID for indexing |
| `esco_skill_preferred_label` | string | Human-readable skill name |
| `skill_type` | string | Skill type from ESCO taxonomy |
| `skill_search_text` | string | Normalized all-label text for search |
| `jobs_count` | long | Total matched job postings |
| `companies_count` | long | Unique hiring companies |
| `locations_count` | long | Unique job locations |
| `avg_salary_mean` | double | Average salary across matched jobs |
| `top_occupations_json` | string | JSON array of top related occupations |
| `top_companies_json` | string | JSON array of top hiring companies |
| `gold_pipeline_version` | string | Lineage |
| `gold_computed_at` | string | Lineage |

### Key

`(esco_skill_concept_uri, country, ingestion_date)`

### Module

`src/skill_radar/domains/gold/analytics/skill_profiles.py`

---

## 5. Dataset C — Occupation Similarity Daily

### Purpose

Computes pairwise **occupation similarity** using weighted Jaccard on
ESCO skill-set overlap. Essential skills carry higher weight by default.

### Algorithm

```
score = (w_essential × |shared_essential| + w_optional × |shared_optional|)
      / (w_essential × |union_essential|  + w_optional × |union_optional|)
```

The default weights are `w_essential = 2.0`, `w_optional = 1.0`. Self-pairs
are excluded. Only the top-N most similar targets per source are retained
(default N = 20).

### Schema

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | string | Partition key |
| `country` | string | Partition key |
| `source_occupation_uri` | string | ESCO URI of the source occupation |
| `source_occupation_label` | string | Source occupation name |
| `target_occupation_uri` | string | ESCO URI of the target occupation |
| `target_occupation_label` | string | Target occupation name |
| `similarity_score` | double | Weighted Jaccard similarity (0–1) |
| `shared_skill_count` | long | Total shared skills |
| `shared_essential_skill_count` | long | Shared essential skills |
| `shared_optional_skill_count` | long | Shared optional skills |
| `source_skill_count` | long | Total skills of source occupation |
| `target_skill_count` | long | Total skills of target occupation |
| `gold_pipeline_version` | string | Lineage |
| `gold_computed_at` | string | Lineage |

### Key

`(source_occupation_uri, target_occupation_uri, country, ingestion_date)`

### Module

`src/skill_radar/domains/gold/analytics/occupation_similarity.py`

---

## 6. Dataset D — Occupation Transition Daily

### Purpose

Provides **career transition guidance** between occupation pairs:
shared skills, missing skills (gap analysis), difficulty score, and
market context (salary and jobs deltas).

### Algorithm

```
missing_essential = essential(target) - all_skills(source)
missing_optional  = optional(target)  - all_skills(source)
difficulty        = w_essential × |missing_essential| + w_optional × |missing_optional|
```

Skills gaps are resolved to human-readable labels and stored as JSON arrays.
Salary and job-count deltas are pulled from occupation profiles for market
context.

### Schema

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | string | Partition key |
| `country` | string | Partition key |
| `from_occupation_uri` | string | Source occupation URI |
| `from_occupation_label` | string | Source occupation name |
| `to_occupation_uri` | string | Target occupation URI |
| `to_occupation_label` | string | Target occupation name |
| `similarity_score` | double | Similarity score from the pairs table |
| `shared_skills_json` | string | JSON array of shared skill labels |
| `missing_skills_json` | string | JSON array of all missing skill labels |
| `missing_essential_skills_json` | string | JSON array of missing essential skills |
| `missing_optional_skills_json` | string | JSON array of missing optional skills |
| `missing_skill_count` | long | Total missing skills |
| `missing_essential_skill_count` | long | Missing essential skills |
| `transition_difficulty_score` | double | Weighted difficulty score |
| `from_avg_salary_mean` | double | Source occupations average salary |
| `to_avg_salary_mean` | double | Target occupation's average salary |
| `salary_delta_mean` | double | Salary difference (to - from) |
| `from_jobs_count` | long | Source occupation job count |
| `to_jobs_count` | long | Target occupation job count |
| `jobs_delta` | long | Job count difference (to - from) |
| `gold_pipeline_version` | string | Lineage |
| `gold_computed_at` | string | Lineage |

### Key

`(from_occupation_uri, to_occupation_uri, country, ingestion_date)`

### Module

`src/skill_radar/domains/gold/analytics/occupation_transitions.py`

---

## 7. Orchestrator Integration

The four datasets are computed in **Phases 8–11** of the Gold orchestrator,
following the existing Phases 1–7 (core matching + insight enhancements).

| Phase | Dataset | Soft-fail? | Dependencies |
|---|---|---|---|
| 8 | `occupation_profile_daily` | Yes | Gold matches, salary, ESCO Silver |
| 9 | `skill_profile_daily` | Yes | Gold demand, salary, matches, ESCO Silver |
| 10 | `occupation_similarity_daily` | Yes | ESCO Silver relations, occupations |
| 11 | `occupation_transition_daily` | Yes | Phase 10 output, Phase 8 output, ESCO Silver |

All phases use the **soft-fail pattern**: `try/except Exception` with
`logger.warning(..., exc_info=True)`. If any phase fails, subsequent phases
and the core pipeline are unaffected.

Phase 11 has a data dependency on Phase 10 (similarity pairs) and Phase 8
(occupation profiles for salary/jobs context). If either is unavailable,
Phase 11 is skipped with a warning.

### Module

`src/skill_radar/domains/gold/analytics/orchestrator.py`

---

## 8. Configuration Reference

All career navigation parameters live under `gold_analytics.career_nav` in
`PlatformSettings`:

```yaml
gold_analytics:
  career_nav:
    similarity:
      top_n: 20           # Max nearest neighbours per occupation
      w_essential: 2.0    # Weight for shared essential skills
      w_optional: 1.0     # Weight for shared optional skills
    transition:
      w_essential: 2.0    # Penalty per missing essential skill
      w_optional: 1.0     # Penalty per missing optional skill
    top_skills: 30         # Max skills in profile JSON fields
    top_companies: 20      # Max companies in profile JSON fields
    top_occupations: 30    # Max occupations in skill-profile JSON
```

### Pydantic Models

| Model | Location | Parent |
|---|---|---|
| `SimilarityConfig` | `config/models.py` | `CareerNavigationConfig` |
| `TransitionConfig` | `config/models.py` | `CareerNavigationConfig` |
| `CareerNavigationConfig` | `config/models.py` | `GoldAnalyticsConfig` |

---

## 9. Search Serving Layer

### Elasticsearch Index Mappings

Four new index mappings are registered in `platform/search/mappings.py`:

| Index Suffix | Key Fields | Notable Types |
|---|---|---|
| `occupation_profile_daily` | occupation URI, label | `text` for search_text, `keyword` for URIs |
| `skill_profile_daily` | skill URI, label | `text` for search_text, `keyword` for URIs |
| `occupation_similarity_daily` | source/target URIs | `double` for similarity_score |
| `occupation_transition_daily` | from/to URIs | `double` for difficulty, `long` for deltas |

JSON array fields (skills, companies, occupations) are mapped as
non-indexed `text` to keep documents self-contained without bloating the
inverted index.

### Document Builders

Each dataset has a document builder function in `domains/search/documents.py`
that converts Spark `Row` objects to Elasticsearch documents with
deterministic IDs (SHA-256[:20]).

### Dataset Registry

Four `ServedDataset` entries in `domains/search/datasets.py`. All four are
included in `PRIMARY_DATASETS` (total: 10 primary datasets, 12 total).

---

## 10. Kibana Dashboard — Career Navigation Explorer

**Dashboard E** provides a focused overview of occupation profiles:

| # | Visualization | Type | Description |
|---|---|---|---|
| 1 | Total Occupation Profiles | KPI (metric) | Count of profiled occupations |
| 2 | Occupations by Matched Jobs | Bar (horizontal) | Top occupations ranked by job demand |
| 3 | Occupations by Average Salary | Bar (horizontal) | Top occupations ranked by salary |
| 4 | Occupation Details | Data table | Searchable table with key metrics |

The dashboard is bound to the `occupation_profile_daily` data view and is
built programmatically by `_build_career_navigation()` in
`platform/search/kibana_builders.py`.

### Kibana Metadata

`DashboardDatasetMeta` entries for all four datasets are registered in
`domains/search/kibana_metadata.py`, providing field bindings for the Kibana
builder layer.

### Kibana Assets

- `EXPECTED_DASHBOARDS`: 5 (added "Career Navigation Explorer")
- `EXPECTED_DATA_VIEW_SUFFIXES`: 10 (added 4 career nav suffixes)

---

## 11. Validation & Observability

### Gold Validation Checks

Each of the four datasets gets the standard validation battery in
`platform/validate/checks/gold.py`:

1. **table_exists** — Iceberg table exists in catalog
2. **non_empty** — Table has at least one row
3. **schema** — All required columns are present
4. **partition_non_empty** — Target partition has data
5. **positive_counts** — Count columns are ≥ 0
6. **no_duplicates** — No duplicate composite keys
7. **lineage** — `gold_pipeline_version` and `gold_computed_at` present

### CLI Output

The `gold` CLI command now reports 4 additional row counts:

```
occupation_profile_rows:   ...
skill_profile_rows:        ...
occupation_similarity_rows: ...
occupation_transition_rows: ...
```

---

## 12. Module Map

```
src/skill_radar/
├── config/
│   └── models.py                    # SimilarityConfig, TransitionConfig,
│                                    # CareerNavigationConfig
├── domains/
│   ├── gold/
│   │   ├── schema.py                # 4 new REQUIRED / KEY / COUNT lists
│   │   ├── matching/
│   │   │   └── models.py            # GoldAnalyticsResult extended
│   │   └── analytics/
│   │       ├── orchestrator.py      # Phases 8-11
│   │       ├── occupation_profiles.py   # NEW
│   │       ├── skill_profiles.py        # NEW
│   │       ├── occupation_similarity.py # NEW
│   │       └── occupation_transitions.py # NEW
│   └── search/
│       ├── datasets.py              # 4 ServedDataset + PRIMARY_DATASETS
│       ├── documents.py             # 4 document builders
│       └── kibana_metadata.py       # 4 DashboardDatasetMeta entries
├── platform/
│   ├── lake/
│   │   └── layout.py               # 4 gold_*_fqn() methods
│   ├── search/
│   │   ├── mappings.py              # 4 ES mappings
│   │   ├── kibana_builders.py       # Dashboard E builder
│   │   └── kibana_assets.py         # EXPECTED_DASHBOARDS/DVs updated
│   └── validate/
│       └── checks/
│           └── gold.py              # 4 × 7 validation checks
└── cli/
    └── gold.py                      # 4 output lines
```

---

## 13. Testing

### Unit Tests Updated

| Test File | Changes |
|---|---|
| `test_documents.py` | 12 expected builders |
| `test_datasets.py` | 12 datasets, 10 primary |
| `test_kibana_metadata.py` | 10 metadata entries |
| `test_kibana_assets.py` | 10 DVs, 5 dashboards, 24 vis, 49 total objects |
| `test_kibana_builders.py` | 5 dashboard tuples, 10 DVs, 24 vis |
| `test_mappings.py` | 12 mappings registered |
| `test_layout.py` | 12 FQNs, 4 new career nav FQN tests |
| `test_validation_checks.py` | 12 table_exists checks, 4 schema alias tests |

### Running Tests

```bash
# Full local check (lint + format + mypy + unit tests)
make check

# CI pipeline (same + coverage)
make ci

# Auto-fix formatting
make fix
```
