# Lakehouse Layout

Bucket structure, Iceberg namespaces, naming conventions, and table inventory.

## Bucket Layout

| Bucket | Purpose |
|--------|---------|
| `skillradar-lake` | Data lake (Iceberg warehouse, landing zone) |
| `skillradar-logs` | Validation reports, operational logs |

## Iceberg Warehouse

```
s3a://skillradar-lake/
├── warehouse/           # Iceberg tables
│   └── sr_bronze/
│   └── sr_silver/
│   └── sr_gold/
└── data/                # Lake paths (landing zone)
    └── landing/
        └── esco/{version}/{lang}/
```

Warehouse root: `s3a://skillradar-lake/warehouse`

## Iceberg Namespaces

| Namespace | Catalog FQN | Layer |
|-----------|------------|-------|
| `sr_bronze` | `sr.sr_bronze` | Raw ingestion (full fidelity) |
| `sr_silver` | `sr.sr_silver` | Cleaned, normalized, deduplicated |
| `sr_gold` | `sr.sr_gold` | Analytical datasets (cross-domain) |

## Lake Path Convention

```
s3a://skillradar-lake/data/<layer>/<domain>/<source>/<entity>/...
```

Example: `s3a://skillradar-lake/data/landing/esco/v1.2.1/fr/esco.zip`

## Table Inventory

### Bronze Tables

| Table FQN | Source | Partitioning |
|-----------|--------|-------------|
| `sr.sr_bronze.esco_skills_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_occupations_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_skill_groups_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_occupation_skill_relations_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_skill_skill_relations_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_broader_relations_skill_pillar_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_broader_relations_occ_pillar_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_skills_hierarchy_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.esco_isco_groups_raw` | ESCO | `(dataset, version, lang)` |
| `sr.sr_bronze.adzuna_jobs_raw` | Adzuna | `(ingestion_date, country)` |
| `sr.sr_bronze.adzuna_request_log_raw` | Adzuna | `(ingestion_date, country)` |

### Silver Tables

| Table FQN | Source | Partitioning |
|-----------|--------|-------------|
| `sr.sr_silver.esco_skills` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_occupations` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_skill_groups` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_occupation_skill_relations` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_skill_skill_relations` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_broader_relations_skill_pillar` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_broader_relations_occ_pillar` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_skills_hierarchy` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.esco_isco_groups` | ESCO | `(dataset, version, lang)` |
| `sr.sr_silver.adzuna_jobs` | Adzuna | `(country, ingestion_date)` |

### Gold Tables

| Table FQN | Key Columns | Description |
|-----------|-------------|-------------|
| `sr.sr_gold.job_skill_matches` | `job_id, skill_uri` | Per-job skill match results |
| `sr.sr_gold.job_occupation_matches` | `job_id, occ_uri, method` | Per-job occupation matches |
| `sr.sr_gold.skill_demand_daily` | `skill_uri, country, date` | Daily skill demand frequency |
| `sr.sr_gold.salary_by_skill_daily` | `skill_uri, country, date` | Salary statistics per skill |
| `sr.sr_gold.occupation_skill_graph` | `occ_uri, skill_uri, country, date` | Occupation ↔ skill relations |
| `sr.sr_gold.skill_emerging_daily` | `skill_uri, country, date` | Emerging skill scores |
| `sr.sr_gold.occupation_market_daily` | `occ_uri, country, date` | Occupation market indicators |
| `sr.sr_gold.skill_demand_segments_daily` | `skill_uri, segment, country, date` | Demand by segment |
| `sr.sr_gold.occupation_profile_daily` | `occ_uri, country, date` | Rich occupation profiles |
| `sr.sr_gold.skill_profile_daily` | `skill_uri, country, date` | Detailed skill profiles |
| `sr.sr_gold.occupation_similarity_daily` | `source_uri, target_uri, country, date` | Pairwise similarity |
| `sr.sr_gold.occupation_transition_daily` | `from_uri, to_uri, country, date` | Career transitions |

All Gold tables are partitioned by `(country, ingestion_date)`.

## LakeLayout SSOT

All table FQNs are resolved through `LakeLayout` (`platform/lake/layout.py`). No hardcoded table names exist elsewhere in the codebase.

```python
from skill_radar.platform.lake.layout import LakeLayout
layout = LakeLayout()
layout.gold_skill_demand_fqn()  # → "sr.sr_gold.skill_demand_daily"
```

## References

- [System Components](system-components.md) — MinIO and Iceberg configuration
- [Data Model](data-model.md) — detailed Gold schemas
- [Modular Design](modular-design.md) — LakeLayout as SSOT
