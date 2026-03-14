# ML Features

KMeans skill demand segmentation and feature engineering for analytical clustering.

## Purpose

Segment skills into demand clusters (niche, growing, established, dominant) using KMeans clustering on demand metrics. Produces the `skill_demand_segments_daily` Gold table (Phase 7b).

## Algorithm

### Feature Vector

Each skill is characterized by a feature vector derived from demand metrics:

| Feature | Source | Description |
|---------|--------|-------------|
| `jobs_count` | `skill_demand_daily` | Total job postings |
| `companies_count` | `skill_demand_daily` | Distinct hiring companies |
| `locations_count` | `skill_demand_daily` | Distinct posting locations |
| `avg_match_score` | `skill_demand_daily` | Average match confidence |
| `title_match_ratio` | Computed | Proportion of matches from job titles |

### KMeans Clustering

- **Algorithm**: Spark MLlib KMeans
- **K**: 4 clusters (configurable)
- **Features**: Standardized (zero mean, unit variance) via `StandardScaler`
- **Seed**: Fixed for reproducibility

### Cluster Interpretation

Clusters are post-hoc labeled based on centroid characteristics:

| Segment | Characteristics |
|---------|----------------|
| **Niche** | Low jobs count, few companies, concentrated locations |
| **Growing** | Moderate demand, spreading across companies |
| **Established** | High demand, many companies, broad geographic spread |
| **Dominant** | Highest demand, highest salary, ubiquitous |

Labels are assigned by sorting centroids by `jobs_count` and mapping to the ordered segment names.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `esco_skill_concept_uri` | string | ESCO skill URI |
| `esco_skill_preferred_label` | string | Skill name |
| `segment_label` | string | Cluster label (niche/growing/established/dominant) |
| `segment_id` | integer | Cluster ID from KMeans |
| `jobs_count` | long | Total job postings |
| `companies_count` | long | Distinct companies |
| `locations_count` | long | Distinct locations |
| `ingestion_date`, `country` | string | Partition keys |
| `gold_pipeline_version`, `gold_computed_at` | string | Lineage |

**Key:** `(esco_skill_concept_uri, country, ingestion_date)`

## Soft-Fail Behavior

Runs with soft-fail — if KMeans fails (e.g., insufficient data for 4 clusters), the error is logged and downstream phases continue.

## Module

`src/skill_radar/domains/gold/analytics/skill_segments.py`

## References

- [Gold Pipeline](../pipelines/gold.md) — Phase 7b context
- [Emerging Skills](emerging-skills.md) — complementary trend analysis
- [Occupation Market](occupation-market.md) — occupation-level analytics
