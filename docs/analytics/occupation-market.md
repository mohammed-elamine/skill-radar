# Occupation Market

Daily occupation market indicators aggregated from Gold matching data.

## Purpose

Produce the `occupation_market_daily` Gold table (Phase 7a) with per-occupation demand, skill breadth, and match quality indicators.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `esco_occupation_concept_uri` | string | ESCO occupation URI |
| `esco_occupation_preferred_label` | string | Occupation name |
| `matched_jobs_count` | long | Job postings matched to this occupation |
| `distinct_skills_count` | long | Unique skills matched across these jobs |
| `avg_match_confidence` | double | Average occupation match confidence |
| `skill_breadth_ratio` | double | Skills matched / total ESCO skills for occupation |
| `ingestion_date`, `country` | string | Partition keys |
| `gold_pipeline_version`, `gold_computed_at` | string | Lineage |

**Key:** `(esco_occupation_concept_uri, country, ingestion_date)`

## Data Sources

- `job_occupation_matches` — matched jobs per occupation
- `job_skill_matches` — skills matched per job
- ESCO Silver `occupation_skill_relations` — total skills per occupation (for breadth ratio)

## Soft-Fail Behavior

Runs with soft-fail. If the upstream occupation matches are empty (e.g., no matching was successful), this phase is skipped with a warning.

## Module

`src/skill_radar/domains/gold/analytics/occupation_market.py`

## References

- [Gold Pipeline](../pipelines/gold.md) — Phase 7a context
- [Emerging Skills](emerging-skills.md) — skill-level trend analysis
- [Career Navigation](../pipelines/career-navigation.md) — occupation profiles (richer view)
