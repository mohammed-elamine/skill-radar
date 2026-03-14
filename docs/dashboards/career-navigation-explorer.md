# Career Navigation Explorer

Kibana dashboard for exploring occupation profiles, demand, and salary data.

## Purpose

**Dashboard E** provides a focused overview of ESCO occupation profiles enriched with market data from Gold analytics.

## Dashboard Details

**Dataset:** `occupation_profile_daily`
**Dashboard ID:** `skillradar-dash-career-navigation`

| # | Visualization | Type | Description |
|---|--------------|------|-------------|
| 1 | Total Occupation Profiles | KPI (metric) | Count of profiled occupations |
| 2 | Occupations by Matched Jobs | Bar (horizontal) | Top occupations ranked by job demand |
| 3 | Occupations by Average Salary | Bar (horizontal) | Top occupations ranked by salary |
| 4 | Occupation Details | Data table | Searchable table with key metrics |

## Data Source

Bound to the `occupation_profile_daily` data view, which covers:
- Occupation name and ESCO URI
- `matched_jobs_count` — total job demand
- `distinct_companies_count` — hiring breadth
- `avg_salary_mean` — weighted salary
- `top_essential_skills_json` — key required skills
- `top_companies_json` — top employers

## Kibana Metadata

`DashboardDatasetMeta` entries for all four career navigation datasets are registered in `domains/search/kibana_metadata.py`:
- `occupation_profile_daily`
- `skill_profile_daily`
- `occupation_similarity_daily`
- `occupation_transition_daily`

## Asset Inventory

After adding career navigation:
- **Total dashboards**: 5
- **Total data views**: 10
- **Total visualizations**: 24
- **Total NDJSON objects**: 49

## Builder

Built programmatically by `_build_career_navigation()` in `platform/search/kibana_builders.py`.

## References

- [Career Navigation Pipeline](../pipelines/career-navigation.md) — data source details
- [Dashboard Overview](overview.md) — code-managed architecture
- [Kibana Dashboards](kibana.md) — other dashboards
