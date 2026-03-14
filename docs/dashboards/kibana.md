# Kibana Dashboards

Dashboard inventory with visualization details for Market Overview, Salary Intelligence, and Occupation–Skill Graph.

## Dashboard A — Market Overview

**Dataset:** `skill_demand_daily`
**Dashboard ID:** `skillradar-dash-market-overview`

| # | Visualization | Type | Description |
|---|--------------|------|-------------|
| 0 | KPI: Total Records | `lnsMetric` | Total skill-demand records |
| 1 | Top Skills by Jobs Count | `lnsXY` (bar) | Top 20 demanded skills |
| 2 | Demand Trend Over Time | `lnsXY` (line) | Job posting trend over dates |
| 3 | Ranked Skills Table | `lnsDatatable` | Skills with companies/locations |
| 4 | Match Source Distribution | `lnsXY` (stacked bar) | Title vs description match split |

## Dashboard B — Salary Intelligence

**Dataset:** `salary_by_skill_daily`
**Dashboard ID:** `skillradar-dash-salary-intelligence`

| # | Visualization | Type | Description |
|---|--------------|------|-------------|
| 0 | KPI: Skills with Salary Data | `lnsMetric` | Count of salary-bearing records |
| 1 | Top Skills by Average Salary | `lnsXY` (bar) | Top 20 by salary level |
| 2 | Top Skills by Salary Job Volume | `lnsXY` (bar) | Highest salary job counts |
| 3 | Salary Trend Over Time | `lnsXY` (line) | Average salary trend |
| 4 | Skill Salary Details | `lnsDatatable` | Full salary metrics table |

## Dashboard C — Occupation–Skill Graph Explorer

**Dataset:** `occupation_skill_graph`
**Dashboard ID:** `skillradar-dash-occ-skill-graph`

| # | Visualization | Type | Description |
|---|--------------|------|-------------|
| 0 | KPI: Total Relationships | `lnsMetric` | Total occ-skill rows |
| 1 | KPI: Unique Occupations | `lnsMetric` | Distinct occupations |
| 2 | KPI: Unique Skills | `lnsMetric` | Distinct skills |
| 3 | Top Skills by Matched Jobs | `lnsXY` (bar) | Skills with most evidence |
| 4 | Top Occupations by Matched Jobs | `lnsXY` (bar) | Occupations with most evidence |
| 5 | Relationship Table | `lnsDatatable` | Full occupation ↔ skill breakdown |

## Dashboard D — Emerging Skills

**Dataset:** `skill_emerging_daily`

Visualizes emerging skill scores, momentum trends, and novelty detection.

## Lens Visualization Format

All visualizations use Kibana 8.x Lens format with:
- `datasourceStates.formBased.layers` — column aggregation definitions
- `visualization` — rendering config (series type, accessors, legend)
- `filters`, `query` — default filter/query state

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No mapping found for index suffix" | Ensure dataset has entry in `platform/search/mappings.py` |
| "Field 'X' not in mapping" | Check for typos in `mappings.py` |
| Import fails with "conflict" | Use `--overwrite` (default) |
| Kibana not reachable | `make search-up`, check `SKILLRADAR_SEARCH_KIBANA_URL` |
| Visualization shows "No data" | Ensure data exported first; data view pattern must match ES indices |

## References

- [Dashboard Overview](overview.md) — code-managed architecture
- [Career Navigation Explorer](career-navigation-explorer.md) — Dashboard E
- [Search Pipeline](../pipelines/search.md) — data export
