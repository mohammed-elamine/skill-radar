# Kibana Dashboards Guide

> **Code-managed, reproducible dashboards for Skill Radar.**
>
> All Kibana assets — data views, visualizations, dashboards, saved searches —
> are generated deterministically from Python code. No manual Kibana UI authoring
> is used. This ensures reproducibility, version control, and environment parity.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Dashboard Inventory](#dashboard-inventory)
3. [Data Views](#data-views)
4. [How to Generate Assets](#how-to-generate-assets)
5. [How to Apply to Kibana](#how-to-apply-to-kibana)
6. [How to Validate](#how-to-validate)
7. [How to Extend](#how-to-extend)
8. [Module Reference](#module-reference)
9. [Design Decisions](#design-decisions)
10. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  kibana_metadata.py                                         │
│  (Single source of truth: fields, roles, dataset config)    │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│  kibana_builders.py                                          │
│  (Lens column builders → visualization states → dashboards)  │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│  kibana_models.py           kibana_assets.py                 │
│  (Typed saved objects)      (NDJSON generation + writing)    │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│  kibana.py  (HTTP client)                                    │
│  import_saved_objects() ─► POST /api/saved_objects/_import   │
└─────────────────────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│  kibana_orchestrator.py                                      │
│  generate_kibana_assets() / apply_kibana_assets()            │
└─────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer | Module | Responsibility |
|-------|--------|----------------|
| **Metadata** | `domains/search/kibana_metadata.py` | Field bindings, dataset config, validation |
| **Models** | `platform/search/kibana_models.py` | Typed saved object representations |
| **Builders** | `platform/search/kibana_builders.py` | Construct Lens visualization states and dashboard layouts |
| **Assets** | `platform/search/kibana_assets.py` | NDJSON generation, writing, expected inventory |
| **Client** | `platform/search/kibana.py` | HTTP calls to Kibana Saved Objects API |
| **Orchestrator** | `domains/search/kibana_orchestrator.py` | End-to-end generate/apply/bootstrap workflow |
| **CLI** | `cli/search.py` | `dashboard export` and `dashboard apply` commands |

---

## Dashboard Inventory

### Dashboard A: Market Overview

**Dataset:** `skill_demand_daily`
**Dashboard ID:** `skillradar-dash-market-overview`

| # | Visualization | Type | Description |
|---|--------------|------|-------------|
| 0 | KPI: Total Records | `lnsMetric` | Total skill-demand records in scope |
| 1 | Top Skills by Jobs Count | `lnsXY` (bar) | Top 20 demanded skills |
| 2 | Demand Trend Over Time | `lnsXY` (line) | Trend of job postings over dates |
| 3 | Ranked Skills Table | `lnsDatatable` | Skills with companies/locations |
| 4 | Match Source Distribution | `lnsXY` (stacked bar) | Title vs description match split |

### Dashboard B: Salary Intelligence

**Dataset:** `salary_by_skill_daily`
**Dashboard ID:** `skillradar-dash-salary-intelligence`

| # | Visualization | Type | Description |
|---|--------------|------|-------------|
| 0 | KPI: Skills with Salary Data | `lnsMetric` | Count of salary-bearing records |
| 1 | Top Skills by Average Salary | `lnsXY` (bar) | Top 20 by salary level |
| 2 | Top Skills by Salary Job Volume | `lnsXY` (bar) | Highest salary job postings |
| 3 | Salary Trend Over Time | `lnsXY` (line) | Average salary trend |
| 4 | Skill Salary Details | `lnsDatatable` | Full salary metrics table |

### Dashboard C: Occupation–Skill Graph Explorer

**Dataset:** `occupation_skill_graph`
**Dashboard ID:** `skillradar-dash-occ-skill-graph`

| # | Visualization | Type | Description |
|---|--------------|------|-------------|
| 0 | KPI: Total Relationships | `lnsMetric` | Total occ-skill rows |
| 1 | KPI: Unique Occupations | `lnsMetric` | Distinct occupations |
| 2 | KPI: Unique Skills | `lnsMetric` | Distinct skills |
| 3 | Top Skills by Matched Jobs | `lnsXY` (bar) | Skills with most evidence |
| 4 | Top Occupations by Matched Jobs | `lnsXY` (bar) | Occupations with most evidence |
| 5 | Relationship Table | `lnsDatatable` | Full occupation↔skill breakdown |

---

## Data Views

Each served dataset gets a dedicated Kibana data view:

| Data View | Index Pattern | Time Field |
|-----------|--------------|------------|
| Skill Radar — Skill Demand Daily | `skillradar-skill-demand-daily-*` | `ingestion_date` |
| Skill Radar — Salary by Skill Daily | `skillradar-salary-by-skill-daily-*` | `ingestion_date` |
| Skill Radar — Occupation-Skill Graph | `skillradar-occupation-skill-graph-*` | `ingestion_date` |

Data view IDs follow the pattern: `{index_prefix}-dv-{index_suffix}`

---

## How to Generate Assets

### CLI

```bash
# Generate NDJSON artifact (default: configs/kibana/skill_radar_dashboards.ndjson)
skill-radar search dashboard export

# Custom output directory
skill-radar search dashboard export --output-dir ./artifacts

# Custom filename
skill-radar search dashboard export --filename my_dashboards.ndjson
```

### Makefile

```bash
make export-kibana-assets
```

### Python

```python
from skill_radar.config.loader import load_platform_config
from skill_radar.domains.search.kibana_orchestrator import generate_kibana_assets

config = load_platform_config()
result = generate_kibana_assets(config)
print(f"Written to {result.artifact_path}: {result.total_objects} objects")
```

---

## How to Apply to Kibana

### CLI

```bash
# Apply directly to running Kibana
skill-radar search dashboard apply

# Dry-run (validate without pushing)
skill-radar search dashboard apply --dry-run

# Custom Kibana URL
skill-radar search dashboard apply --kibana-url http://kibana:5601

# Don't overwrite existing objects
skill-radar search dashboard apply --no-overwrite
```

### Makefile

```bash
make apply-kibana-assets
```

### Manual Import

1. Generate the NDJSON file: `make export-kibana-assets`
2. Open Kibana → Stack Management → Saved Objects
3. Click "Import" and upload `configs/kibana/skill_radar_dashboards.ndjson`
4. Select "Overwrite existing objects" if updating

---

## How to Validate

### Makefile

```bash
make validate-kibana
```

### Validation Checks

The validation framework includes these Kibana-specific checks:

| Check | Description |
|-------|-------------|
| `search.kibana.data_view.{suffix}` | Data view exists in Kibana with correct title |
| `search.kibana.dashboard.{slug}` | Dashboard exists in Kibana with correct title |

These checks run as part of `get_search_checks()` when Kibana is reachable.

---

## How to Extend

### Adding a New Dashboard

1. **Define metadata** in `kibana_metadata.py`:
   ```python
   NEW_DATASET_META = DashboardDatasetMeta(
       key="my_new_dataset",
       index_suffix="my-new-dataset",
       data_view_title="Skill Radar — My New Dataset",
       time_field="ingestion_date",
       label="My New Dataset",
       description="Description of what this dataset contains",
       fields=(
           FieldMeta("ingestion_date", "Date", "time"),
           FieldMeta("my_dimension", "My Dim", "dimension", agg_field="my_dimension.raw"),
           FieldMeta("my_metric", "My Metric", "metric"),
       ),
   )
   ```

2. **Add to registry**:
   ```python
   DASHBOARD_DATASETS["my_new_dataset"] = NEW_DATASET_META
   PRIMARY_DASHBOARD_DATASETS.append("my_new_dataset")
   ```

3. **Create builder** in `kibana_builders.py`:
   ```python
   def _build_my_new_dashboard(meta, data_view_id):
       # Build visualizations and panels...
       return vises, dashboard

   _DASHBOARD_BUILDERS["my_new_dataset"] = ("my-new", _build_my_new_dashboard)
   ```

4. **Update expected inventory** in `kibana_assets.py` if needed.

5. **Run tests**: `uv run pytest tests/unit/platform/search/test_kibana_builders.py -v`

### Adding a Visualization to an Existing Dashboard

Edit the corresponding `_build_*` function in `kibana_builders.py`. Each function
constructs its visualization list and panel layout explicitly.

### Changing Field Bindings

Edit only `kibana_metadata.py`. The `FieldMeta` entries control what fields
builders use. The `agg_field` parameter handles `.raw` sub-fields for text+keyword
mappings automatically.

---

## Module Reference

### Deterministic ID Scheme

| Object Type | Pattern | Example |
|-------------|---------|---------|
| Data View | `{prefix}-dv-{suffix}` | `skillradar-dv-skill-demand-daily` |
| Visualization | `skillradar-vis-{dashboard}-{viz}` | `skillradar-vis-market-overview-top-skills-bar` |
| Dashboard | `skillradar-dash-{slug}` | `skillradar-dash-market-overview` |
| Saved Search | `skillradar-ss-{suffix}` | `skillradar-ss-skill-demand-daily` |

### NDJSON Format

Each line in the NDJSON file is a complete JSON object:

```json
{"type": "index-pattern", "id": "...", "attributes": {...}}
{"type": "search", "id": "...", "attributes": {...}, "references": [...]}
{"type": "lens", "id": "...", "attributes": {...}, "references": [...]}
{"type": "dashboard", "id": "...", "attributes": {...}, "references": [...]}
```

Objects are ordered by dependency: data views → saved searches → visualizations → dashboards.

### Lens Visualization Format

Visualizations use Kibana 8.x Lens format. The `state` attribute is a JSON
string containing:

- `datasourceStates.formBased.layers` — column aggregation definitions
- `visualization` — rendering configuration (series type, accessors, legend)
- `filters`, `query` — default filter/query state

---

## Design Decisions

### Why Code-Managed?

- **Reproducibility**: Same code = same dashboards across all environments
- **Version Control**: Dashboard changes are tracked in Git with full diff history
- **No UI Drift**: Eliminates manual edits that diverge between dev/staging/prod
- **CI/CD Integration**: Assets can be generated and validated in pipelines
- **Single Source of Truth**: Field bindings defined once in metadata, not scattered

### Why Lens Format?

Lens is the standard Kibana 8.x visualization format. It's the primary
recommended format for new visualizations and supports all common chart types
(metric, bar, line, area, pie, datatable).

### Why NDJSON over Direct API?

The generate-then-apply pattern supports:
- Offline artifact inspection before import
- Manual review of generated objects
- Import via CLI, API, or Kibana UI
- Archival and rollback of dashboard versions

### Why Separate Metadata from Builders?

The metadata layer (`kibana_metadata.py`) owns *what* to visualize. The builder
layer (`kibana_builders.py`) owns *how* to visualize it. This separation means:
- Adding a field requires editing only metadata
- Changing a chart type requires editing only the builder
- Both can be tested independently

---

## Troubleshooting

### "No mapping found for index suffix"

The metadata references an index suffix that doesn't exist in `MAPPINGS`.
Ensure the dataset has a corresponding entry in `platform/search/mappings.py`.

### "Field 'X' not in mapping"

A field in the metadata doesn't exist in the Elasticsearch mapping for that
index. Check for typos or missing fields in `mappings.py`.

### Import fails with "conflict"

Use `--overwrite` (default) to replace existing objects. Without it, Kibana
rejects objects with IDs that already exist.

### Kibana not reachable

Ensure Kibana is running: `make search-up`. Check the URL:
`SKILLRADAR_SEARCH_KIBANA_URL` environment variable or `kibana_url` in config.

### Visualization shows "No data"

The data view pattern must match existing Elasticsearch indices. Ensure data
has been exported first: `skill-radar search export --ingestion-date ... --country fr`
