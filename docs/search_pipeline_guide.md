# Search Serving Pipeline Guide

This guide documents the Elasticsearch + Kibana serving stage for Skill Radar.

## Overview

The search stage exports Gold-layer Iceberg tables to Elasticsearch indices,
enabling fast, low-latency querying via the Elasticsearch API and visual
exploration through Kibana dashboards.

**Design Principles**:
- **Gold is truth**: Iceberg tables remain the source of truth; Elasticsearch is
  a derived serving layer that can be rebuilt at any time.
- **Deterministic IDs**: Each document has a content-based `doc_id` (SHA-256 truncated
  to 20 hex chars) ensuring idempotent re-indexing.
- **Alias-based routing**: Queries hit stable aliases (`skillradar-{dataset}-{country}`)
  while physical indices are date-versioned (`skillradar-{dataset}-{country}-{YYYY.MM.DD}`).

## Architecture

```
┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│  Gold Iceberg     │ --> │  Elasticsearch    │ --> │  Kibana           │
│  (source of truth)│     │  (search/serving) │     │  (visualization)  │
└───────────────────┘     └───────────────────┘     └───────────────────┘
         │                         │                         │
         │ partition:              │ index:                  │ data view:
         │ ingestion_date/country  │ skillradar-{ds}-{c}-{d} │ skillradar-{ds}-{c}
         └─────────────────────────┴─────────────────────────┴──────────────────────
```

## Served Datasets

| Dataset | Description | Primary? |
|---------|-------------|----------|
| `skill_demand_daily` | Daily skill demand aggregates | ✓ |
| `salary_by_skill_daily` | Salary statistics per skill | ✓ |
| `occupation_skill_graph` | Occupation↔Skill relationships | ✓ |
| `job_skill_matches` | Per-job skill matches | |
| `job_occupation_matches` | Per-job occupation matches | |

Primary datasets are always exported; job-level datasets can be included via
`--dataset` flag.

## Quick Start

### 1. Start Elasticsearch + Kibana

```bash
make search-up
```

This starts the `search` Docker Compose profile with:
- Elasticsearch 8.13.4 on `http://localhost:9200`
- Kibana 8.13.4 on `http://localhost:5601`

### 2. Export Gold to Elasticsearch

```bash
make export-search SEARCH_COUNTRY=fr SEARCH_INGESTION_DATE=2025-01-15
```

Or via CLI directly:

```bash
docker compose exec -T spark bash -lc \
  "uv run skill-radar search export --ingestion-date 2025-01-15 --country fr --alias-swap --refresh"
```

### 3. Validate Indices

```bash
make validate-search SEARCH_COUNTRY=fr SEARCH_INGESTION_DATE=2025-01-15
```

### 4. Bootstrap Kibana Data Views

```bash
make bootstrap-kibana
```

### 5. Full Pipeline

```bash
make run-search SEARCH_COUNTRY=fr SEARCH_INGESTION_DATE=2025-01-15
```

## CLI Reference

### `skill-radar search export`

Export Gold Iceberg partitions to Elasticsearch.

```bash
skill-radar search export \
  --ingestion-date 2025-01-15 \
  --country fr \
  [--dataset skill_demand_daily] \
  [--es-url http://elasticsearch:9200] \
  [--create-index|--no-create-index] \
  [--refresh] \
  [--alias-swap] \
  [--dry-run] \
  [--quiet]
```

| Option | Description | Default |
|--------|-------------|---------|
| `--ingestion-date` | Partition date (YYYY-MM-DD) | (required) |
| `--country` | Country code | (required) |
| `--dataset` | Specific dataset(s), repeat for multiple | Primary datasets |
| `--es-url` | Elasticsearch URL | Config default |
| `--create-index` | Create index if missing | Enabled |
| `--refresh` | Refresh index after bulk | Disabled |
| `--alias-swap` | Swap alias to new index | Disabled |
| `--dry-run` | Log actions without executing | Disabled |

### `skill-radar search bootstrap-kibana`

Create Kibana data views from Elasticsearch aliases.

```bash
skill-radar search bootstrap-kibana \
  [--es-url http://elasticsearch:9200] \
  [--kibana-url http://localhost:5601] \
  [--output-file kibana-artifacts.ndjson] \
  [--apply]
```

### `skill-radar validate search`

Validate Elasticsearch indices and Kibana health.

```bash
skill-radar validate search \
  --ingestion-date 2025-01-15 \
  --country fr \
  [--dataset skill_demand_daily] \
  [--es-url http://elasticsearch:9200] \
  [--infra-only] \
  [--upload] \
  [--quiet]
```

## Makefile Targets

| Target | Description |
|--------|-------------|
| `search-up` | Start Elasticsearch + Kibana |
| `search-down` | Stop Elasticsearch + Kibana |
| `search-reset` | Stop + remove data volumes |
| `search-logs` | Tail ES/Kibana logs |
| `validate-search-infra` | Check ES/Kibana reachable |
| `export-search` | Export Gold → ES |
| `validate-search` | Validate indices |
| `bootstrap-kibana` | Create Kibana data views |
| `run-search` | Full pipeline: export → validate → bootstrap |

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SKILLRADAR_SEARCH_ENABLED` | Enable search stage | `true` |
| `SKILLRADAR_SEARCH_ELASTICSEARCH_URL` | ES URL (host) | `http://localhost:9200` |
| `SKILLRADAR_SEARCH_ELASTICSEARCH_URL_DOCKER` | ES URL (Docker) | `http://elasticsearch:9200` |
| `SKILLRADAR_SEARCH_KIBANA_URL` | Kibana URL (host) | `http://localhost:5601` |
| `SKILLRADAR_SEARCH_INDEX_PREFIX` | Index name prefix | `skillradar` |
| `SKILLRADAR_SEARCH_INDEX_SHARDS` | Shards per index | `1` |
| `SKILLRADAR_SEARCH_INDEX_REPLICAS` | Replicas per index | `0` |
| `SKILLRADAR_SEARCH_BULK_CHUNK_SIZE` | Docs per bulk request | `500` |

### YAML Configuration

```yaml
# configs/defaults.yaml (excerpt)
search:
  enabled: true
  elasticsearch_url: "http://localhost:9200"
  elasticsearch_url_docker: "http://elasticsearch:9200"
  kibana_url: "http://localhost:5601"
  index_prefix: "skillradar"
  index_shards: 1
  index_replicas: 0
  bulk_chunk_size: 500
  indices:
    skill_demand_daily: "skill-demand-daily"
    salary_by_skill_daily: "salary-by-skill-daily"
    occupation_skill_graph: "occupation-skill-graph"
    job_skill_matches: "job-skill-matches"
    job_occupation_matches: "job-occupation-matches"
```

## Index Naming

| Component | Pattern | Example |
|-----------|---------|---------|
| Physical index | `{prefix}-{dataset}-{country}-{date}` | `skillradar-skill-demand-daily-fr-2025.01.15` |
| Alias | `{prefix}-{dataset}-{country}` | `skillradar-skill-demand-daily-fr` |

Aliases provide stable query endpoints while physical indices enable atomic
daily swaps and historical retention.

## Document ID Strategy

Documents use deterministic IDs built from partition keys and primary keys:

| Dataset | ID Components |
|---------|--------------|
| `skill_demand_daily` | `country \| ingestion_date \| skill_uri` |
| `salary_by_skill_daily` | `country \| ingestion_date \| skill_uri` |
| `occupation_skill_graph` | `country \| ingestion_date \| occ_uri \| skill_uri` |
| `job_skill_matches` | `country \| ingestion_date \| job_id \| skill_uri` |
| `job_occupation_matches` | `country \| ingestion_date \| job_id \| occ_uri \| method` |

IDs are SHA-256 hashed and truncated to 20 hex characters.

## Testing

### Unit Tests

```bash
uv run pytest tests/unit/platform/search/ tests/unit/domains/search/ -v
```

### Integration Tests

```bash
make search-up
uv run pytest tests/integration/platform/search/ -v
```

## Troubleshooting

### Elasticsearch not reachable

```bash
# Check ES is running
docker compose --profile search ps

# Check ES health
curl -s http://localhost:9200/_cluster/health | jq

# Check logs
make search-logs
```

### Index not created

```bash
# List indices
curl -s http://localhost:9200/_cat/indices?v

# Force create
skill-radar search export --ingestion-date 2025-01-15 --country fr --create-index
```

### Documents not appearing in Kibana

```bash
# Verify alias exists
curl -s http://localhost:9200/_cat/aliases?v

# Refresh index
curl -X POST http://localhost:9200/skillradar-skill-demand-daily-fr/_refresh

# Check data view pattern matches alias
# In Kibana: Stack Management → Data Views
```

## Related Documentation

- [Gold Pipeline Guide](gold_pipeline_guide.md)
- [Airflow Orchestration Guide](airflow_orchestration_guide.md)
- [Platform Validation Guide](platform_validation_guide.md)
