# Command Reference

Complete reference for the `skill-radar` CLI and Makefile targets.

## CLI Overview

```
skill-radar [OPTIONS] COMMAND [ARGS]...
```

| Group | Purpose |
|-------|---------|
| `adzuna` | Adzuna dataset operations (Bronze / Silver) |
| `esco` | ESCO dataset operations |
| `gold` | Gold layer operations (matching / analytics) |
| `infra` | Infrastructure provisioning and status |
| `run` | Coarse-grained stage units (process + validate) |
| `search` | Search serving (Gold → Elasticsearch) |
| `validate` | Validation commands for infra and datasets |

## Infrastructure

### `skill-radar infra apply`

Provision infrastructure (S3 buckets + Iceberg namespaces).

| Option | Default | Description |
|--------|---------|-------------|
| `--upload` | off | Upload report to S3 logs bucket |
| `--quiet` | off | Suppress console output |
| `--skip-namespaces` | off | Skip namespace creation (buckets only) |
| `--context` | auto-detected | Runtime context: `host` or `docker` |

### `skill-radar infra status`

Show infrastructure status (read-only validation).

## ESCO Dataset

### `skill-radar esco upload`

Upload an ESCO artifact to the S3 landing zone.

### `skill-radar esco bronze`

| Option | Default | Description |
|--------|---------|-------------|
| `--version` | required | Artifact version (e.g. `v1.2.1`) |
| `--lang` | required | Language code (e.g. `fr`) |
| `--entities` | all | Comma-separated subset of entities |
| `--fail-fast` / `--no-fail-fast` | on | Abort on first entity error |
| `--validate-landing` / `--no-validate-landing` | on | Run landing validation first |
| `--upload` | off | Upload report to S3 logs bucket |

### `skill-radar esco silver`

| Option | Default | Description |
|--------|---------|-------------|
| `--version` | required | Artifact version |
| `--lang` | required | Language code |
| `--entities` | all | Comma-separated subset |
| `--upload` | off | Upload report to S3 |

## Adzuna Dataset

### `skill-radar adzuna bronze`

| Option | Default | Description |
|--------|---------|-------------|
| `--preset` | from config | Extraction preset name |
| `--country` | from config | Country code override |
| `--max-pages` | from config | Maximum pages to fetch |
| `--results-per-page` | from config | Results per API page |
| `--ingestion-date` | today | Date `YYYY-MM-DD` |

### `skill-radar adzuna silver`

| Option | Default | Description |
|--------|---------|-------------|
| `--country` | from config | Country code |
| `--ingestion-date` | today | Date scope `YYYY-MM-DD` |

## Gold Layer

### `skill-radar gold pipeline`

Full Gold pipeline: matching → analytics.

| Option | Default | Description |
|--------|---------|-------------|
| `--ingestion-date` | required | Adzuna date `YYYY-MM-DD` |
| `--country` | required | Country code |
| `--esco-version` | required | ESCO version |
| `--esco-lang` | required | ESCO language |
| `--job-limit` | all | Debug: limit jobs processed |

### `skill-radar gold matching`

Matching only (job-skill + job-occupation). Same options as `gold pipeline`.

### `skill-radar gold analytics`

Analytics only (KPIs + graphs). Same options minus `--job-limit`.

## Search & Kibana

### `skill-radar search export`

| Option | Default | Description |
|--------|---------|-------------|
| `--dataset` | primary | Dataset(s) to export |
| `--ingestion-date` | required | Partition date |
| `--country` | required | Country code |
| `--es-url` | from env | Elasticsearch URL |
| `--create-index` / `--no-create-index` | on | Create index if missing |
| `--refresh` | on | Refresh index after load |
| `--alias-swap` | on | Update alias to latest index |
| `--dry-run` | off | Transform only, no indexing |

### `skill-radar search bootstrap-kibana`

| Option | Default | Description |
|--------|---------|-------------|
| `--apply` / `--write-artifacts-only` | write-only | Apply via API vs write NDJSON |
| `--force` | off | Overwrite existing data views |
| `--kibana-url` | from env | Kibana URL override |

### `skill-radar search dashboard export`

| Option | Default | Description |
|--------|---------|-------------|
| `--output-dir` | `configs/kibana` | NDJSON output directory |
| `--filename` | `skill_radar_dashboards.ndjson` | Output filename |

### `skill-radar search dashboard apply`

| Option | Default | Description |
|--------|---------|-------------|
| `--kibana-url` | from env | Kibana URL |
| `--overwrite` / `--no-overwrite` | on | Overwrite existing objects |
| `--dry-run` | off | Generate only, don't push |

## Validation

### `skill-radar validate infra`

| Option | Default | Description |
|--------|---------|-------------|
| `--upload` | off | Upload report to S3 |
| `--scope` | `host` | `host`, `runtime`, or `all` |

### Dataset Validators

| Command | Description |
|---------|-------------|
| `validate esco-landing` | ESCO landing zone |
| `validate esco-bronze` | ESCO Bronze tables |
| `validate esco-bronze-e2e` | Extract + validate end-to-end |
| `validate esco-silver` | ESCO Silver tables |
| `validate adzuna-bronze` | Adzuna Bronze tables |
| `validate adzuna-silver` | Adzuna Silver tables |
| `validate bronze` | Group validator for any Bronze |
| `validate gold` | Gold tables |
| `validate search` | ES indices + Kibana |

All accept `--upload` and `--quiet`. ESCO validators require `--version` and `--lang`.

## Stage Units (run)

Bundles processing + validation in a single Spark session. Used by Airflow DAGs.

| Command | Description |
|---------|-------------|
| `run diagnostics` | Print Spark runtime info |
| `run infra` | Provision + validate infrastructure |
| `run esco-bronze` | Landing validation + Bronze extraction + validation |
| `run esco-silver` | Silver formatting + validation |
| `run adzuna-bronze` | Bronze extraction + validation |
| `run adzuna-silver` | Silver formatting + validation |
| `run gold` | Full Gold pipeline + validation |
| `run search` | Export to ES + validation |

## Makefile Quick Reference

All targets: `make <target>`. Append `VERBOSE=1` for full command output.

### Development

| Target | Description |
|--------|-------------|
| `bootstrap` | Install deps, editable package, pre-commit, doctor |
| `fix` | Auto-fix lint and format issues |
| `lint` | Run ruff linter |
| `fmt` | Check formatting (no changes) |
| `type` | Run mypy type checking |
| `utest` | Run unit tests |
| `check` | All local checks (lint + format + types + tests) |
| `ci` | Full CI pipeline |

### Infrastructure

| Target | Description |
|--------|-------------|
| `up` | Start core infrastructure (MinIO + Spark) |
| `down` | Stop core infrastructure |
| `reset` | Stop infra + remove volumes |
| `up-all` | Start ALL services |
| `nuke` | Factory reset: stop everything, remove all volumes |
| `infra` | Start infra + validate + smoke test |
| `smoke` | Spark + Iceberg smoke test |

### Pipeline Execution

| Target | Description |
|--------|-------------|
| `run-esco-bronze` | Full ESCO Bronze: upload → extract → validate |
| `run-esco-silver` | Full ESCO Silver: format → validate |
| `run-adzuna` | Full Adzuna: Bronze → Silver → validate |
| `run-gold` | Full Gold: matching → analytics → validate |
| `run-search` | Full Search: export → Kibana → validate |
| `run-all` | End-to-end: infra → ESCO → Adzuna → Gold → Search |

### Search & Kibana

| Target | Description |
|--------|-------------|
| `search-up` | Start ES + Kibana |
| `search-down` | Stop ES + Kibana |
| `search-reset` | Stop + remove volumes |
| `export-search` | Export Gold → ES |
| `bootstrap-kibana` | Create data views + dashboards |
| `export-kibana-assets` | Generate NDJSON (no push) |
| `apply-kibana-assets` | Push dashboards to Kibana |

### Airflow

| Target | Description |
|--------|-------------|
| `airflow-up` | Start Airflow |
| `airflow-down` | Stop Airflow |
| `airflow-trigger-adzuna` | Trigger Adzuna daily DAG |
| `airflow-trigger-esco` | Trigger ESCO manual DAG |
| `airflow-smoke` | Verify DockerOperator setup |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SKILLRADAR_S3_ENDPOINT` | `http://localhost:9000` | MinIO/S3 endpoint |
| `SKILLRADAR_S3_ACCESS_KEY` | — | S3 access key |
| `SKILLRADAR_S3_SECRET_KEY` | — | S3 secret key |
| `SKILLRADAR_ES_URL` | `http://localhost:9200` | Elasticsearch URL |
| `SKILLRADAR_KIBANA_URL` | `http://localhost:5601` | Kibana URL |
| `SKILLRADAR_ADZUNA_APP_ID` | — | Adzuna API application ID |
| `SKILLRADAR_ADZUNA_APP_KEY` | — | Adzuna API key |
| `SKILLRADAR_ADZUNA_COUNTRY` | `fr` | Default country |
| `SKILLRADAR_ADZUNA_SCHEDULE` | `0 6 * * *` | Cron for daily DAG |
| `SKILLRADAR_ESCO_VERSION` | `v1.2.0` | ESCO version |
| `SKILLRADAR_ESCO_LANG` | `fr` | ESCO language |
| `SKILLRADAR_SEARCH_ENABLED` | `true` | Enable search export |

## References

- [Running the Project](../getting-started/running-the-project.md) — quick operational guide
- [Airflow Orchestration](airflow.md) — DAG execution details
- [Validation System](validation.md) — validation framework
