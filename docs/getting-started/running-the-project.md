# Running the Project

Detailed guide to service management, individual pipelines, and day-to-day operations.

## Service Lifecycle

### Core Infrastructure (MinIO + Spark)

```bash
make up          # Start core services
make down        # Stop core services
make reset       # Stop + remove volumes
make ps          # Show running containers
```

### All Services (includes Airflow + Elasticsearch + Kibana)

```bash
make up-all      # Start everything
make nuke        # Factory reset: stop all, remove all volumes and logs
```

### Search Stack (Elasticsearch + Kibana)

```bash
make search-up      # Start ES + Kibana
make search-down    # Stop ES + Kibana
make search-reset   # Stop + remove data volumes
make search-logs    # Tail ES/Kibana logs
```

### Airflow

```bash
make airflow-up              # Start Airflow (scheduler + webserver)
make airflow-down            # Stop Airflow
make airflow-reset           # Stop + remove volumes
make airflow-dags-list       # List discovered DAGs
make airflow-trigger-adzuna  # Trigger Adzuna daily DAG
make airflow-trigger-esco    # Trigger ESCO manual DAG
```

## Pipeline Execution

### Full Pipeline

```bash
make run-all    # Infra → ESCO → Adzuna → Gold → Search
```

### Individual Pipelines

```bash
# ESCO
make upload-esco VERSION=v1.2.1 ESCO_LANG=fr EXTRA=--force
make run-esco-bronze VERSION=v1.2.1 ESCO_LANG=fr
make run-esco-silver VERSION=v1.2.1 ESCO_LANG=fr

# Adzuna
make run-adzuna ADZUNA_COUNTRY=fr

# Gold
make run-gold GOLD_COUNTRY=fr

# Search
make run-search SEARCH_COUNTRY=fr SEARCH_INGESTION_DATE=2025-01-15
```

### Step-Level Commands

```bash
# ESCO steps
make bronze-esco VERSION=v1.2.1 ESCO_LANG=fr
make silver-esco VERSION=v1.2.1 ESCO_LANG=fr

# Adzuna steps
make adzuna-bronze ADZUNA_COUNTRY=fr ADZUNA_MAX_PAGES=5
make adzuna-silver ADZUNA_COUNTRY=fr

# Search steps
make export-search SEARCH_COUNTRY=fr SEARCH_INGESTION_DATE=2025-01-15
make bootstrap-kibana
make export-kibana-assets
make apply-kibana-assets
```

## ESCO Dataset Notes

The CLI searches for ESCO files in this order:

1. `data/incoming/esco/esco.zip`
2. `data/incoming/esco/esco_{lang}.zip`
3. `data/incoming/esco/{version}/esco.zip`
4. `data/incoming/esco/{version}/{lang}/esco.zip`

## Validation

```bash
make validate-infra          # Infrastructure health
make validate-esco-bronze    # ESCO Bronze tables
make validate-esco-silver    # ESCO Silver tables
make validate-adzuna-bronze  # Adzuna Bronze tables
make validate-adzuna-silver  # Adzuna Silver tables
make validate-gold           # Gold layer tables
make validate-search         # Elasticsearch indices + Kibana
```

## Development Commands

```bash
make check     # All local checks (lint + format + types + tests)
make fix       # Auto-fix lint and format issues
make ci        # Full CI pipeline
make doctor    # Validate local tooling + Docker infra
```

## References

- [Command Reference](../platform/command-reference.md) — complete CLI and Makefile reference
- [Airflow Orchestration](../platform/airflow.md) — DAG-based pipeline execution
- [Pipeline Overview](../pipelines/overview.md) — architecture and data flow
