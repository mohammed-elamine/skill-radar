# Quick Start

Run the entire Skill Radar platform end-to-end in 5 minutes.

## Prerequisites

Complete the [Installation](installation.md) steps first (uv, Docker, `.env`, ESCO ZIP).

## One-Liner

```bash
make nuke up-all run-all
```

This factory-resets, starts all services, and runs the full pipeline.

## Step by Step

### 1. Start all services

```bash
make up-all
```

Starts MinIO, Spark, Airflow, Elasticsearch, and Kibana.

### 2. Run the full pipeline

```bash
make run-all
```

Executes 7 phases in sequence:

| Phase | What it does |
|-------|-------------|
| 1. Infrastructure | Provisions S3 buckets + Iceberg namespaces |
| 2. Search stack | Starts Elasticsearch + Kibana, waits for health |
| 3. ESCO pipeline | Uploads ZIP → Bronze extraction → Silver normalization |
| 4. Adzuna pipeline | API extraction → Bronze → Silver (deduplicated) |
| 5. Gold pipeline | Skill matching + 12 analytical datasets |
| 6. Health wait | Ensures ES + Kibana are healthy |
| 7. Search pipeline | Exports Gold to ES → deploys Kibana dashboards → validates |

### 3. Explore dashboards

Open **http://localhost:5601** to access Kibana with 5 dashboards:

- **Market Overview** — top skills, demand trends, match sources
- **Salary Intelligence** — salary by skill, volume, trends
- **Occupation–Skill Graph** — occupation ↔ skill relationships
- **Career Navigation Explorer** — occupation profiles, salary, companies
- **Emerging Skills** — momentum, novelty, skill segments

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Airflow Web UI | http://localhost:8085 | `admin` / `admin` |
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |

## Factory Reset

```bash
make nuke
```

Stops everything, removes all volumes and logs. Safe for a clean restart.

## Next Steps

- [Running the Project](running-the-project.md) — individual pipeline commands, service management
- [Pipeline Overview](../pipelines/overview.md) — detailed pipeline architecture
