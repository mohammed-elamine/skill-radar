# Skill Radar

Skill Radar is a production-oriented data engineering platform that analyzes job market data to extract actionable insights about technology demand, salary trends, skill evolution, and career navigation.

The system ingests fresh job postings daily from the Adzuna API, cross-references them with the ESCO European skills taxonomy, processes everything through a structured lakehouse pipeline (Bronze → Silver → Gold), and serves the results via Elasticsearch + Kibana dashboards.

Built as part of a Big Data master thesis at Télécom Paris with a strong emphasis on reproducibility, clean architecture, data reliability, and production-style workflow.

---

## Architecture Overview

```
┌─────────────┐    ┌─────────────┐
│  Adzuna API │    │  ESCO ZIP   │
└──────┬──────┘    └──────┬──────┘
       │                  │
       ▼                  ▼
┌──────────────────────────────────┐
│          Bronze (raw)            │  ← Apache Spark + Iceberg
├──────────────────────────────────┤
│          Silver (clean)          │
├──────────────────────────────────┤
│          Gold (analytics)        │  ← 12 analytical datasets
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│   Elasticsearch + Kibana         │  ← dashboards, search
└──────────────────────────────────┘
```

| Component | Technology |
|-----------|-----------|
| Object Storage | MinIO (S3-compatible) |
| Table Format | Apache Iceberg |
| Compute Engine | Apache Spark 3.5 |
| Orchestration | Apache Airflow 2.9.3 (DockerOperator) |
| Search & Dashboards | Elasticsearch 8.13 + Kibana 8.13 |
| Package Manager | uv |

Data lake: `s3a://skillradar-lake/warehouse` (MinIO)

---

## Repository Structure

```
skill-radar/
├── src/skill_radar/         # Core package (business logic, CLI)
├── dags/                    # Airflow DAGs (orchestration only)
│   ├── _shared/             # Shared orchestration helpers
│   ├── adzuna_daily_pipeline.py
│   ├── esco_manual_pipeline.py
│   └── full_pipeline.py     # End-to-end DAG
├── jobs/                    # Spark jobs
├── configs/                 # Spark + Kibana configuration
├── docker/                  # Custom Docker images
│   ├── airflow/             # Airflow image (2.9.3 + DockerOperator)
│   └── spark/               # Spark image (3.5.7 + Iceberg + uv)
├── tests/                   # Unit + integration tests
├── data/incoming/esco/      # ESCO dropzone (place ZIP here)
├── docker-compose.yml       # Full stack (core + airflow + search profiles)
├── Makefile                 # Single developer interface (~80 targets)
└── pyproject.toml
```

---

## System Requirements

| Requirement | Version |
|-------------|---------|
| Python | 3.11+ |
| Docker + Docker Compose | Latest (v2) |
| uv | Latest |
| Disk space | ~4 GB (Docker images + data) |
| RAM | 8 GB recommended (Spark + ES) |

---

## Quick Start (5-minute Demo)

### Prerequisites

1. **Install uv** (Python package manager):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   # or: brew install uv
   ```

2. **Get Adzuna API credentials** — register at [developer.adzuna.com](https://developer.adzuna.com/) (free tier).

3. **Download the ESCO dataset** — download the French CSV ZIP from the [ESCO portal](https://esco.ec.europa.eu/en/use-esco/download).

### Step 1 — Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your Adzuna credentials:
```dotenv
ADZUNA_APP_ID=your_id
ADZUNA_APP_KEY=your_key
```

All other values have working defaults for local development (MinIO credentials, ports, etc.).

### Step 2 — Bootstrap the project

```bash
make bootstrap
```

This installs Python dependencies, the `skill-radar` CLI, pre-commit hooks, and validates the local environment.

### Step 3 — Place the ESCO dataset

```bash
mkdir -p data/incoming/esco
cp ~/Downloads/ESCO_dataset_*.zip data/incoming/esco/esco.zip
```

The Spark container will see this file via a bind mount at `/opt/skillradar/incoming/esco/esco.zip`.

### Step 4 — Run the full pipeline

> Make sure `skill-radar` is installed and in your `PATH` (via `make bootstrap`), then run:

```bash
make nuke         # factory reset (clean slate)
make up-all       # start ALL services (MinIO, Spark, Airflow, ES, Kibana)
make run-all      # run the entire pipeline end-to-end
```

`make run-all` executes 7 phases in sequence:

| Phase | What it does |
|-------|-------------|
| 1. Infrastructure | Provisions S3 buckets + Iceberg namespaces |
| 2. Search stack | Starts Elasticsearch + Kibana, waits for health |
| 3. ESCO pipeline | Uploads ZIP → Bronze extraction → Silver normalization |
| 4. Adzuna pipeline | API extraction → Bronze → Silver (deduplicated) |
| 5. Gold pipeline | Skill matching + 12 analytical datasets |
| 6. Health wait | Ensures ES + Kibana are healthy |
| 7. Search pipeline | Exports Gold to ES → deploys Kibana dashboards → validates |

When complete, open **http://localhost:5601** to explore the Kibana dashboards.

### One-liner (factory reset + full pipeline)

```bash
make nuke up-all run-all
```

---

## Service Endpoints

Once `make up-all` has started all services:

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | `skillradar` / `skillradar-secret` |
| Airflow Web UI | http://localhost:8085 | `admin` / `admin` |
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |

---

## Makefile — Key Commands

The Makefile is the single entry point for all operations. Run `make help` for the full list (~80 targets). See [docs/platform/command-reference.md](docs/platform/command-reference.md) for the complete CLI and Makefile reference.

### Lifecycle

| Command | Description |
|---------|-------------|
| `make up-all` | Start ALL services (core + Airflow + ES/Kibana) |
| `make nuke` | Factory reset: stop everything, remove all volumes and logs |
| `make run-all` | Full end-to-end pipeline (infra → ESCO → Adzuna → Gold → Search) |
| `make up` | Start core services only (MinIO + Spark) |
| `make down` | Stop core services |
| `make ps` | Show running containers |

### Individual Pipelines

| Command | Description |
|---------|-------------|
| `make run-esco-bronze` | ESCO: upload → bronze → validate |
| `make run-esco-silver` | ESCO: silver formatting → validate |
| `make run-adzuna` | Adzuna: bronze → silver (full pipeline) |
| `make run-gold` | Gold: matching → analytics → validate |
| `make run-search` | Search: export to ES → Kibana dashboards → validate |

### Search Stack

| Command | Description |
|---------|-------------|
| `make search-up` | Start Elasticsearch + Kibana |
| `make search-down` | Stop Elasticsearch + Kibana |
| `make search-reset` | Stop + remove ES data volumes |
| `make bootstrap-kibana` | Deploy Kibana data views + dashboards |

### Airflow

| Command | Description |
|---------|-------------|
| `make airflow-up` | Start Airflow (scheduler + webserver) |
| `make airflow-down` | Stop Airflow |
| `make airflow-dags-list` | List discovered DAGs |
| `make airflow-trigger-adzuna` | Trigger Adzuna daily DAG |
| `make airflow-trigger-esco` | Trigger ESCO manual DAG |

### Development

| Command | Description |
|---------|-------------|
| `make check` | Run all code checks (lint + format + types + unit tests) |
| `make fix` | Auto-fix lint and format issues |
| `make ci` | Full CI pipeline |
| `make doctor` | Validate local tooling + Docker infra |

---

## Pipeline Details

### ESCO Dataset (European Skills Taxonomy)

ESCO provides the reference taxonomy of skills, occupations, and their relationships. It requires a manual download from the [official portal](https://esco.ec.europa.eu/).

```bash
# Place the ZIP in the dropzone
cp ~/Downloads/ESCO_v1.2.1.zip data/incoming/esco/esco.zip

# Run the full ESCO pipeline (or let make run-all do it)
make upload-esco VERSION=v1.2.1 ESCO_LANG=fr EXTRA=--force
make run-esco-bronze VERSION=v1.2.1 ESCO_LANG=fr
make run-esco-silver VERSION=v1.2.1 ESCO_LANG=fr
```

The CLI searches for ESCO files in this order:
1. `data/incoming/esco/esco.zip`
2. `data/incoming/esco/esco_{lang}.zip`
3. `data/incoming/esco/{version}/esco.zip`
4. `data/incoming/esco/{version}/{lang}/esco.zip`

### Adzuna Job Postings (Live API)

Adzuna provides a REST API for job postings. Requires `ADZUNA_APP_ID` and `ADZUNA_APP_KEY` in `.env`.

```bash
# Run the full Adzuna pipeline
make run-adzuna ADZUNA_COUNTRY=fr

# Or run steps individually
make adzuna-bronze ADZUNA_COUNTRY=fr ADZUNA_MAX_PAGES=5
make adzuna-silver ADZUNA_COUNTRY=fr
```

**Bronze tables** (raw, append-only):
- `sr.sr_bronze.adzuna_jobs_raw` — one row per job posting
- `sr.sr_bronze.adzuna_request_log_raw` — one row per API request (lineage)

**Silver table** (deduplicated, typed):
- `sr.sr_silver.adzuna_jobs` — partitioned by `(country, ingestion_date)`

### Gold Analytics

Gold produces 12 analytical datasets by cross-referencing Adzuna job postings with the ESCO taxonomy:

| Dataset | Description |
|---------|-------------|
| `skill_demand_daily` | Skill demand frequency and trends |
| `salary_by_skill_daily` | Salary statistics per skill |
| `occupation_skill_graph` | Occupation ↔ skill relationships |
| `job_skill_matches` | Job-to-skill matching results |
| `job_occupation_matches` | Job-to-occupation matching results |
| `skill_emerging_daily` | Emerging skill detection |
| `occupation_market_daily` | Occupation market indicators |
| `skill_demand_segments_daily` | Skill demand by contract/location segments |
| `occupation_profile_daily` | Rich occupation profiles (career navigation) |
| `skill_profile_daily` | Detailed skill profiles |
| `occupation_similarity_daily` | Occupation similarity matrix |
| `occupation_transition_daily` | Career transition pathways |

```bash
make run-gold GOLD_COUNTRY=fr
```

### Search & Dashboards

All 12 Gold datasets are exported to Elasticsearch indices and served via Kibana dashboards.

```bash
make run-search SEARCH_COUNTRY=fr
```

Open Kibana at **http://localhost:5601** to explore dashboards for skill demand, salary trends, career navigation, and more.

---

## Airflow Orchestration

Airflow acts as the **control-plane only** — every task launches an ephemeral Docker container from the Spark image via `DockerOperator`. No business logic runs inside Airflow.

### DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `adzuna_daily_pipeline` | `0 6 * * *` | Adzuna Bronze → Silver → Gold → Search |
| `esco_manual_pipeline` | Manual | ESCO Upload → Bronze → Silver (+ optional Gold) |
| `full_pipeline` | Manual | End-to-end: Infra → ESCO + Adzuna (parallel) → Gold → Search |

The `full_pipeline` DAG runs ESCO and Adzuna branches in parallel, then converges at Gold:

```
infra ──┬── esco_landing → esco_bronze → esco_silver ──┬── gold ── search
        └── adzuna_bronze → adzuna_silver ─────────────┘
```

### Web UI

http://localhost:8085 — credentials: `admin` / `admin`

Trigger the full pipeline from the UI: click `full_pipeline` → Trigger DAG w/ Config → set parameters → Trigger.

---

## Lakehouse Layout

```
s3a://skillradar-lake/data/<layer>/<domain>/<source>/<entity>/...
```

| Layer | Purpose | Examples |
|-------|---------|---------|
| Bronze | Raw ingestion (full fidelity) | `adzuna_jobs_raw`, ESCO CSVs |
| Silver | Cleaned, normalized, deduplicated | `adzuna_jobs`, ESCO skills/occupations |
| Gold | Analytical datasets (cross-domain) | 12 datasets (see above) |

---

## Environment Variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ADZUNA_APP_ID` | Yes (for ingestion) | — | Adzuna API application ID |
| `ADZUNA_APP_KEY` | Yes (for ingestion) | — | Adzuna API application key |
| `MINIO_ROOT_USER` | No | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | No | `minioadmin` | MinIO secret key |
| `AWS_ACCESS_KEY_ID` | No | `minioadmin` | S3 access key (for host-side commands) |
| `AWS_SECRET_ACCESS_KEY` | No | `minioadmin` | S3 secret key (for host-side commands) |
| `AIRFLOW_WEB_PORT` | No | `8085` | Airflow UI port |
| `ES_PORT` | No | `9200` | Elasticsearch port |
| `KIBANA_PORT` | No | `5601` | Kibana port |

---

## Design Principles

- **Deterministic builds** — `uv.lock` for exact dependency pinning
- **Strict CI gates** — lint, format, types, unit tests on every change
- **Dockerized everything** — all compute runs in containers
- **Iceberg transactional storage** — ACID guarantees, time travel, schema evolution
- **Makefile as single interface** — one entry point for all operations
- **DAGs are orchestration only** — no business logic in Airflow; all processing via `skill-radar` CLI

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
