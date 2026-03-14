# Installation

How to set up the Skill Radar development environment from scratch.

## Prerequisites

| Requirement | Version | Purpose |
|-------------|---------|---------|
| Python | 3.11+ | Runtime |
| Docker + Docker Compose | Latest (v2) | Containerized services |
| uv | Latest | Python package management |
| Disk space | ~4 GB | Docker images + data |
| RAM | 8 GB recommended | Spark + Elasticsearch |

## Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or: brew install uv
```

## Get API Credentials

Register at [developer.adzuna.com](https://developer.adzuna.com/) (free tier) to obtain `ADZUNA_APP_ID` and `ADZUNA_APP_KEY`.

## Download ESCO Dataset

Download the French CSV ZIP from the [ESCO portal](https://esco.ec.europa.eu/en/use-esco/download). Place it in the dropzone:

```bash
mkdir -p data/incoming/esco
cp ~/Downloads/ESCO_dataset_*.zip data/incoming/esco/esco.zip
```

The Spark container sees this file via bind mount at `/opt/skillradar/incoming/esco/esco.zip`.

## Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your Adzuna credentials:

```dotenv
ADZUNA_APP_ID=your_id
ADZUNA_APP_KEY=your_key
```

All other values have working defaults for local development (MinIO credentials, ports, etc.).

### Key Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ADZUNA_APP_ID` | Yes (for ingestion) | — | Adzuna API application ID |
| `ADZUNA_APP_KEY` | Yes (for ingestion) | — | Adzuna API application key |
| `MINIO_ROOT_USER` | No | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | No | `minioadmin` | MinIO secret key |
| `AWS_ACCESS_KEY_ID` | No | `minioadmin` | S3 access key (host-side commands) |
| `AWS_SECRET_ACCESS_KEY` | No | `minioadmin` | S3 secret key (host-side commands) |
| `AIRFLOW_WEB_PORT` | No | `8085` | Airflow UI port |
| `ES_PORT` | No | `9200` | Elasticsearch port |
| `KIBANA_PORT` | No | `5601` | Kibana port |

## Bootstrap

```bash
make bootstrap
```

This installs Python dependencies, the `skill-radar` CLI (editable), pre-commit hooks, and validates the local environment via `make doctor`.

## Verify Installation

```bash
skill-radar --help        # CLI is available
make doctor               # Docker, uv, Python all OK
```

## Next Steps

- [Quick Start](quickstart.md) — run the full pipeline in 5 minutes
- [Running the Project](running-the-project.md) — detailed service and pipeline operations
