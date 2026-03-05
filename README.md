# Skill Radar

Skill Radar is a production-oriented data engineering project that analyzes job market data to extract actionable insights about technology demand, salary trends, and skill evolution.

The system ingests fresh job postings daily, processes them through a structured lakehouse pipeline, and exposes reliable KPIs for analysis and visualization.

This project is built as part of a Big Data master thesis with a strong emphasis on:

- Reproducibility
- Clean architecture
- Data reliability
- Automated validation
- CI/CD discipline
- Production-style workflow

---

# Architecture Overview

Skill Radar follows a **modern lakehouse architecture**:

- **Object Storage**: MinIO (S3-compatible)
- **Table Format**: Apache Iceberg
- **Compute Engine**: Apache Spark
- **Orchestration (future)**: Airflow
- **Serving (future)**: Elasticsearch + Kibana

The lake follows a structured layer model:

- `bronze` → raw ingestion
- `silver` → cleaned, normalized
- `gold` → analytical datasets

Iceberg warehouse location:
```bash
s3a://skillradar-lake/warehouse
```

---

# Repository Structure
```bash
skill-radar/
│
├── src/skill_radar/         # Core package (business logic)
├── jobs/                    # Spark jobs (batch processing)
├── configs/                 # Spark configuration
├── tests/
│   ├── unit/
│   └── integration/
├── scripts/                 # Environment & tooling scripts
├── docs/architecture/       # Architecture documentation
├── docker-compose.yml       # Local lakehouse stack
├── pyproject.toml
├── uv.lock
├── Makefile
└── .github/workflows/
```

---

# System Requirements

- Python 3.11+
- Docker + Docker Compose
- uv (dependency manager)

---

# Quick Start

## 1. Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

or

```bash
brew install uv
```

## 2. Bootstrap the Project
```bash
make bootstrap
```

This installs:
- Dependencies
- Pre-commit hooks
- Validates environment

## 3. Start the Lakehouse Stack
```bash
make infra
```

This will:
- Start MinIO
- Start Spark
- Run Iceberg smoke test

You can inspect MinIO at http://localhost:9000 with credentials from `.env`.

## 4. Validate Everything
```bash
make doctor
```

Checks:
- Local tooling
- Docker services
- Iceberg connectivity
- Spark job execution

## Infrastructure Validation

Validate infrastructure health from the host:
```bash
make validate-infra      # Host scope: MinIO connectivity, bucket existence
make validate-infra-all  # Full scope: includes Spark/Iceberg checks (runs in container)
```

**Note:** Host validation targets (`validate-infra`, `apply-infra-host`, `run-infra-host`) automatically source `.env` to load credentials. Required environment variables:

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | MinIO/S3 access key (matches `MINIO_ROOT_USER`) |
| `AWS_SECRET_ACCESS_KEY` | MinIO/S3 secret key (matches `MINIO_ROOT_PASSWORD`) |

If `.env` is missing or credentials are not set, you'll see:
```
Missing AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or source .env
```

---

# Development Workflow

During development
```bash
make quality
```

Before pushing
```bash
make ci
```

CI runs:
- Lint
- Format check
- Type-check
- Unit tests
- Integration tests

---

# ESCO Dataset Ingestion

The ESCO (European Skills, Competences, Qualifications and Occupations) dataset requires manual download due to authentication requirements on the official portal.

## Workflow

### Step 1: Download ESCO ZIP

Download the ESCO dataset manually from the [official portal](https://esco.ec.europa.eu/).

### Step 2: Place in Dropzone

Move the downloaded ZIP file to the host dropzone directory:

```bash
# Create dropzone if it doesn't exist
mkdir -p ./data/incoming/esco

# Move your downloaded file (any of these locations work):
mv ~/Downloads/ESCO_v1.2.1.zip ./data/incoming/esco/esco.zip
# Or with language suffix:
mv ~/Downloads/ESCO_v1.2.1.zip ./data/incoming/esco/esco_fr.zip
```

The container will see this file at `/opt/skillradar/incoming/esco/esco.zip`.

### Step 3: Upload to Landing Zone

```bash
make upload-esco VERSION=v1.2.1 LANG=fr
```

This runs inside the Spark container and uploads the artifact to the MinIO landing bucket.

### Step 4: Run Bronze Extraction

```bash
make bronze-esco VERSION=v1.2.1 LANG=fr
```

### Step 5: Validate

```bash
make validate-esco-bronze VERSION=v1.2.1 LANG=fr
```

## File Resolution

The CLI searches for ESCO files in this order:
1. `{dropzone}/esco/esco.zip`
2. `{dropzone}/esco/esco_{lang}.zip`
3. `{dropzone}/esco/{version}/esco.zip`
4. `{dropzone}/esco/{version}/{lang}/esco.zip`

## Alternative: Direct File Upload (Host)

If you prefer to run the upload on the host (requires AWS credentials configured):

```bash
make upload-esco-local FILE=path/to/esco.zip VERSION=v1.2.1 LANG=fr
```

---

# Lakehouse Layout

Storage contract:
```bash
data/<layer>/<domain>/<source>/<entity>/<version_or_dt>/<partitions...>/
```

Examples:
- `data/bronze/labour_market/esco/skills/version=2025-11-15/lang=fr/...`
- `data/silver/labour_market/adzuna/job_postings/dt=2026-02-27/country=gb/...`
- `data/gold/skill_radar/skill_index/version=2025-11-15/lang=fr/...`

See: [docs/architecture/lakehouse_layout.md](docs/architecture/lakehouse_layout.md)

---

# Environment Variables

Create `.env` from `.env.example` and fill in the required values (e.g., `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`).

These are only required for live ingestion (not infra setup).

---

# Design Principles

**Skill Radar** follows production-style discipline:
- Deterministic builds (`uv.lock`)
- Strict CI gates
- Dockerized infrastructure
- Iceberg transactional storage
- Clear separation of unit vs integration tests
- Makefile as single developer interface

---

# Current Milestone

- **Milestone 1** — Lakehouse Infrastructure Bootstrap
- **Milestone 2** — Bronze Ingestion (ESCO + Adzuna)

---

# License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
