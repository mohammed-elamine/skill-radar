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
