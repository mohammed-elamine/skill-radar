# Skill Radar

Skill Radar is a production-oriented data engineering project that analyzes job market data to extract actionable insights about technology demand, salary trends, and skill evolution.

The system ingests fresh job postings daily, processes them through a structured pipeline, and exposes reliable KPIs for analysis and visualization.

This project is built as part of a Big Data master thesis with a strong emphasis on:

- Reproducibility
- Clean architecture
- Data reliability
- Automated validation
- CI/CD discipline
- Production-style workflow

---

# Project Overview

Skill Radar builds a complete data pipeline:

1. Ingestion
   - Fetch job postings from public APIs (e.g., Adzuna)
   - Daily incremental ingestion
   - Idempotent logic (no duplicates)

2. Processing
   - Normalize job data
   - Extract structured skill signals
   - Clean and enrich metadata

3. Storage
   - Structured storage in the data layer
   - Versioned and reproducible datasets

4. Analytics
   - Compute KPIs:
     - Skill demand trends
     - Salary distribution by skill
     - Geographic demand patterns
     - Emerging skills detection

5. Validation
   - Type-checking
   - Linting
   - Unit testing
   - CI validation

---

# Repository Structure

```

skill-radar/
│
├── src/skill_radar/       # Core package
├── tests/                 # Unit tests
├── scripts/               # Environment & tooling scripts
├── data/                  # Local data storage (ignored by git)
├── pyproject.toml         # Project configuration
├── uv.lock                # Locked dependencies
├── Makefile               # Developer workflow
└── .github/workflows/     # CI pipeline

```

---

# Quick Start

## 1. Install uv (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
````

or on macOS:

```bash
brew install uv
```

---

## 2. Bootstrap the Project

```bash
make bootstrap
```

This will:

* Install all dependencies
* Install pre-commit hooks
* Validate your environment

---

## 3. Verify Environment

At any time, run:

```bash
make doctor
```

This checks:

* Python version (3.11 required)
* Dependency installation
* Required environment variables
* Package import
* Data directory permissions

If issues are detected, you can attempt automatic repair:

```bash
make doctor-fix
```

---

# Development Workflow

During development:

```bash
make fix
```

Before opening a Pull Request:

```bash
make check
```

This validates:

* Linting
* Formatting
* Type checking
* Tests

All checks must pass before merging.

CI enforces the same validation rules automatically.

---

# Environment Variables

Create a `.env` file at the project root:

```
ADZUNA_APP_ID=your_id
ADZUNA_APP_KEY=your_key
```

Never commit secrets.

---

# Design Principles

Skill Radar follows a production-style engineering mindset:

* Deterministic builds (uv lockfile)
* Strict CI gates
* Explicit dependency management
* Clean Git history (Conventional Commits)
* Environment validation via `doctor`
* Separation of fix vs check commands

---

# Why This Project Matters

Most job analytics tools provide surface-level statistics.

Skill Radar aims to provide:

* Reliable trend detection
* Skill evolution tracking
* Actionable insights for developers and decision-makers
* Clean, reproducible engineering practices

It is not just a data analysis notebook — it is a structured, professional data pipeline.

---

# Contributing

See `CONTRIBUTING.md` for:

* Branching model
* Commit conventions
* Quality standards
* PR requirements

---

# License

MIT License
