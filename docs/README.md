# Skill Radar Documentation

Skill Radar is a production-oriented data engineering platform that analyzes job market data to extract actionable insights about technology demand, salary trends, skill evolution, and career navigation. Built as part of a Big Data master thesis at Télécom Paris.

## Getting Started

- [Installation](getting-started/installation.md) — prerequisites, dependencies, environment setup
- [Quick Start](getting-started/quickstart.md) — 5-minute end-to-end demo
- [Running the Project](getting-started/running-the-project.md) — service endpoints, lifecycle commands, pipeline execution

## Architecture

- [Overview](architecture/overview.md) — project goals, data sources, milestones, technology stack
- [Modular Design](architecture/modular-design.md) — layered architecture, SSOT principle, design philosophy
- [System Components](architecture/system-components.md) — Spark runtime, MinIO, Iceberg configuration
- [Lakehouse Layout](architecture/lakehouse.md) — bucket structure, Iceberg namespaces, naming conventions
- [Data Model](architecture/data-model.md) — Gold layer schemas, keys, and lineage

## Pipelines

- [Pipeline Overview](pipelines/overview.md) — medallion architecture, data flow, phase summary
- [ESCO Pipeline](pipelines/esco.md) — Landing → Bronze → Silver for ESCO taxonomy
- [Adzuna Pipeline](pipelines/adzuna.md) — API extraction → Bronze → Silver for job postings
- [Gold Pipeline](pipelines/gold.md) — matching, analytics, 12 analytical datasets
- [Search Pipeline](pipelines/search.md) — Elasticsearch export, alias routing, index management
- [Career Navigation](pipelines/career-navigation.md) — occupation/skill profiles, similarity, transitions

## Platform

- [Airflow Orchestration](platform/airflow.md) — DAGs, DockerOperator, shared layer, config
- [Validation System](platform/validation.md) — check framework, runners, sinks, exit codes
- [Command Reference](platform/command-reference.md) — full CLI and Makefile reference

## Analytics

- [Emerging Skills](analytics/emerging-skills.md) — momentum, acceleration, novelty scoring
- [ML Features](analytics/ml-features.md) — KMeans skill segmentation, demand clusters
- [Occupation Market](analytics/occupation-market.md) — daily occupation market indicators

## Dashboards

- [Dashboard Overview](dashboards/overview.md) — code-managed approach, NDJSON pipeline
- [Kibana Dashboards](dashboards/kibana.md) — Market Overview, Salary Intelligence, Occ-Skill Graph
- [Career Navigation Explorer](dashboards/career-navigation-explorer.md) — occupation profiles dashboard

## Development

- [Contributing](development/contributing.md) — contribution workflow and guidelines
- [Coding Standards](development/coding-standards.md) — style, conventions, design rules
- [Testing](development/testing.md) — test strategy, coverage, running tests

## Design Philosophy

Three governing principles drive every design decision:

1. **Modularity** — each component does one thing and exposes a clean interface
2. **Delegation** — business logic stays in pure functions; I/O lives in platform agents
3. **Single Source of Truth** — every fact (table name, schema, config) is defined once

These principles are detailed in [Modular Design](architecture/modular-design.md).
