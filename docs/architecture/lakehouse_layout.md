# Lakehouse Layout and Naming Conventions

This document defines the **storage layout contract** for Skill Radar.
The goal is to ensure consistency across ingestion jobs, transformations, and future cloud deployment.

---

## Buckets

Local MinIO buckets (mirrors production S3 buckets):

- `skillradar-lake`
  Stores the data lake content and the Iceberg warehouse.

- `skillradar-logs`
  Reserved for logs and pipeline run artifacts (optional; may be used later by Airflow/log shippers).

---

## Iceberg warehouse

Iceberg warehouse path (in S3A):

- `s3a://skillradar-lake/warehouse`

Iceberg table metadata + data files live under this prefix.

---

## Lake path convention

For raw files and non-Iceberg assets, we use:

`data/<layer>/<domain>/<source>/<entity>/<version_or_dt>/<partitions...>/`

Where:
- `<layer>`: `bronze` | `silver` | `gold`
- `<domain>`: business domain (e.g., `labor_market`, `taxonomy`)
- `<source>`: data source (e.g., `adzuna`, `esco`)
- `<entity>`: dataset name (e.g., `job_postings`, `skills`, `occupations`)
- `<version_or_dt>`:
  - `version=<ESCO_VERSION>` for ESCO (versioned static reference data)
  - `dt=YYYY-MM-DD` for Adzuna (daily snapshots)
- `<partitions...>`: optional partitions (`country=fr`, `lang=en`, etc.)

Examples:
- `data/bronze/taxonomy/esco/skills/version=2025.XX/lang=fr/skills.csv`
- `data/bronze/labor_market/adzuna/job_ads/dt=2026-03-02/country=fr/part-0000.json`
- `data/gold/skill_radar/kibana_skill_trends` (daily aggregates)

---

## Iceberg namespaces and tables

We standardize Iceberg namespaces to match layers:

- `sr.bronze`
- `sr.silver`
- `sr.gold`

Table naming principles:
- Use nouns, not verbs: `job_postings`, `skill_labels`
- Prefer stable names; versioning handled via columns/partitions and snapshots
- Store lineage/provenance columns (e.g., `source`, `dt`, `version`)

---

## Source-specific guidance

### Adzuna (facts, daily)
- Primary partition: `dt`
- Secondary partition: `country=fr` (if useful)
- Bronze stores raw responses + request metadata.
- Silver stores cleaned schema-conformant records.
- Gold stores aggregates/matches for dashboards.

### ESCO (dimensions/taxonomy, versioned)
- Primary partition: `version=<ESCO_VERSION>`
- Language: `lang=fr` and `lang=en` are ingested.
- Bronze stores the downloaded ZIP and extracted CSVs.
- Silver normalizes labels:
  - explode `altLabels` / `hiddenLabels`
  - track label type (`preferred|alt|hidden`)
- Gold provides searchable index tables for matching.

---

## Why this structure?
- **Auditability**: you can replay from Bronze
- **Reproducibility**: version/dt are explicit
- **Migration-ready**: structure maps directly to S3 in the cloud
