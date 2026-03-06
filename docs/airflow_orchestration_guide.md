# Airflow Orchestration Guide

> **Audience:** Data engineers, reviewers, and future contributors.
> **Scope:** Control-plane architecture, DAG design, local setup, operational runbooks, and future roadmap for Airflow orchestration within Skill Radar.

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [Architecture Overview](#2-architecture-overview)
3. [Execution Model — DockerOperator](#3-execution-model--dockeroperator)
4. [Infrastructure Setup](#4-infrastructure-setup)
5. [DAG Inventory](#5-dag-inventory)
   - 5.1 [Adzuna Daily Pipeline](#51-adzuna-daily-pipeline)
   - 5.2 [ESCO Manual Pipeline](#52-esco-manual-pipeline)
6. [Shared Orchestration Layer](#6-shared-orchestration-layer)
7. [Configuration Reference](#7-configuration-reference)
8. [Operational Runbook](#8-operational-runbook)
9. [CLI Compatibility](#9-cli-compatibility)
10. [Testing Strategy](#10-testing-strategy)
11. [Observability](#11-observability)
12. [Future Roadmap](#12-future-roadmap)

---

## 1. Design Philosophy

Airflow acts as the **control-plane only**. No business logic, no Spark code, no data transformation exists inside DAG files. Every task delegates to the existing CLI commands by launching an **ephemeral Docker container** from the Spark runtime image.

### Core Principles

| Principle | Implementation |
|-----------|---------------|
| No logic in DAGs | Tasks call `uv run skill-radar <command>` inside containers |
| Single image | All tasks use the same `skillradar-spark:3.5.7-uv` image |
| CLI parity | Every Airflow task maps 1:1 to a CLI command |
| Idempotent reruns | All commands use `--ingestion-date {{ ds }}` to partition by date |
| Environment-driven config | All tuning via `SKILLRADAR_*` env vars, never hardcoded |

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Airflow (control-plane)                                │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  Scheduler   │  │  Webserver   │  │  PostgreSQL   │  │
│  │  (custom img)│  │  (custom img)│  │  (metadata)   │  │
│  └──────┬───────┘  └──────────────┘  └───────────────┘  │
│         │ DockerOperator                                │
│         ▼                                               │
│  ┌──────────────────────┐                               │
│  │ skillradar-spark:3.5 │ ← ephemeral task container    │
│  │ uv run skill-radar … │                               │
│  └──────────┬───────────┘                               │
│             │                                           │
│  ┌──────────▼───────────┐                               │
│  │   MinIO / Iceberg    │ ← lakehouse storage           │
│  └──────────────────────┘                               │
└─────────────────────────────────────────────────────────┘
```

Key points:
- The Airflow scheduler connects to the Docker daemon via `/var/run/docker.sock`
- Each task creates a short-lived container, runs one CLI command, and exits
- Containers share the `skillradar_default` Docker network for MinIO/Spark access
- Bind mounts expose project source code, configs, and data directories

---

## 3. Execution Model — DockerOperator

Every pipeline task is created through a single factory function:

```python
from _shared.docker_tasks import make_skill_radar_task

task = make_skill_radar_task(
    task_id="adzuna_bronze",
    command="uv run skill-radar adzuna bronze --preset default_fr --country fr --ingestion-date {{ ds }}",
    dag=dag,
)
```

The factory guarantees consistent:
- **Image**: `SKILLRADAR_SPARK_IMAGE` (default: `skillradar-spark:3.5.7-uv`)
- **Network**: `SKILLRADAR_DOCKER_NETWORK` (default: `skillradar_default`)
- **Environment**: Full set of credentials, paths, and runtime variables
- **Mounts**: Source code, configs, data directories, and logs
- **Lifecycle**: `auto_remove="success"`, no XCom push, no TTY

### Why DockerOperator?

| Alternative | Reason rejected |
|-------------|----------------|
| BashOperator | Requires Airflow workers to have Spark/Java/uv installed |
| KubernetesPodOperator | Adds K8s dependency for a local/dev-first project |
| PythonOperator | Would import business logic into the DAG → tight coupling |
| SparkSubmitOperator | Less flexible than CLI invocation, harder to debug |

---

## 4. Infrastructure Setup

### Docker Compose Profile

Airflow runs under the `airflow` profile, separate from the core lakehouse stack:

```bash
# Start Airflow (also starts core lakehouse if not running)
make airflow-up

# Stop Airflow only
make airflow-down

# Full reset (wipe Airflow metadata)
make airflow-reset
```

### Services

| Service | Image | Purpose |
|---------|-------|---------|
| `airflow-init` | `skillradar-airflow:2.9.3` | DB migration + admin user creation |
| `airflow-webserver` | `skillradar-airflow:2.9.3` | Web UI on port 8085 |
| `airflow-scheduler` | `skillradar-airflow:2.9.3` | DAG scheduling + task execution |
| `airflow-postgres` | `postgres:16` | Metadata database (port 5433) |

### Custom Airflow Image

Built from `docker/airflow/Dockerfile` with dependencies declared in
`docker/airflow/requirements.txt`:

```dockerfile
FROM apache/airflow:2.9.3
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

The build uses `network: host` (configured in `docker-compose.yml`) so
`pip install` can reach PyPI regardless of Docker's bridge networking
configuration.

### Network & Mounts

The scheduler container has:
- `/var/run/docker.sock` mounted (to launch task containers via Docker API)
- `group_add: ["${DOCKER_GID:-0}"]` for Docker socket permissions
- The `dags/` directory mounted at `/opt/airflow/dags`
- Access to the `skillradar_default` Docker network

### Web UI Access

- **URL**: http://localhost:8085 (override with `AIRFLOW_WEB_PORT`)
- **Credentials**: `admin` / `admin` (set during `airflow-init`)

---

## 5. DAG Inventory

### 5.1 Adzuna Daily Pipeline

**File**: `dags/adzuna_daily_pipeline.py`<br>
**DAG ID**: `adzuna_daily_pipeline`<br>
**Schedule**: Configurable via `SKILLRADAR_ADZUNA_SCHEDULE` (default: `0 6 * * *`)<br>
**Catchup**: `False`<br>

#### Task Graph

```
adzuna_bronze → validate_adzuna_bronze → adzuna_silver → validate_adzuna_silver → gold_pipeline → validate_gold
```

#### Task Details

| Task | CLI Command |
|------|-------------|
| `adzuna_bronze` | `uv run skill-radar adzuna bronze --preset {PRESET} --country {COUNTRY} --ingestion-date {{ ds }}` |
| `validate_adzuna_bronze` | `uv run skill-radar validate adzuna-bronze --country {COUNTRY} --ingestion-date {{ ds }}` |
| `adzuna_silver` | `uv run skill-radar adzuna silver --country {COUNTRY} --ingestion-date {{ ds }}` |
| `validate_adzuna_silver` | `uv run skill-radar validate adzuna-silver --country {COUNTRY} --ingestion-date {{ ds }}` |
| `gold_pipeline` | `uv run skill-radar gold pipeline --ingestion-date {{ ds }} --country {COUNTRY} --esco-version {VERSION} --esco-lang {LANG}` |
| `validate_gold` | `uv run skill-radar validate gold --ingestion-date {{ ds }} --country {COUNTRY} --esco-version {VERSION} --esco-lang {LANG}` |

#### Trigger Examples

```bash
# Via Makefile
make airflow-trigger-adzuna AIRFLOW_ADZUNA_DATE=2025-01-15

# Via Airflow CLI
airflow dags trigger adzuna_daily_pipeline --exec-date 2025-01-15
```

---

### 5.2 ESCO Manual Pipeline

**File**: `dags/esco_manual_pipeline.py`<br>
**DAG ID**: `esco_manual_pipeline`<br>
**Schedule**: `None` (manual trigger only)<br>

#### Task Graph

```
validate_esco_landing → esco_bronze → validate_esco_bronze → esco_silver → validate_esco_silver
                                                                                 ↓
                                                                          should_run_gold → gold_pipeline → validate_gold
```

#### Runtime Parameters

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `version` | string | `v1.2.1` | ESCO version identifier |
| `lang` | string | `fr` | Language code |
| `run_gold_after` | boolean | `false` | Trigger Gold recompute |
| `validate_landing_first` | boolean | `true` | Run landing validation |
| `gold_ingestion_date` | string | `""` | Adzuna date for Gold (empty = today) |

#### Why Gold is Optional

ESCO is a static taxonomy. Gold requires both ESCO Silver *and* Adzuna Silver as inputs. Running Gold without fresh Adzuna data produces stale results. Enable `run_gold_after=true` only when Adzuna data is available.

#### Trigger Examples

```bash
# Via Makefile
make airflow-trigger-esco AIRFLOW_ESCO_VERSION=v1.2.1 AIRFLOW_ESCO_LANG=fr

# Via Airflow CLI
airflow dags trigger esco_manual_pipeline \
  --conf '{"version": "v1.2.1", "lang": "fr", "run_gold_after": false}'

# With Gold recompute
airflow dags trigger esco_manual_pipeline \
  --conf '{"version": "v1.2.1", "lang": "fr", "run_gold_after": true, "gold_ingestion_date": "2025-01-15"}'
```

---

## 6. Shared Orchestration Layer

All shared orchestration code lives in `dags/_shared/`:

| Module | Purpose |
|--------|---------|
| `config.py` | Environment variable loading with typed defaults |
| `docker_tasks.py` | `make_skill_radar_task()` factory for DockerOperator |
| `defaults.py` | Common `default_args` and `dag_tags()` helper |
| `callbacks.py` | Task failure/success logging callbacks |
| `templates.py` | Jinja date macro helpers |

### Module Design Rules

- **No Airflow imports** in `config.py` — pure Python for easy testing
- **Single factory** for all tasks — guarantees consistency
- **Environment-driven** — all values from `SKILLRADAR_*` env vars
- **Declarative mount spec** — same mounts as the Spark service in docker-compose

---

## 7. Configuration Reference

All configuration is via environment variables set on the Airflow services in `docker-compose.yml`:

### Infrastructure

| Variable | Default | Description |
|----------|---------|-------------|
| `SKILLRADAR_SPARK_IMAGE` | `skillradar-spark:3.5.7-uv` | Docker image for task containers |
| `SKILLRADAR_DOCKER_NETWORK` | `skillradar_default` | Docker network for container communication |
| `SKILLRADAR_DOCKER_URL` | `unix:///var/run/docker.sock` | Docker daemon URL |
| `SKILLRADAR_HOST_PROJECT_DIR` | `$PWD` | Host project root for bind mounts |

### Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `SKILLRADAR_S3_BUCKET` | `skillradar-lake` | Primary data lake bucket |
| `SKILLRADAR_S3_LOGS_BUCKET` | `skillradar-logs` | Logs/manifest bucket |
| `SKILLRADAR_S3_ENDPOINT` | `http://minio:9000` | S3 endpoint for containers |
| `SKILLRADAR_S3_ENDPOINT_DOCKER` | `http://minio:9000` | Docker-internal S3 endpoint |

### Adzuna Pipeline

| Variable | Default | Description |
|----------|---------|-------------|
| `SKILLRADAR_ADZUNA_SCHEDULE` | `0 6 * * *` | Cron schedule for daily pipeline |
| `SKILLRADAR_ADZUNA_COUNTRY` | `fr` | Target country code |
| `SKILLRADAR_ADZUNA_PRESET` | `default_fr` | Extraction preset from contract.yaml |

### ESCO Pipeline

| Variable | Default | Description |
|----------|---------|-------------|
| `SKILLRADAR_ESCO_VERSION` | `v1.2.1` | ESCO taxonomy version |
| `SKILLRADAR_ESCO_LANG` | `fr` | Language code |
| `SKILLRADAR_ESCO_RUN_GOLD_AFTER` | `false` | Default for Gold recompute |

### Task Execution

| Variable | Default | Description |
|----------|---------|-------------|
| `SKILLRADAR_TASK_RETRIES` | `2` | Default retry count |
| `SKILLRADAR_TASK_RETRY_DELAY_SECONDS` | `120` | Seconds between retries |
| `SKILLRADAR_MAX_ACTIVE_RUNS` | `1` | Max concurrent DAG runs |

---

## 8. Operational Runbook

### Start Airflow

```bash
make airflow-up
```

This builds the custom Airflow image (if needed) and starts all services under the `airflow` profile.

### Check DAGs Are Loaded

```bash
make airflow-dags-list
```

Expected output should include `adzuna_daily_pipeline` and `esco_manual_pipeline`.

### Trigger a Manual Run

```bash
# Adzuna — specific date
make airflow-trigger-adzuna AIRFLOW_ADZUNA_DATE=2025-01-15

# ESCO — default params
make airflow-trigger-esco
```

### View Logs

```bash
make airflow-logs
```

### Debug a Failing Task

1. Check the Airflow UI at http://localhost:8085
2. Click on the failed task instance
3. View the container logs in the "Log" tab
4. The failure callback logs structured context:
   ```
   TASK FAILED | dag=adzuna_daily_pipeline task=adzuna_bronze date=2025-01-15 try=2
   ```

### Full Reset

```bash
make airflow-reset
```

This stops all Airflow services and removes the metadata database volume.

### Verify DockerOperator Readiness (Smoke Check)

Before running DAGs, verify that the scheduler can actually communicate
with the Docker daemon and launch ephemeral containers:

```bash
make airflow-smoke
```

This executes `scripts/airflow/docker_operator_smoke.py` **inside** the
`airflow-scheduler` container and validates four prerequisites:

| Check | What it verifies |
|-------|------------------|
| Docker daemon | Socket is mounted and daemon responds |
| Spark image | `SKILLRADAR_SPARK_IMAGE` is present locally |
| Docker network | `SKILLRADAR_DOCKER_NETWORK` exists |
| Container launch | A short-lived test container runs and exits cleanly |

If any check fails, the script prints a structured diagnostic message with
actionable hints (missing image → `make infra-up`, missing network →
`make airflow-up`, etc.).

> **Note:** `make airflow-smoke` is the recommended local health check for
> DockerOperator readiness. `airflow dags test` runs tasks inline in the
> scheduler process, which is useful for validating DAG structure but **is
> not a reliable test** of the DockerOperator container-launch path. Use
> `make airflow-smoke` instead.

### Test DAGs Without Running Tasks

Validate DAG integrity — import errors and task tree structure — without
launching any containers or requiring pipeline data:

```bash
make airflow-test-adzuna    # validate adzuna_daily_pipeline
make airflow-test-esco      # validate esco_manual_pipeline
```

Each target performs two checks:

1. **Import errors** — `airflow dags list-import-errors` must report none.
2. **Task tree** — `airflow tasks list <dag_id> --tree` renders the full
   dependency graph, confirming that all tasks parse correctly and wire up
   as expected.

> **Why not `airflow dags test`?**  That command runs tasks **inline** in
> the scheduler process.  For DockerOperator DAGs this means it actually
> launches containers and executes the real CLI commands — which fails
> without pipeline data and takes minutes due to retry delays.  The
> integrity check above is fast, deterministic, and validates everything
> that matters at the DAG authoring level.  For DockerOperator runtime
> validation, use `make airflow-smoke` instead.

---

## 9. CLI Compatibility

All Airflow tasks invoke existing CLI commands. The only change for Airflow support:

### `--ingestion-date` Flag

The `adzuna bronze` command now accepts an optional `--ingestion-date` parameter:

```bash
# Without flag — uses today's date (backward compatible)
uv run skill-radar adzuna bronze --preset default_fr --country fr

# With flag — uses the provided date (Airflow passes {{ ds }})
uv run skill-radar adzuna bronze --preset default_fr --country fr --ingestion-date 2025-01-15
```

This ensures the same CLI command works both interactively and under Airflow orchestration.

---

## 10. Testing Strategy

### Test File

`tests/unit/test_airflow_orchestration.py` — 34 tests covering:

| Test Class | Count | Coverage |
|------------|-------|----------|
| `TestConfig` | 7 | Config loading, env overrides, type parsing |
| `TestDefaults` | 3 | Default args, tag helper |
| `TestTemplates` | 1 | Jinja macro generation |
| `TestCallbacks` | 3 | Failure/success logging |
| `TestDockerTasks` | 4 | Factory kwargs, env merging, mounts |
| `TestDAGImports` | 4 | Clean parsing, DAG discovery |
| `TestDAGStructure` | 12 | Task IDs, chains, schedules, params, tags |

### Running Tests

```bash
# Orchestration tests only
uv run pytest tests/unit/test_airflow_orchestration.py -v

# All unit tests
uv run pytest tests/unit/ -v
```

### Test Design

- **No Airflow infrastructure required** — tests run with in-memory SQLite
- **DagBag** used for DAG loading to avoid import conflicts
- **DockerOperator mocked** via `patch.object` — verifies kwargs without container execution
- **Config tests** use `patch.dict(os.environ)` for isolation

---

## 11. Observability

### Task Callbacks

Every task has failure and success callbacks that emit structured log lines:

```
TASK FAILED | dag=adzuna_daily_pipeline task=adzuna_bronze date=2025-01-15 try=2 | log_url=http://...
TASK OK     | dag=adzuna_daily_pipeline task=adzuna_bronze date=2025-01-15
```

### Airflow Logs

Accessible via:
- Web UI task instance logs
- `make airflow-logs` for scheduler/webserver output

### Container Logs

Each task container writes to stdout/stderr, which Airflow captures. The `auto_remove="success"` setting keeps failed containers for debugging.

---

## 12. Future Roadmap

### Phase 2 — Landing Manifest Sensor

A sensor DAG that watches for new ESCO artifacts in MinIO/S3 and automatically triggers `esco_manual_pipeline`:

```
esco_landing_sensor (S3KeySensor)
    → esco_manual_pipeline (TriggerDagRunOperator)
```

This is intentionally not implemented in the current phase to avoid brittle state handling before the patterns are validated.

### Phase 3 — Multi-Country Expansion

Dynamic DAG generation from `contract.yaml` to support multiple countries:

```python
for country in contract["countries"]:
    create_adzuna_dag(country=country)
```

### Phase 4 — Alerting

Integration with Slack/email via Airflow connections for production alerting on task failures.

---

## Module Map

```
dags/
├── _shared/
│   ├── __init__.py
│   ├── config.py          ← env-var config (no Airflow imports)
│   ├── docker_tasks.py    ← DockerOperator factory
│   ├── defaults.py        ← DAG default_args / tags
│   ├── callbacks.py       ← task failure/success logging
│   └── templates.py       ← Jinja date macros
├── adzuna_daily_pipeline.py  ← Bronze → Silver → Gold (daily)
└── esco_manual_pipeline.py   ← Landing → Bronze → Silver (manual)

docker/
└── airflow/
    ├── Dockerfile            ← Custom Airflow image
    └── requirements.txt      ← Pinned Python deps for image

scripts/
└── airflow/
    └── docker_operator_smoke.py  ← Scheduler-side smoke check

tests/unit/
└── test_airflow_orchestration.py  ← 34 unit tests
```
