"""Adzuna Daily Pipeline DAG.

Orchestrates the full Adzuna data pipeline for a single logical date
using **coarse stage units** — each task bundles processing + validation
in a single Spark session to minimise container overhead:

    adzuna_bronze_unit → adzuna_silver_unit → gold_unit [→ search_unit]

Schedule
--------
Configurable via ``SKILLRADAR_ADZUNA_SCHEDULE`` (default: ``0 6 * * *``).
``catchup=False`` by default — backfills must be triggered explicitly.

Execution model
---------------
Every task launches an **ephemeral container** from the Spark runtime image
via ``DockerOperator`` and calls ``skill-radar run <stage-unit> --quiet``
which performs both data processing and validation within one JVM/Spark
session.

Parameters
----------
All pipeline parameters are derived from centralized configuration
(:mod:`_shared.config`) and the Airflow **logical date** (``{{ ds }}``).
No hidden defaults or hardcoded values exist in this file.
"""

from __future__ import annotations

from datetime import datetime

from _shared.callbacks import on_task_failure, on_task_success
from _shared.config import (
    ADZUNA_COUNTRY,
    ADZUNA_PRESET,
    ADZUNA_SCHEDULE,
    ESCO_LANG,
    ESCO_VERSION,
    MAX_ACTIVE_RUNS,
    SEARCH_ENABLED,
    SEARCH_ES_URL_DOCKER,
)
from _shared.defaults import COMMON_DEFAULT_ARGS, dag_tags
from _shared.docker_tasks import make_skill_radar_task
from _shared.templates import partition_date_macro
from airflow.models.dag import DAG

# ---------------------------------------------------------------------------
# DAG-level configuration
# ---------------------------------------------------------------------------

_DS = partition_date_macro()  # "{{ ds }}" — resolved at runtime

_DOC_MD = f"""\
### Adzuna Daily Pipeline

Runs the full Adzuna Bronze → Silver → Gold → Search pipeline for **one logical date**.

| Parameter | Value |
|-----------|-------|
| Country | `{ADZUNA_COUNTRY}` |
| Preset | `{ADZUNA_PRESET}` |
| ESCO version | `{ESCO_VERSION}` |
| ESCO lang | `{ESCO_LANG}` |
| Schedule | `{ADZUNA_SCHEDULE}` |
| Search export | `{SEARCH_ENABLED}` |

#### Task flow (coarse stage units)

Each task bundles processing + validation in a single Spark session:

1. **adzuna_bronze_unit** — fetch job postings → Iceberg Bronze + validate
2. **adzuna_silver_unit** — normalize / deduplicate Bronze → Silver + validate
3. **gold_unit** — matching + analytics against ESCO taxonomy + validate
4. **search_unit** — export Gold → Elasticsearch + validate (if enabled)
"""

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="adzuna_daily_pipeline",
    description="Daily Adzuna ingestion: Bronze → Silver → Gold (coarse stage units).",
    doc_md=_DOC_MD,
    schedule=ADZUNA_SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,
    default_args={
        **COMMON_DEFAULT_ARGS,
        "on_failure_callback": on_task_failure,
        "on_success_callback": on_task_success,
    },
    tags=dag_tags("adzuna", "daily", "bronze", "silver", "gold", "search", "validation"),
) as dag:
    # ── Bronze unit (extraction + validation) ───────────────────────────
    adzuna_bronze_unit = make_skill_radar_task(
        task_id="adzuna_bronze_unit",
        command=(
            f"skill-radar run adzuna-bronze"
            f" --preset {ADZUNA_PRESET}"
            f" --country {ADZUNA_COUNTRY}"
            f" --ingestion-date {_DS}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=4,
    )

    # ── Silver unit (formatting + validation) ───────────────────────────
    adzuna_silver_unit = make_skill_radar_task(
        task_id="adzuna_silver_unit",
        command=(
            f"skill-radar run adzuna-silver"
            f" --country {ADZUNA_COUNTRY}"
            f" --ingestion-date {_DS}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=3,
    )

    # ── Gold unit (matching + analytics + validation) ───────────────────
    gold_unit = make_skill_radar_task(
        task_id="gold_unit",
        command=(
            f"skill-radar run gold"
            f" --ingestion-date {_DS}"
            f" --country {ADZUNA_COUNTRY}"
            f" --esco-version {ESCO_VERSION}"
            f" --esco-lang {ESCO_LANG}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=2,
    )

    # ── Search unit (export + validation) ───────────────────────────────
    if SEARCH_ENABLED:
        search_unit = make_skill_radar_task(
            task_id="search_unit",
            command=(
                f"skill-radar run search"
                f" --ingestion-date {_DS}"
                f" --country {ADZUNA_COUNTRY}"
                f" --es-url {SEARCH_ES_URL_DOCKER}"
                f" --upload --quiet"
            ),
            dag=dag,
            priority_weight=1,
        )

    # ── Dependencies ────────────────────────────────────────────────────
    adzuna_bronze_unit >> adzuna_silver_unit >> gold_unit

    if SEARCH_ENABLED:
        gold_unit >> search_unit
