"""Adzuna Daily Pipeline DAG.

Orchestrates the full Adzuna data pipeline for a single logical date:

    bronze → validate_bronze → silver → validate_silver → gold → validate_gold

Schedule
--------
Configurable via ``SKILLRADAR_ADZUNA_SCHEDULE`` (default: ``0 6 * * *``).
``catchup=False`` by default — backfills must be triggered explicitly.

Execution model
---------------
Every task launches an **ephemeral container** from the Spark runtime image
via ``DockerOperator``.  Business logic lives in the existing CLI commands;
this DAG file is a thin orchestration wrapper.

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

Runs the full Adzuna Bronze → Silver → Gold pipeline for **one logical date**.

| Parameter | Value |
|-----------|-------|
| Country | `{ADZUNA_COUNTRY}` |
| Preset | `{ADZUNA_PRESET}` |
| ESCO version | `{ESCO_VERSION}` |
| ESCO lang | `{ESCO_LANG}` |
| Schedule | `{ADZUNA_SCHEDULE}` |

#### Task flow

1. **adzuna_bronze** — fetch job postings from Adzuna API into Iceberg Bronze
2. **validate_adzuna_bronze** — run Bronze-level validation checks
3. **adzuna_silver** — normalize/deduplicate into Silver Iceberg
4. **validate_adzuna_silver** — run Silver-level validation checks
5. **gold_pipeline** — matching + analytics against ESCO taxonomy
6. **validate_gold** — run Gold-level validation checks
"""

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="adzuna_daily_pipeline",
    description="Daily Adzuna ingestion: Bronze → Silver → Gold with validation.",
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
    tags=dag_tags("adzuna", "daily", "bronze", "silver", "gold", "validation"),
) as dag:
    # ── Bronze ──────────────────────────────────────────────────────────
    adzuna_bronze = make_skill_radar_task(
        task_id="adzuna_bronze",
        command=(
            f"skill-radar adzuna bronze"
            f" --preset {ADZUNA_PRESET}"
            f" --country {ADZUNA_COUNTRY}"
            f" --ingestion-date {_DS}"
        ),
        dag=dag,
    )

    validate_adzuna_bronze = make_skill_radar_task(
        task_id="validate_adzuna_bronze",
        command=(
            f"skill-radar validate adzuna-bronze --country {ADZUNA_COUNTRY} --ingestion-date {_DS}"
        ),
        dag=dag,
    )

    # ── Silver ──────────────────────────────────────────────────────────
    adzuna_silver = make_skill_radar_task(
        task_id="adzuna_silver",
        command=(f"skill-radar adzuna silver --country {ADZUNA_COUNTRY} --ingestion-date {_DS}"),
        dag=dag,
    )

    validate_adzuna_silver = make_skill_radar_task(
        task_id="validate_adzuna_silver",
        command=(
            f"skill-radar validate adzuna-silver --country {ADZUNA_COUNTRY} --ingestion-date {_DS}"
        ),
        dag=dag,
    )

    # ── Gold ────────────────────────────────────────────────────────────
    gold_pipeline = make_skill_radar_task(
        task_id="gold_pipeline",
        command=(
            f"skill-radar gold pipeline"
            f" --ingestion-date {_DS}"
            f" --country {ADZUNA_COUNTRY}"
            f" --esco-version {ESCO_VERSION}"
            f" --esco-lang {ESCO_LANG}"
        ),
        dag=dag,
    )

    validate_gold = make_skill_radar_task(
        task_id="validate_gold",
        command=(
            f"skill-radar validate gold"
            f" --ingestion-date {_DS}"
            f" --country {ADZUNA_COUNTRY}"
            f" --esco-version {ESCO_VERSION}"
            f" --esco-lang {ESCO_LANG}"
        ),
        dag=dag,
    )

    # ── Dependencies ────────────────────────────────────────────────────
    (
        adzuna_bronze
        >> validate_adzuna_bronze
        >> adzuna_silver
        >> validate_adzuna_silver
        >> gold_pipeline
        >> validate_gold
    )
