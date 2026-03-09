"""Full End-to-End Pipeline DAG.

Orchestrates the **entire** Skill Radar platform pipeline in one run:

    infra_unit ──┬── esco_landing_unit → esco_bronze_unit → esco_silver_unit ──┬── gold_unit [→ search_unit]
                 └── adzuna_bronze_unit → adzuna_silver_unit ─────────────────┘

Gold waits for **both** ESCO Silver and Adzuna Silver to succeed before
starting, enabling maximum parallelism during the ingestion phase.

Trigger mode
------------
**Manual only** (``schedule=None``).  Trigger via Airflow UI, CLI, or
the Makefile target ``make airflow-trigger-full``:

.. code-block:: bash

    # Airflow CLI
    airflow dags trigger full_pipeline \\
      --conf '{"country": "fr", "ingestion_date": "2026-03-08"}'

    # cURL (Airflow REST API)
    curl -X POST http://localhost:8080/api/v1/dags/full_pipeline/dagRuns \\
      -H 'Content-Type: application/json' \\
      -d '{"conf": {"country": "fr"}}'

Execution model
---------------
Every task launches an **ephemeral container** from the Spark runtime
image via ``DockerOperator`` and calls ``skill-radar run <stage-unit>``
which performs both data processing and validation within one Spark
session.

Parameters
----------
All pipeline parameters have sensible defaults from centralised config.
Override at trigger time for ad-hoc runs.
"""

from __future__ import annotations

from datetime import datetime

from _shared.callbacks import on_task_failure, on_task_success
from _shared.config import (
    ADZUNA_COUNTRY,
    ADZUNA_PRESET,
    ESCO_LANG,
    ESCO_VERSION,
    SEARCH_ENABLED,
    SEARCH_ES_URL_DOCKER,
    SEARCH_KIBANA_URL_DOCKER,
)
from _shared.defaults import COMMON_DEFAULT_ARGS, dag_tags
from _shared.docker_tasks import make_skill_radar_task
from airflow.models.dag import DAG
from airflow.models.param import Param

# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------

_DOC_MD = """\
### Full End-to-End Pipeline

Runs the **complete** Skill Radar pipeline from infrastructure through
Kibana dashboards in a single DAG run.

#### Task flow

```
infra_unit ──┬── esco_landing_unit → esco_bronze_unit → esco_silver_unit ──┬── gold_unit ── search_unit
             └── adzuna_bronze_unit → adzuna_silver_unit ─────────────────┘
```

Gold waits for both ESCO Silver and Adzuna Silver branches to succeed.

#### Parameters

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `country` | str | `fr` | Adzuna / Gold country code |
| `ingestion_date` | str | *(logical date)* | Pipeline date (YYYY-MM-DD) |
| `esco_version` | str | `v1.2.1` | ESCO taxonomy version |
| `esco_lang` | str | `fr` | ESCO language code |
| `adzuna_preset` | str | `default_fr` | Adzuna extraction preset |
| `run_search` | bool | `true` | Export to ES + deploy Kibana dashboards |
| `job_limit` | int | `0` | Limit Gold job count (0 = no limit) |

#### Trigger examples

**Airflow CLI:**
```bash
airflow dags trigger full_pipeline \\
  --conf '{"country": "fr", "ingestion_date": "2026-03-08"}'
```

**Airflow Web UI:**
Trigger DAG → fill in parameters → Trigger.
"""

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="full_pipeline",
    description="Full end-to-end pipeline: Infra → ESCO + Adzuna → Gold → Search.",
    doc_md=_DOC_MD,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        **COMMON_DEFAULT_ARGS,
        "on_failure_callback": on_task_failure,
        "on_success_callback": on_task_success,
    },
    tags=dag_tags(
        "full",
        "e2e",
        "esco",
        "adzuna",
        "gold",
        "search",
        "manual",
    ),
    params={
        "country": Param(
            default=ADZUNA_COUNTRY,
            type="string",
            description="Country code for Adzuna / Gold (e.g. fr, gb)",
        ),
        "ingestion_date": Param(
            default="",
            type="string",
            description=("Pipeline date (YYYY-MM-DD). Leave empty to use the logical date."),
        ),
        "esco_version": Param(
            default=ESCO_VERSION,
            type="string",
            description="ESCO taxonomy version (e.g. v1.2.1)",
        ),
        "esco_lang": Param(
            default=ESCO_LANG,
            type="string",
            description="ESCO language code (e.g. fr)",
        ),
        "adzuna_preset": Param(
            default=ADZUNA_PRESET,
            type="string",
            description="Adzuna extraction preset (e.g. default_fr)",
        ),
        "run_search": Param(
            default=SEARCH_ENABLED,
            type="boolean",
            description="Export to Elasticsearch + deploy Kibana dashboards",
        ),
        "job_limit": Param(
            default=0,
            type="integer",
            description="Limit Gold job count (0 = no limit, useful for debugging)",
        ),
    },
    render_template_as_native_obj=True,
) as dag:
    # Jinja param expressions (resolved at runtime)
    _COUNTRY = "{{ params.country }}"
    _DATE = "{{ params.ingestion_date if params.ingestion_date else ds }}"
    _ESCO_VERSION = "{{ params.esco_version }}"
    _ESCO_LANG = "{{ params.esco_lang }}"
    _PRESET = "{{ params.adzuna_preset }}"
    _JOB_LIMIT_FLAG = "{{ '--job-limit ' ~ params.job_limit if params.job_limit else '' }}"

    # ── 1. Infrastructure provisioning ──────────────────────────────────
    infra_unit = make_skill_radar_task(
        task_id="infra_unit",
        command="skill-radar run infra --upload --quiet",
        dag=dag,
        priority_weight=6,
    )

    # ── 2a. ESCO Landing validation ─────────────────────────────────────
    esco_landing_unit = make_skill_radar_task(
        task_id="esco_landing_unit",
        command=(
            f"skill-radar validate esco-landing"
            f" --version {_ESCO_VERSION}"
            f" --lang {_ESCO_LANG}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=7,
    )

    # ── 2b. ESCO Bronze extraction ─────────────────────────────────────
    esco_bronze_unit = make_skill_radar_task(
        task_id="esco_bronze_unit",
        command=(
            f"skill-radar run esco-bronze"
            f" --version {_ESCO_VERSION}"
            f" --lang {_ESCO_LANG}"
            f" --no-validate-landing"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=6,
    )

    # ── 2c. ESCO Silver formatting ─────────────────────────────────────
    esco_silver_unit = make_skill_radar_task(
        task_id="esco_silver_unit",
        command=(
            f"skill-radar run esco-silver"
            f" --version {_ESCO_VERSION}"
            f" --lang {_ESCO_LANG}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=5,
    )

    # ── 3a. Adzuna Bronze ──────────────────────────────────────────────
    adzuna_bronze_unit = make_skill_radar_task(
        task_id="adzuna_bronze_unit",
        command=(
            f"skill-radar run adzuna-bronze"
            f" --preset {_PRESET}"
            f" --country {_COUNTRY}"
            f" --ingestion-date {_DATE}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=5,
    )

    # ── 3b. Adzuna Silver ──────────────────────────────────────────────
    adzuna_silver_unit = make_skill_radar_task(
        task_id="adzuna_silver_unit",
        command=(
            f"skill-radar run adzuna-silver"
            f" --country {_COUNTRY}"
            f" --ingestion-date {_DATE}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=4,
    )

    # ── 4. Gold (matching + analytics + validation) ────────────────────
    gold_unit = make_skill_radar_task(
        task_id="gold_unit",
        command=(
            f"skill-radar run gold"
            f" --ingestion-date {_DATE}"
            f" --country {_COUNTRY}"
            f" --esco-version {_ESCO_VERSION}"
            f" --esco-lang {_ESCO_LANG}"
            f" {_JOB_LIMIT_FLAG}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=3,
    )

    # ── 5. Search (export + Kibana dashboards + validation) ────────────
    from airflow.operators.python import ShortCircuitOperator

    should_run_search = ShortCircuitOperator(
        task_id="should_run_search",
        python_callable=lambda params: bool(params.get("run_search", True)),
        op_kwargs={"params": "{{ params }}"},
        dag=dag,
    )

    search_unit = make_skill_radar_task(
        task_id="search_unit",
        command=(
            f"skill-radar run search"
            f" --ingestion-date {_DATE}"
            f" --country {_COUNTRY}"
            f" --es-url {SEARCH_ES_URL_DOCKER}"
            f" --kibana-url {SEARCH_KIBANA_URL_DOCKER}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=1,
    )

    # ── Dependencies ────────────────────────────────────────────────────
    # After infra, ESCO and Adzuna branches run in parallel.
    # Gold waits for both branches to complete.
    infra_unit >> [esco_landing_unit, adzuna_bronze_unit]
    esco_landing_unit >> esco_bronze_unit >> esco_silver_unit
    adzuna_bronze_unit >> adzuna_silver_unit

    [esco_silver_unit, adzuna_silver_unit] >> gold_unit

    # Search is gated by the run_search parameter.
    gold_unit >> should_run_search >> search_unit
