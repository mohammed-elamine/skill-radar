"""ESCO Manual Pipeline DAG.

Orchestrates ESCO processing after a manual artifact upload:

    validate_landing → bronze → validate_bronze → silver → validate_silver
    └── (optional) → gold_pipeline → validate_gold

Trigger mode
------------
**Manual only** (``schedule=None``).  Trigger via Airflow UI or CLI with
runtime parameters.

Runtime parameters
------------------
version : str (required)
    ESCO version identifier (e.g. ``v1.2.1``).
lang : str (required)
    Language code (e.g. ``fr``).
run_gold_after : bool (default from config, typically ``false``)
    Whether to trigger Gold recompute after Silver completes.
    Gold recompute requires Adzuna Silver data to exist for the
    target date.  Disabled by default for safety — enable explicitly
    when you are sure Adzuna data is available.
validate_landing_first : bool (default ``true``)
    Whether to run landing validation before Bronze.

Design decision — Gold recompute
---------------------------------
Gold recompute is **optional and disabled by default** for the manual
ESCO DAG because:

- ESCO is a static taxonomy; reprocessing it does not imply new Adzuna
  data is available.
- Gold requires both ESCO Silver *and* Adzuna Silver as inputs.
- Running Gold without fresh Adzuna data would produce stale results.

Enable ``run_gold_after=true`` only when you explicitly want to
recompute Gold against the latest available Adzuna partition.

Future enhancement
------------------
A landing-manifest sensor DAG could automatically detect new validated
ESCO artifacts in MinIO/S3 and trigger this DAG with resolved
``version``/``lang`` parameters.  See ``docs/airflow_orchestration_guide.md``
for the design sketch.  This is intentionally **not implemented** in the
current phase to avoid brittle state handling.
"""

from __future__ import annotations

from datetime import datetime

from _shared.callbacks import on_task_failure, on_task_success
from _shared.config import (
    ADZUNA_COUNTRY,
    ESCO_LANG,
    ESCO_RUN_GOLD_AFTER,
    ESCO_VERSION,
)
from _shared.defaults import COMMON_DEFAULT_ARGS, dag_tags
from _shared.docker_tasks import make_skill_radar_task
from airflow.models.dag import DAG
from airflow.models.param import Param

# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------

_DOC_MD = """\
### ESCO Manual Pipeline

Processes an ESCO taxonomy artifact through Landing → Bronze → Silver
with optional Gold recompute.

#### Required parameters

| Param | Type | Description |
|-------|------|-------------|
| `version` | str | ESCO version (e.g. `v1.2.1`) |
| `lang` | str | Language code (e.g. `fr`) |

#### Optional parameters

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `run_gold_after` | bool | `false` | Trigger Gold recompute after Silver |
| `validate_landing_first` | bool | `true` | Run landing validation |
| `gold_ingestion_date` | str | *(today)* | Adzuna date for Gold recompute |

#### Trigger example (Airflow CLI)

```bash
airflow dags trigger esco_manual_pipeline \\
  --conf '{"version": "v1.2.1", "lang": "fr", "run_gold_after": false}'
```

#### Why Gold is optional

ESCO is static; Gold requires both ESCO + Adzuna Silver.
Running Gold without fresh Adzuna data produces stale results.
Only enable `run_gold_after` when Adzuna data is available.
"""

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="esco_manual_pipeline",
    description="Manual ESCO ingestion: Landing → Bronze → Silver (+ optional Gold).",
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
    tags=dag_tags("esco", "manual", "bronze", "silver", "validation"),
    params={
        "version": Param(
            default=ESCO_VERSION,
            type="string",
            description="ESCO version identifier (e.g. v1.2.1)",
        ),
        "lang": Param(
            default=ESCO_LANG,
            type="string",
            description="Language code (e.g. fr)",
        ),
        "run_gold_after": Param(
            default=ESCO_RUN_GOLD_AFTER,
            type="boolean",
            description="Trigger Gold pipeline after Silver completes",
        ),
        "validate_landing_first": Param(
            default=True,
            type="boolean",
            description="Run landing validation before Bronze extraction",
        ),
        "gold_ingestion_date": Param(
            default="",
            type="string",
            description=(
                "Adzuna ingestion date for Gold recompute (YYYY-MM-DD). "
                "Leave empty to use today's date."
            ),
        ),
    },
    render_template_as_native_obj=True,
) as dag:
    # Jinja expressions for param access
    _VERSION = "{{ params.version }}"
    _LANG = "{{ params.lang }}"
    # For gold_ingestion_date: use param if non-empty, else fall back to ds
    _GOLD_DATE = "{{ params.gold_ingestion_date if params.gold_ingestion_date else ds }}"

    # ── Landing validation ──────────────────────────────────────────────
    validate_esco_landing = make_skill_radar_task(
        task_id="validate_esco_landing",
        command=(f"skill-radar validate esco-landing --version {_VERSION} --lang {_LANG}"),
        dag=dag,
    )

    # ── Bronze ──────────────────────────────────────────────────────────
    esco_bronze = make_skill_radar_task(
        task_id="esco_bronze",
        command=(f"skill-radar esco bronze --version {_VERSION} --lang {_LANG}"),
        dag=dag,
    )

    validate_esco_bronze = make_skill_radar_task(
        task_id="validate_esco_bronze",
        command=(f"skill-radar validate esco-bronze --version {_VERSION} --lang {_LANG}"),
        dag=dag,
    )

    # ── Silver ──────────────────────────────────────────────────────────
    esco_silver = make_skill_radar_task(
        task_id="esco_silver",
        command=(f"skill-radar esco silver --version {_VERSION} --lang {_LANG}"),
        dag=dag,
    )

    validate_esco_silver = make_skill_radar_task(
        task_id="validate_esco_silver",
        command=(f"skill-radar validate esco-silver --version {_VERSION} --lang {_LANG}"),
        dag=dag,
    )

    # ── Gold (conditional) ──────────────────────────────────────────────
    gold_pipeline = make_skill_radar_task(
        task_id="gold_pipeline",
        command=(
            f"skill-radar gold pipeline"
            f" --ingestion-date {_GOLD_DATE}"
            f" --country {ADZUNA_COUNTRY}"
            f" --esco-version {_VERSION}"
            f" --esco-lang {_LANG}"
        ),
        dag=dag,
    )

    validate_gold = make_skill_radar_task(
        task_id="validate_gold",
        command=(
            f"skill-radar validate gold"
            f" --ingestion-date {_GOLD_DATE}"
            f" --country {ADZUNA_COUNTRY}"
            f" --esco-version {_VERSION}"
            f" --esco-lang {_LANG}"
        ),
        dag=dag,
    )

    # ── Dependencies ────────────────────────────────────────────────────
    # Core flow: landing validate → bronze → validate → silver → validate
    (
        validate_esco_landing
        >> esco_bronze
        >> validate_esco_bronze
        >> esco_silver
        >> validate_esco_silver
    )

    # Conditional gold: only runs when run_gold_after=true
    # Note: Airflow evaluates trigger rules. We use the trigger_rule
    # approach with a BranchPythonOperator-like pattern. For simplicity
    # and readability, we use the ShortCircuitOperator approach.
    from airflow.operators.python import ShortCircuitOperator

    should_run_gold = ShortCircuitOperator(
        task_id="should_run_gold",
        python_callable=lambda params: bool(params.get("run_gold_after", False)),
        op_kwargs={"params": "{{ params }}"},
        dag=dag,
    )

    validate_esco_silver >> should_run_gold >> gold_pipeline >> validate_gold
