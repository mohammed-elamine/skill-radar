"""ESCO Manual Pipeline DAG.

Orchestrates ESCO processing after a manual artifact upload using
**coarse stage units** — each task bundles processing + validation
in a single Spark session:

    esco_bronze_unit → esco_silver_unit [→ gold_unit]

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
    Whether to run landing validation before Bronze extraction.
    Passed as ``--validate-landing`` to the bronze stage unit.

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

Design decision — coarse stage units
--------------------------------------
The previous version spawned 8 separate containers (one per step +
validation).  This version reduces it to 2-3 by grouping each
processing step with its validation inside a ``skill-radar run``
command that shares a single Spark session.
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
with optional Gold recompute.  Uses **coarse stage units** — each task
bundles processing + validation in a single Spark session.

#### Required parameters

| Param | Type | Description |
|-------|------|-------------|
| `version` | str | ESCO version (e.g. `v1.2.1`) |
| `lang` | str | Language code (e.g. `fr`) |

#### Optional parameters

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `run_gold_after` | bool | `false` | Trigger Gold recompute after Silver |
| `validate_landing_first` | bool | `true` | Run landing validation before Bronze |
| `gold_ingestion_date` | str | *(today)* | Adzuna date for Gold recompute |

#### Task flow (coarse stage units)

1. **esco_bronze_unit** — [landing validation +] Bronze extraction + Bronze validation
2. **esco_silver_unit** — Silver formatting + Silver validation
3. **gold_unit** — matching + analytics + Gold validation (if enabled)

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
    description="Manual ESCO ingestion: Bronze → Silver (coarse stage units, + optional Gold).",
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
    _GOLD_DATE = "{{ params.gold_ingestion_date if params.gold_ingestion_date else ds }}"

    # Compute --validate-landing / --no-validate-landing flag via Jinja
    _VALIDATE_LANDING_FLAG = (
        "{{ '--validate-landing' if params.validate_landing_first else '--no-validate-landing' }}"
    )

    # ── Bronze unit (landing validation + extraction + bronze validation)
    esco_bronze_unit = make_skill_radar_task(
        task_id="esco_bronze_unit",
        command=(
            f"skill-radar run esco-bronze"
            f" --version {_VERSION}"
            f" --lang {_LANG}"
            f" {_VALIDATE_LANDING_FLAG}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=3,
    )

    # ── Silver unit (formatting + silver validation) ────────────────────
    esco_silver_unit = make_skill_radar_task(
        task_id="esco_silver_unit",
        command=(
            f"skill-radar run esco-silver --version {_VERSION} --lang {_LANG} --upload --quiet"
        ),
        dag=dag,
        priority_weight=2,
    )

    # ── Gold unit (conditional — matching + analytics + validation) ─────
    gold_unit = make_skill_radar_task(
        task_id="gold_unit",
        command=(
            f"skill-radar run gold"
            f" --ingestion-date {_GOLD_DATE}"
            f" --country {ADZUNA_COUNTRY}"
            f" --esco-version {_VERSION}"
            f" --esco-lang {_LANG}"
            f" --upload --quiet"
        ),
        dag=dag,
        priority_weight=1,
    )

    # ── Dependencies ────────────────────────────────────────────────────
    esco_bronze_unit >> esco_silver_unit

    # Conditional gold: only runs when run_gold_after=true
    from airflow.operators.python import ShortCircuitOperator

    should_run_gold = ShortCircuitOperator(
        task_id="should_run_gold",
        python_callable=lambda params: bool(params.get("run_gold_after", False)),
        op_kwargs={"params": "{{ params }}"},
        dag=dag,
    )

    esco_silver_unit >> should_run_gold >> gold_unit
