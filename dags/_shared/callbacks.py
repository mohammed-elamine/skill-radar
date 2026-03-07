"""Lightweight Airflow callbacks for Skill Radar.

Provides structured logging on task failure/success without external
alerting integrations.  Callbacks are intentionally minimal — the
primary audit artifacts remain the validation reports and container
logs produced by the CLI commands themselves.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("skill_radar.airflow.callbacks")


def on_task_failure(context: dict[str, Any]) -> None:
    """Log structured information when a task fails.

    Emits the DAG id, task id, logical date, and a direct link
    to the Airflow task log so operators can triage quickly.
    """
    ti = context.get("task_instance")
    if ti is None:
        logger.error("Task failure callback invoked without task_instance context.")
        return

    dag_id = ti.dag_id
    task_id = ti.task_id
    logical_date = context.get("logical_date", "unknown")
    try_number = ti.try_number
    log_url = ti.log_url

    logger.error(
        "TASK FAILED | dag=%s | task=%s | date=%s | try=%s | log=%s",
        dag_id,
        task_id,
        logical_date,
        try_number,
        log_url,
    )


def on_task_success(context: dict[str, Any]) -> None:
    """Log a brief success record for observability."""
    ti = context.get("task_instance")
    if ti is None:
        return

    logger.info(
        "TASK OK | dag=%s | task=%s | date=%s",
        ti.dag_id,
        ti.task_id,
        context.get("logical_date", "unknown"),
    )
