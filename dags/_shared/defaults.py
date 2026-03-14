"""Common DAG ``default_args`` and tagging helpers for Skill Radar.

Provides a single source of truth so individual DAG files do not
repeat boilerplate configuration.
"""

from __future__ import annotations

from datetime import timedelta

from _shared.config import TASK_EXECUTION_TIMEOUT_SECONDS, TASK_RETRIES, TASK_RETRY_DELAY_SECONDS

COMMON_DEFAULT_ARGS: dict = {
    "owner": "skill-radar",
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(seconds=TASK_RETRY_DELAY_SECONDS),
    "execution_timeout": timedelta(seconds=TASK_EXECUTION_TIMEOUT_SECONDS),
    "depends_on_past": False,
}

BASE_TAGS: list[str] = ["skill-radar"]


def dag_tags(*extra: str) -> list[str]:
    """Return a merged tag list with the base project tag prepended.

    Examples
    --------
    >>> dag_tags("adzuna", "daily")
    ['skill-radar', 'adzuna', 'daily']
    """
    return BASE_TAGS + list(extra)
