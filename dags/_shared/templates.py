"""Jinja-templated date helpers for Skill Radar DAGs.

Airflow's ``{{ ds }}`` macro already renders as ``YYYY-MM-DD``, so for
most use-cases no extra transformation is needed.  This module exists
as the designated extension point for any future date/partition
rendering logic that goes beyond what ``{{ ds }}`` provides.
"""

from __future__ import annotations


def partition_date_macro() -> str:
    """Return the Jinja template string for a YYYY-MM-DD partition date.

    This wraps the Airflow ``{{ ds }}`` macro so DAGs reference a named
    helper instead of scattering raw template strings.

    Returns
    -------
    str
        ``"{{ ds }}"`` — Airflow resolves this to the logical date in
        ``YYYY-MM-DD`` format at render time.
    """
    return "{{ ds }}"
