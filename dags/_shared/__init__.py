"""Shared Airflow orchestration helpers for Skill Radar.

This package centralizes all reusable orchestration code so that
individual DAG files remain thin declarative wrappers.

Modules
-------
config
    Centralized orchestration configuration loaded from environment variables.
defaults
    Common DAG ``default_args`` and tag helpers.
docker_tasks
    ``DockerOperator`` factory — single source of truth for task execution.
callbacks
    Lightweight failure/success callback helpers.
templates
    Jinja-templated date rendering utilities.
"""
