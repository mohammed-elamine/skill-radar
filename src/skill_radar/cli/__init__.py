"""Skill Radar CLI entry point.

Usage::

    uv run python -m skill_radar.cli.esco upload --version v1.2.1 --lang fr --file /path/to/esco.zip
"""

from __future__ import annotations

import click

from .esco import esco_group


@click.group()
def main() -> None:
    """Skill Radar — Data Platform CLI."""


main.add_command(esco_group, name="esco")
