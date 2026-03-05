"""Skill Radar CLI entry point.

Usage::

    uv run skill-radar esco upload --version v1.2.1 --lang fr --file /path/to/esco.zip
    uv run skill-radar validate infra
    uv run skill-radar validate esco-landing --version v1.2.0 --lang fr
    uv run skill-radar infra apply
    uv run skill-radar run infra
"""

from __future__ import annotations

import click

from .esco import esco_group
from .infra import infra_group
from .run import run_group
from .validate import validate_group


@click.group()
def main() -> None:
    """Skill Radar — Data Platform CLI."""


main.add_command(esco_group, name="esco")
main.add_command(infra_group, name="infra")
main.add_command(run_group, name="run")
main.add_command(validate_group, name="validate")
