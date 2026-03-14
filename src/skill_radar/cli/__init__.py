"""Skill Radar CLI entry point."""

from __future__ import annotations

import click

from .adzuna import adzuna_group
from .esco import esco_group
from .gold import gold_group
from .infra import infra_group
from .run import run_group
from .search import search_group
from .validate import validate_group


@click.group()
def main() -> None:
    """Skill Radar — Data Platform CLI."""


main.add_command(adzuna_group, name="adzuna")
main.add_command(esco_group, name="esco")
main.add_command(gold_group, name="gold")
main.add_command(infra_group, name="infra")
main.add_command(run_group, name="run")
main.add_command(search_group, name="search")
main.add_command(validate_group, name="validate")
