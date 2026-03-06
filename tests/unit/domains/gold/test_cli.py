"""Unit tests for Gold CLI command registration."""

from __future__ import annotations

from click.testing import CliRunner

from skill_radar.cli import main
from skill_radar.cli.gold import gold_group


class TestGoldCLIRegistered:
    """Gold CLI commands are properly wired."""

    def test_gold_group_registered_in_main(self) -> None:
        """'gold' command should be registered in main CLI group."""
        command_names = list(main.commands)
        assert "gold" in command_names

    def test_gold_subcommands_exist(self) -> None:
        """gold group has matching, analytics, pipeline subcommands."""
        command_names = list(gold_group.commands.keys())
        assert "matching" in command_names
        assert "analytics" in command_names
        assert "pipeline" in command_names

    def test_gold_help_shows(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["gold", "--help"])
        assert result.exit_code == 0
        assert "matching" in result.output
        assert "analytics" in result.output
        assert "pipeline" in result.output

    def test_validate_gold_registered(self) -> None:
        """'validate gold' command must be registered."""
        from skill_radar.cli.validate import validate_group

        command_names = list(validate_group.commands.keys())
        assert "gold" in command_names

    def test_run_gold_registered(self) -> None:
        """'run gold' command must be registered."""
        from skill_radar.cli.run import run_group

        command_names = list(run_group.commands.keys())
        assert "gold" in command_names
