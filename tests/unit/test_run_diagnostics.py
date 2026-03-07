"""Tests for the ``skill-radar run diagnostics`` CLI command."""

from __future__ import annotations

from click.testing import CliRunner

from skill_radar.cli.run import run_diagnostics


class TestRunDiagnostics:
    """Verify runtime diagnostics output format."""

    def test_prints_spark_version(self):
        runner = CliRunner()
        result = runner.invoke(run_diagnostics)
        assert result.exit_code == 0
        assert "Spark version" in result.output

    def test_prints_java_version(self):
        runner = CliRunner()
        result = runner.invoke(run_diagnostics)
        assert result.exit_code == 0
        assert "Java version" in result.output

    def test_prints_architecture(self):
        runner = CliRunner()
        result = runner.invoke(run_diagnostics)
        assert result.exit_code == 0
        assert "Architecture" in result.output

    def test_prints_python_version(self):
        runner = CliRunner()
        result = runner.invoke(run_diagnostics)
        assert result.exit_code == 0
        assert "Python version" in result.output

    def test_all_four_fields_present(self):
        runner = CliRunner()
        result = runner.invoke(run_diagnostics)
        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        assert len(lines) == 4
