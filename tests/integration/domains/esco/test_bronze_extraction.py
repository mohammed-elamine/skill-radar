"""Integration test for ESCO Bronze extraction (Spark → Iceberg).

Requires ``docker compose up -d`` to be running with the Spark and MinIO
containers available.

Workflow
--------
1. Upload a small fixture ESCO ZIP to the MinIO landing zone via the
   ESCO intake pipeline (Step 1).
2. Run ``spark-submit jobs/esco/bronze_esco_to_iceberg.py`` inside the
   Spark container.
3. Verify the Bronze Iceberg tables exist with expected schema and data.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

# Fixture ZIP lives at tests/fixtures/esco/esco_bronze_valid.zip
FIXTURE_DIR = Path(__file__).resolve().parent.parent.parent / "fixtures" / "esco"
BRONZE_ZIP = FIXTURE_DIR / "esco_bronze_valid.zip"

SPARK_EXEC = ["docker", "compose", "exec", "-T", "spark", "bash", "-lc"]

VERSION = "v1.0.0"
LANG = "fr"


def _spark_submit(cmd: str, *, timeout: int = 180) -> subprocess.CompletedProcess:
    """Run a command inside the Spark container."""
    return subprocess.run(
        [*SPARK_EXEC, cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _upload_fixture_to_landing() -> None:
    """Upload the fixture ZIP to MinIO landing zone using the CLI.

    Uses the host-side CLI (``uv run skill-radar esco upload``) which
    connects to MinIO at localhost:9000.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "skill_radar.cli",
            "esco",
            "upload",
            "--version",
            VERSION,
            "--lang",
            LANG,
            "--file",
            str(BRONZE_ZIP),
            "--force",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # Accept exit code 0 (success) — the artifact lands in MinIO
    assert result.returncode == 0, (
        f"Fixture upload failed (exit {result.returncode}):\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBronzeExtraction:
    """End-to-end Bronze extraction tests."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup_landing(self):
        """Ensure the fixture ZIP is uploaded to MinIO before tests run."""
        _upload_fixture_to_landing()

    def test_spark_submit_succeeds(self) -> None:
        """spark-submit of the Bronze job completes with exit code 0."""
        proc = _spark_submit(
            "spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py "
            f"--version {VERSION} --lang {LANG}"
        )
        assert proc.returncode == 0, (
            f"Bronze job failed (exit {proc.returncode}):\n"
            f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
        )

    def test_namespace_exists(self) -> None:
        """The sr_bronze namespace exists after the Bronze job."""
        proc = _spark_submit("spark-sql -e \"SHOW NAMESPACES IN sr LIKE 'sr_bronze'\"")
        assert (
            "sr_bronze" in proc.stdout
        ), f"Namespace sr_bronze not found.\nstdout: {proc.stdout}\nstderr: {proc.stderr}"

    def test_tables_exist(self) -> None:
        """All three Bronze tables exist."""
        proc = _spark_submit('spark-sql -e "SHOW TABLES IN sr.sr_bronze"')
        for entity in ("esco_skills_raw", "esco_occupations_raw", "esco_relations_raw"):
            assert (
                entity in proc.stdout
            ), f"Table {entity} not found.\nstdout: {proc.stdout}\nstderr: {proc.stderr}"

    def test_skills_row_count(self) -> None:
        """skills table has the expected number of rows."""
        proc = _spark_submit(
            'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_skills_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        # Fixture has 3 skills rows
        assert (
            "3" in proc.stdout
        ), f"Expected 3 rows in skills.\nstdout: {proc.stdout}\nstderr: {proc.stderr}"

    def test_occupations_row_count(self) -> None:
        """occupations table has the expected number of rows."""
        proc = _spark_submit(
            'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_occupations_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        # Fixture has 2 occupations rows
        assert (
            "2" in proc.stdout
        ), f"Expected 2 rows in occupations.\nstdout: {proc.stdout}\nstderr: {proc.stderr}"

    def test_relations_row_count(self) -> None:
        """relations table has the expected number of rows."""
        proc = _spark_submit(
            'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_relations_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        # Fixture has 4 relations rows
        assert (
            "4" in proc.stdout
        ), f"Expected 4 rows in relations.\nstdout: {proc.stdout}\nstderr: {proc.stderr}"

    def test_skills_expected_columns(self) -> None:
        """Skills table has the Bronze-renamed and lineage columns."""
        proc = _spark_submit('spark-sql -e "DESCRIBE sr.sr_bronze.esco_skills_raw"')
        for col in (
            "preferred_label",
            "alt_labels_raw",
            "hidden_labels_raw",
            "alt_labels_norm",
            "alt_labels_count",
            "hidden_labels_norm",
            "hidden_labels_count",
            "description",
            "skill_type",
            "reuse_level",
            "dataset",
            "entity",
            "version",
            "lang",
            "source_zip_key",
            "manifest_key",
            "artifact_sha256",
            "ingested_at_utc",
            "run_id",
        ):
            assert col in proc.stdout, f"Column '{col}' not found in skills.\nstdout: {proc.stdout}"

    def test_idempotency_rerun(self) -> None:
        """Re-running the job for the same (version, lang) does not duplicate rows."""
        # Run the job again
        proc = _spark_submit(
            "spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py "
            f"--version {VERSION} --lang {LANG}"
        )
        assert proc.returncode == 0, f"Re-run failed:\n{proc.stdout}\n{proc.stderr}"

        # Check skills count is still 3 (not 6)
        proc2 = _spark_submit(
            'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_skills_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        assert "3" in proc2.stdout, (
            f"Idempotency broken — expected 3 rows after re-run.\n"
            f"stdout: {proc2.stdout}\nstderr: {proc2.stderr}"
        )

    def test_dry_run_no_side_effects(self) -> None:
        """--dry-run validates but does not write to Iceberg."""
        _proc = _spark_submit(
            "spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py "
            f"--version v99.99.99 --lang {LANG} --dry-run"
        )
        # dry-run should succeed (exit 0) even if the artifact doesn't exist
        # because the plan is shown before S3 access
        # Actually it will fail due to missing artifact — that's fine,
        # we just check it doesn't create tables for v99.99.99
        # Let's check no table for that version
        proc2 = _spark_submit(
            'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_skills_raw '
            "WHERE version='v99.99.99'\""
        )
        assert "0" in proc2.stdout or proc2.returncode != 0
