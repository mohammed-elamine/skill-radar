"""Integration tests for ESCO Bronze extraction (Spark + Iceberg).

Requires Docker infrastructure to be running (MinIO + Spark containers).
Run with: pytest -m integration
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

# Project root for subprocess cwd
PROJECT_ROOT = Path(__file__).resolve().parents[4]
FIXTURE_ZIP = PROJECT_ROOT / "tests" / "fixtures" / "esco" / "esco_bronze_valid.zip"

# Test version to avoid collisions with production data
VERSION = "v0.0.1"
LANG = "fr"

# Mark all tests as integration
pytestmark = pytest.mark.integration


def spark_available() -> bool:
    """Check if Spark container is available."""
    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "spark", "echo", "ok"],
            capture_output=True,
            timeout=10,
            cwd=PROJECT_ROOT,
        )
        return result.returncode == 0
    except Exception:
        return False


def minio_available() -> bool:
    """Check if MinIO is reachable."""
    try:
        import boto3
        from botocore.exceptions import EndpointConnectionError

        client = boto3.client("s3", endpoint_url="http://localhost:9000")
        client.list_buckets()
        return True
    except (EndpointConnectionError, Exception):
        return False


def spark_exec(cmd: str, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    """Run a command inside the Spark container."""
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "spark", "bash", "-lc", cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
    )


def upload_fixture() -> bool:
    """Upload fixture ZIP to MinIO landing zone."""
    if not FIXTURE_ZIP.exists():
        return False

    result = subprocess.run(
        [
            "uv",
            "run",
            "skill-radar",
            "esco",
            "upload",
            "--version",
            VERSION,
            "--lang",
            LANG,
            "--file",
            str(FIXTURE_ZIP),
            "--force",
        ],
        capture_output=True,
        timeout=60,
        cwd=PROJECT_ROOT,
    )
    return result.returncode == 0


@pytest.mark.skipif(not spark_available(), reason="Spark container not available")
@pytest.mark.skipif(not minio_available(), reason="MinIO not available")
class TestBronzeExtraction:
    """Integration tests for Bronze extraction pipeline."""

    @pytest.fixture(autouse=True, scope="class")
    def setup_landing(self) -> None:
        """Upload fixture to landing zone before tests."""
        if not upload_fixture():
            pytest.skip("Failed to upload fixture to landing zone")

    def test_bronze_job_succeeds(self) -> None:
        """spark-submit of the Bronze job completes successfully."""
        proc = spark_exec(
            f"spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py "
            f"--version {VERSION} --lang {LANG}"
        )
        assert proc.returncode == 0, f"Bronze job failed:\n{proc.stderr}"

    def test_namespace_exists(self) -> None:
        """The sr_bronze namespace exists after the Bronze job."""
        proc = spark_exec("spark-sql -e \"SHOW NAMESPACES IN sr LIKE 'sr_bronze'\"")
        assert "sr_bronze" in proc.stdout, f"Namespace not found:\n{proc.stdout}"

    def test_tables_exist(self) -> None:
        """Bronze tables exist in the namespace."""
        proc = spark_exec('spark-sql -e "SHOW TABLES IN sr.sr_bronze"')
        for entity in ("esco_skills_raw", "esco_occupations_raw", "esco_relations_raw"):
            assert entity in proc.stdout, f"Table {entity} not found"

    def test_skills_row_count(self) -> None:
        """Skills table has expected row count."""
        proc = spark_exec(
            f'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_skills_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        # Fixture has 3 skills rows
        assert "3" in proc.stdout, f"Expected 3 skills rows:\n{proc.stdout}"

    def test_occupations_row_count(self) -> None:
        """Occupations table has expected row count."""
        proc = spark_exec(
            f'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_occupations_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        # Fixture has 2 occupations rows
        assert "2" in proc.stdout, f"Expected 2 occupations rows:\n{proc.stdout}"

    def test_relations_row_count(self) -> None:
        """Relations table has expected row count."""
        proc = spark_exec(
            f'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_relations_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        # Fixture has 4 relations rows
        assert "4" in proc.stdout, f"Expected 4 relations rows:\n{proc.stdout}"

    def test_idempotency(self) -> None:
        """Re-running the job does not duplicate rows."""
        # Run job again
        proc = spark_exec(
            f"spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py "
            f"--version {VERSION} --lang {LANG}"
        )
        assert proc.returncode == 0, f"Re-run failed:\n{proc.stderr}"

        # Check skills count is still 3
        proc2 = spark_exec(
            f'spark-sql -e "SELECT count(*) FROM sr.sr_bronze.esco_skills_raw '
            f"WHERE version='{VERSION}' AND lang='{LANG}'\""
        )
        assert "3" in proc2.stdout, f"Idempotency broken:\n{proc2.stdout}"
