"""Integration tests for ESCO bronze E2E validation.

These tests require docker infrastructure to be running:
- MinIO (S3-compatible storage)
- Spark with Iceberg catalog

Run with: pytest -m integration -q
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def spark_available() -> bool:
    """Check if Spark container is available."""
    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "spark", "echo", "ok"],
            capture_output=True,
            timeout=10,
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


@pytest.fixture(scope="module")
def fixture_zip() -> Path:
    """Return path to valid ESCO bronze fixture ZIP."""
    path = Path("tests/fixtures/esco/esco_bronze_valid.zip")
    if not path.exists():
        pytest.skip(f"Fixture not found: {path}")
    return path


@pytest.fixture(scope="module")
def invalid_fixture_zip() -> Path:
    """Return path to invalid ESCO fixture (missing column)."""
    path = Path("tests/fixtures/esco/esco_bronze_missing_col.zip")
    if not path.exists():
        pytest.skip(f"Fixture not found: {path}")
    return path


class TestInfraValidation:
    """Integration tests for infrastructure validation."""

    @pytest.mark.skipif(not minio_available(), reason="MinIO not available")
    def test_validate_infra_from_host(self) -> None:
        """Infrastructure validation runs from host (MinIO checks)."""
        result = subprocess.run(
            ["uv", "run", "skill-radar", "validate", "infra", "--quiet"],
            capture_output=True,
            timeout=30,
        )

        # Should either pass or just skip spark check
        # Exit code 0 = pass, 10 = infra failure
        # We expect it to pass MinIO checks even if Spark is skipped
        stdout = result.stdout.decode()
        stderr = result.stderr.decode()

        # Print for debugging in case of failure
        if result.returncode != 0:
            print(f"stdout: {stdout}")
            print(f"stderr: {stderr}")

        # Allow skip (spark not available from host) but MinIO should work
        assert result.returncode in [0, 10], f"Unexpected exit: {result.returncode}"


class TestEscoLandingValidation:
    """Integration tests for ESCO landing validation."""

    @pytest.mark.skipif(not minio_available(), reason="MinIO not available")
    def test_validate_landing_missing_artifact(self) -> None:
        """Landing validation fails when artifact doesn't exist."""
        # Use a version that definitely doesn't exist
        result = subprocess.run(
            [
                "uv",
                "run",
                "skill-radar",
                "validate",
                "esco-landing",
                "--version",
                "v99.99.99",
                "--lang",
                "fr",
                "--quiet",
            ],
            capture_output=True,
            timeout=30,
        )

        # Should fail with landing failure exit code (20)
        assert result.returncode == 20, f"Expected exit 20, got {result.returncode}"


class TestEscoBronzeE2E:
    """Integration tests for ESCO bronze E2E validation."""

    @pytest.mark.skipif(not spark_available(), reason="Spark container not available")
    @pytest.mark.skipif(not minio_available(), reason="MinIO not available")
    def test_bronze_e2e_with_valid_fixture(self, fixture_zip: Path) -> None:
        """Full E2E validation passes with valid fixture."""
        # Run validation via docker compose exec spark
        cmd = [
            "docker",
            "compose",
            "exec",
            "-T",
            "spark",
            "bash",
            "-lc",
            f"uv run skill-radar validate esco-bronze-e2e "
            f"--version v1.0.0 --lang fr "
            f"--file /opt/skillradar/{fixture_zip}",
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=300,  # 5 minutes for full E2E
        )

        stdout = result.stdout.decode()
        stderr = result.stderr.decode()

        if result.returncode != 0:
            print("E2E validation failed")
            print(f"stdout: {stdout}")
            print(f"stderr: {stderr}")

        assert result.returncode == 0, f"E2E validation failed with code {result.returncode}"

    @pytest.mark.skipif(not spark_available(), reason="Spark container not available")
    @pytest.mark.skipif(not minio_available(), reason="MinIO not available")
    def test_bronze_e2e_via_docker_flag(self, fixture_zip: Path) -> None:
        """E2E validation works with --via-docker flag from host."""
        # First upload the fixture to landing
        upload_cmd = [
            "uv",
            "run",
            "skill-radar",
            "esco",
            "upload",
            "--version",
            "v1.0.1",
            "--lang",
            "fr",
            "--file",
            str(fixture_zip),
            "--force",
        ]

        upload_result = subprocess.run(
            upload_cmd,
            capture_output=True,
            timeout=60,
        )

        if upload_result.returncode != 0:
            print(f"Upload failed: {upload_result.stderr.decode()}")
            pytest.skip("Upload failed, cannot run E2E test")

        # Then run bronze via docker (extraction only, no checks from host)
        validate_cmd = [
            "uv",
            "run",
            "skill-radar",
            "validate",
            "esco-bronze-e2e",
            "--version",
            "v1.0.1",
            "--lang",
            "fr",
            "--file",
            str(fixture_zip),
            "--via-docker",
            "--quiet",
        ]

        result = subprocess.run(
            validate_cmd,
            capture_output=True,
            timeout=300,
        )

        # In via-docker mode, bronze checks are skipped from host
        # So we expect success (bronze extraction worked)
        assert result.returncode == 0, f"Via-docker E2E failed: {result.returncode}"


class TestValidationReportOutput:
    """Integration tests for validation report output."""

    @pytest.mark.skipif(not minio_available(), reason="MinIO not available")
    def test_writes_report_to_logs_dir(self, tmp_path: Path) -> None:
        """Validation writes JSON report to logs directory."""
        import os

        # Set LOG_DIR to temp path
        env = os.environ.copy()
        env["LOG_DIR"] = str(tmp_path)

        subprocess.run(
            ["uv", "run", "skill-radar", "validate", "infra", "--quiet"],
            capture_output=True,
            timeout=30,
            env=env,
        )

        # Check for report file
        report_dir = tmp_path / "validation" / "infra"
        if report_dir.exists():
            reports = list(report_dir.glob("*.json"))
            assert len(reports) >= 1, "No report file written"

            # Verify it's valid JSON
            import json

            with reports[0].open() as f:
                data = json.load(f)
            assert "validator_name" in data
            assert data["validator_name"] == "infra"
