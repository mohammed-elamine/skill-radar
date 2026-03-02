import subprocess

import pytest


@pytest.mark.integration
def test_iceberg_smoke():
    # Assumes `docker compose up -d` already ran
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "spark",
            "bash",
            "-lc",
            "spark-submit /opt/skillradar/jobs/examples/iceberg_smoke_test.py",
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + "\n" + proc.stderr
