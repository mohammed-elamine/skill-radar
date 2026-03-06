#!/usr/bin/env python3
"""DockerOperator smoke check — scheduler-side operational validation.

Run inside the ``airflow-scheduler`` container to verify every
prerequisite for DockerOperator-based task execution:

    1. Docker daemon connectivity (via mounted socket)
    2. Spark runtime image present locally
    3. Compose network reachable
    4. Short-lived container launch + teardown

Exit 0 only when **all** checks pass.

Configuration (read from environment):
    SKILLRADAR_SPARK_IMAGE    - e.g. ``skillradar-spark:3.5.7-uv``
    SKILLRADAR_DOCKER_NETWORK - e.g. ``skillradar_default``
"""

from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SEPARATOR = "-" * 60
_TEST_COMMAND = ["python", "--version"]
_CONTAINER_TIMEOUT = 30  # seconds


def _fail(msg: str, *, hint: str | None = None) -> int:
    """Print a structured failure message and return exit code 1."""
    print(f"FAIL  {msg}", file=sys.stderr)
    if hint:
        print(f"      Hint: {hint}", file=sys.stderr)
    return 1


def main() -> int:
    # ------------------------------------------------------------------
    # 0. Read configuration from environment
    # ------------------------------------------------------------------
    spark_image = os.environ.get("SKILLRADAR_SPARK_IMAGE", "")
    docker_network = os.environ.get("SKILLRADAR_DOCKER_NETWORK", "")

    if not spark_image:
        return _fail(
            "SKILLRADAR_SPARK_IMAGE is not set.",
            hint="Export the variable or check the Airflow compose environment.",
        )
    if not docker_network:
        return _fail(
            "SKILLRADAR_DOCKER_NETWORK is not set.",
            hint="Export the variable or check the Airflow compose environment.",
        )

    print(_SEPARATOR)
    print("DockerOperator Smoke Check")
    print(_SEPARATOR)
    print(f"  Image  : {spark_image}")
    print(f"  Network: {docker_network}")
    print(_SEPARATOR)

    # Lazy-import to surface a clear message if the package is missing.
    try:
        from docker.errors import DockerException, ImageNotFound  # type: ignore[import-untyped]

        import docker  # type: ignore[import-untyped]
    except ImportError:
        return _fail(
            "'docker' Python package is not installed in this image.",
            hint="Ensure apache-airflow-providers-docker is in requirements.txt.",
        )

    container = None

    try:
        # ------------------------------------------------------------------
        # 1. Docker daemon connectivity
        # ------------------------------------------------------------------
        print("\n[1/4] Connecting to Docker daemon ...")
        client = docker.from_env()
        version_info = client.version()
        daemon_version = version_info.get("Version", "unknown")
        api_version = version_info.get("ApiVersion", "unknown")
        print(f"  OK  Docker {daemon_version} (API {api_version})")

        # ------------------------------------------------------------------
        # 2. Spark image availability
        # ------------------------------------------------------------------
        print(f"\n[2/4] Checking image: {spark_image} ...")
        try:
            img = client.images.get(spark_image)
            short_id = img.short_id.replace("sha256:", "")
            print(f"  OK  Image found ({short_id})")
        except ImageNotFound:
            return _fail(
                f"Image '{spark_image}' not found locally.",
                hint="Run 'make infra-up' or 'docker compose build spark' first.",
            )

        # ------------------------------------------------------------------
        # 3. Docker network availability
        # ------------------------------------------------------------------
        print(f"\n[3/4] Checking network: {docker_network} ...")
        networks = client.networks.list(names=[docker_network])
        if not networks:
            return _fail(
                f"Network '{docker_network}' not found.",
                hint="Run 'make airflow-up' to create the Compose network.",
            )
        driver = networks[0].attrs.get("Driver", "unknown")
        print(f"  OK  Network found (driver={driver})")

        # ------------------------------------------------------------------
        # 4. Launch a short-lived test container
        # ------------------------------------------------------------------
        print(f"\n[4/4] Launching test container ({' '.join(_TEST_COMMAND)}) ...")
        container = client.containers.run(
            image=spark_image,
            command=_TEST_COMMAND,
            network=docker_network,
            environment={"PYTHONDONTWRITEBYTECODE": "1"},
            detach=True,
            remove=False,  # manual cleanup in finally
        )
        result = container.wait(timeout=_CONTAINER_TIMEOUT)
        exit_code = result.get("StatusCode", -1)
        logs = container.logs().decode("utf-8", errors="replace").strip()

        if exit_code != 0:
            return _fail(
                f"Test container exited with code {exit_code}.",
                hint=f"Container logs:\n{logs}",
            )
        print(f"  OK  Container exited 0 — output: {logs}")

    except DockerException as exc:
        return _fail(f"Docker error — {exc}")
    except Exception as exc:
        return _fail(f"Unexpected error — {exc}")
    finally:
        if container is not None:
            try:
                container.remove(force=True)
                print("  Cleanup: test container removed.")
            except Exception:
                pass  # best-effort removal

    # ------------------------------------------------------------------
    # All checks passed
    # ------------------------------------------------------------------
    print(f"\n{_SEPARATOR}")
    print("All checks passed. DockerOperator prerequisites verified.")
    print(_SEPARATOR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
