"""Unit tests for the Airflow shared orchestration layer.

Tests cover:
- Configuration loading from environment variables
- DockerOperator factory kwargs generation
- DAG default_args and tag helpers
- DAG import stability (all DAGs parse without errors)
- DAG task graph structure validation
- Callback behavior

These tests run without Airflow infrastructure — they only verify
that our orchestration helpers produce correct values and that DAG
files parse cleanly.
"""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Skip the entire module when apache-airflow is not installed (it lives
# only inside the Airflow Docker image, not in the project's dev deps).
pytest.importorskip("airflow", reason="apache-airflow not installed")

# ---------------------------------------------------------------------------
# Airflow test environment — MUST be set before any Airflow import
# ---------------------------------------------------------------------------

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow_test_"))
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__XCOM_BACKEND"] = "airflow.models.xcom.BaseXCom"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
    f"sqlite:///{tempfile.mkdtemp(prefix='airflow_test_')}/airflow.db"
)

# ---------------------------------------------------------------------------
# Ensure the dags directory is on sys.path so _shared is importable
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DAGS_DIR = _PROJECT_ROOT / "dags"

if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


# ===========================================================================
# Config tests
# ===========================================================================


class TestConfig:
    """Tests for ``_shared.config``."""

    def test_default_values(self):
        """Config loads safe defaults when no env vars are set."""
        # Clear any SKILLRADAR_ env vars that might be set
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("SKILLRADAR_")}
        with patch.dict(os.environ, clean_env, clear=True):
            # Force reimport to pick up clean env
            import _shared.config as cfg_mod

            importlib.reload(cfg_mod)

            assert cfg_mod.SPARK_IMAGE == "skillradar-spark:3.5.7-uv"
            assert cfg_mod.DOCKER_NETWORK == "skillradar_default"
            assert cfg_mod.S3_BUCKET == "skillradar-lake"
            assert cfg_mod.S3_LOGS_BUCKET == "skillradar-logs"
            assert cfg_mod.ADZUNA_COUNTRY == "fr"
            assert cfg_mod.ADZUNA_SCHEDULE == "0 6 * * *"
            assert cfg_mod.ESCO_VERSION == "v1.2.1"
            assert cfg_mod.ESCO_LANG == "fr"
            assert cfg_mod.ESCO_RUN_GOLD_AFTER is False
            assert cfg_mod.TASK_RETRIES == 2
            assert cfg_mod.TASK_RETRY_DELAY_SECONDS == 120
            assert cfg_mod.MAX_ACTIVE_RUNS == 1

    def test_env_overrides(self):
        """Config values can be overridden via environment variables."""
        overrides = {
            "SKILLRADAR_SPARK_IMAGE": "custom-spark:latest",
            "SKILLRADAR_DOCKER_NETWORK": "custom_net",
            "SKILLRADAR_ADZUNA_SCHEDULE": "0 12 * * *",
            "SKILLRADAR_ADZUNA_COUNTRY": "gb",
            "SKILLRADAR_ESCO_VERSION": "v2.0.0",
            "SKILLRADAR_TASK_RETRIES": "5",
            "SKILLRADAR_ESCO_RUN_GOLD_AFTER": "true",
        }
        with patch.dict(os.environ, overrides):
            import _shared.config as cfg_mod

            importlib.reload(cfg_mod)

            assert cfg_mod.SPARK_IMAGE == "custom-spark:latest"
            assert cfg_mod.DOCKER_NETWORK == "custom_net"
            assert cfg_mod.ADZUNA_SCHEDULE == "0 12 * * *"
            assert cfg_mod.ADZUNA_COUNTRY == "gb"
            assert cfg_mod.ESCO_VERSION == "v2.0.0"
            assert cfg_mod.TASK_RETRIES == 5
            assert cfg_mod.ESCO_RUN_GOLD_AFTER is True

        # Reload to reset
        importlib.reload(cfg_mod)

    def test_env_bool_parsing(self):
        """Boolean env vars accept true/1/yes variants."""
        import _shared.config as cfg_mod

        for truthy in ("true", "True", "TRUE", "1", "yes", "YES"):
            with patch.dict(os.environ, {"SKILLRADAR_ESCO_RUN_GOLD_AFTER": truthy}):
                assert cfg_mod._env_bool("SKILLRADAR_ESCO_RUN_GOLD_AFTER", False) is True

        for falsy in ("false", "False", "0", "no", "whatever"):
            with patch.dict(os.environ, {"SKILLRADAR_ESCO_RUN_GOLD_AFTER": falsy}):
                assert cfg_mod._env_bool("SKILLRADAR_ESCO_RUN_GOLD_AFTER", True) is False

    def test_env_int_fallback_on_invalid(self):
        """Invalid integer env var falls back to default."""
        import _shared.config as cfg_mod

        with patch.dict(os.environ, {"SKILLRADAR_TASK_RETRIES": "not_a_number"}):
            assert cfg_mod._env_int("SKILLRADAR_TASK_RETRIES", 42) == 42

    def test_get_task_environment(self):
        """Task environment contains all required keys for Spark containers."""
        import _shared.config as cfg_mod

        importlib.reload(cfg_mod)
        env = cfg_mod.get_task_environment()

        required_keys = {
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
            "ADZUNA_APP_ID",
            "ADZUNA_APP_KEY",
            "JAVA_HOME",
            "PATH",
            "PYTHONPATH",
            "PYSPARK_PYTHON",
            "PYSPARK_DRIVER_PYTHON",
            "SKILLRADAR_RUNTIME_CONTEXT",
        }
        assert required_keys.issubset(set(env.keys()))
        assert env["SKILLRADAR_RUNTIME_CONTEXT"] == "docker"

    def test_container_mounts_non_empty(self):
        """Container mount spec has entries."""
        import _shared.config as cfg_mod

        importlib.reload(cfg_mod)
        assert len(cfg_mod.CONTAINER_MOUNTS) > 0
        for m in cfg_mod.CONTAINER_MOUNTS:
            assert "source" in m
            assert "target" in m


# ===========================================================================
# Defaults tests
# ===========================================================================


class TestDefaults:
    """Tests for ``_shared.defaults``."""

    def test_common_default_args_keys(self):
        """Common default_args contains expected keys."""
        from _shared.defaults import COMMON_DEFAULT_ARGS

        assert "owner" in COMMON_DEFAULT_ARGS
        assert COMMON_DEFAULT_ARGS["owner"] == "skill-radar"
        assert "retries" in COMMON_DEFAULT_ARGS
        assert "retry_delay" in COMMON_DEFAULT_ARGS
        assert "execution_timeout" in COMMON_DEFAULT_ARGS
        assert COMMON_DEFAULT_ARGS["depends_on_past"] is False

    def test_dag_tags(self):
        """Tag helper prepends base tag."""
        from _shared.defaults import dag_tags

        result = dag_tags("adzuna", "daily")
        assert result == ["skill-radar", "adzuna", "daily"]

    def test_dag_tags_empty(self):
        """Tag helper with no extras returns base tag only."""
        from _shared.defaults import dag_tags

        assert dag_tags() == ["skill-radar"]


# ===========================================================================
# Templates tests
# ===========================================================================


class TestTemplates:
    """Tests for ``_shared.templates``."""

    def test_partition_date_macro(self):
        """Partition date macro returns Jinja template."""
        from _shared.templates import partition_date_macro

        assert partition_date_macro() == "{{ ds }}"


# ===========================================================================
# Callbacks tests
# ===========================================================================


class TestCallbacks:
    """Tests for ``_shared.callbacks``."""

    def test_on_task_failure_logs(self, caplog):
        """Failure callback logs structured error."""
        from _shared.callbacks import on_task_failure

        class FakeTI:
            dag_id = "test_dag"
            task_id = "test_task"
            try_number = 2
            log_url = "http://localhost:8080/log"

        with caplog.at_level("ERROR", logger="skill_radar.airflow.callbacks"):
            on_task_failure({"task_instance": FakeTI(), "logical_date": "2025-01-15"})

        assert "TASK FAILED" in caplog.text
        assert "test_dag" in caplog.text
        assert "test_task" in caplog.text

    def test_on_task_failure_no_ti(self, caplog):
        """Failure callback handles missing task_instance gracefully."""
        from _shared.callbacks import on_task_failure

        with caplog.at_level("ERROR", logger="skill_radar.airflow.callbacks"):
            on_task_failure({})

        assert "without task_instance" in caplog.text

    def test_on_task_success_logs(self, caplog):
        """Success callback logs INFO message."""
        from _shared.callbacks import on_task_success

        class FakeTI:
            dag_id = "test_dag"
            task_id = "test_task"

        with caplog.at_level("INFO", logger="skill_radar.airflow.callbacks"):
            on_task_success({"task_instance": FakeTI(), "logical_date": "2025-01-15"})

        assert "TASK OK" in caplog.text


# ===========================================================================
# DockerOperator factory tests
# ===========================================================================


class TestDockerTasks:
    """Tests for ``_shared.docker_tasks``.

    We patch ``DockerOperator`` on the already-imported module to verify
    that the factory produces correct kwargs without instantiating real
    Airflow operators.
    """

    @pytest.fixture(autouse=True)
    def _reload_config(self):
        """Ensure config is loaded with clean defaults."""
        import _shared.config as cfg_mod

        importlib.reload(cfg_mod)

    def test_make_skill_radar_task_kwargs(self):
        """Factory produces DockerOperator with correct kwargs."""
        import _shared.config as cfg_mod
        from _shared import docker_tasks as dt_mod

        with patch.object(dt_mod, "DockerOperator") as MockOp:
            mock_dag = MagicMock()
            mock_dag.dag_id = "test_dag"

            dt_mod.make_skill_radar_task(
                task_id="test_task",
                command="skill-radar adzuna bronze --country fr",
                dag=mock_dag,
            )

            MockOp.assert_called_once()
            call_kwargs = MockOp.call_args[1]

            assert call_kwargs["task_id"] == "test_task"
            assert call_kwargs["image"] == cfg_mod.SPARK_IMAGE
            assert "skill-radar adzuna bronze --country fr" in call_kwargs["command"]
            assert call_kwargs["network_mode"] == cfg_mod.DOCKER_NETWORK
            assert call_kwargs["working_dir"] == cfg_mod.CONTAINER_WORKING_DIR
            assert call_kwargs["auto_remove"] == "success"
            assert call_kwargs["mount_tmp_dir"] is False
            assert call_kwargs["tty"] is False
            assert call_kwargs["do_xcom_push"] is False
            assert "environment" in call_kwargs
            assert "mounts" in call_kwargs

    def test_extra_env_merged(self):
        """Extra env vars are merged into the base environment."""
        from _shared import docker_tasks as dt_mod

        with patch.object(dt_mod, "DockerOperator") as MockOp:
            mock_dag = MagicMock()
            mock_dag.dag_id = "test_dag"

            dt_mod.make_skill_radar_task(
                task_id="test_task",
                command="echo hello",
                dag=mock_dag,
                extra_env={"CUSTOM_VAR": "custom_value"},
            )

            call_kwargs = MockOp.call_args[1]
            assert call_kwargs["environment"]["CUSTOM_VAR"] == "custom_value"
            # Base vars still present
            assert "AWS_ACCESS_KEY_ID" in call_kwargs["environment"]

    def test_retries_override(self):
        """Retry count can be overridden per task."""
        from _shared import docker_tasks as dt_mod

        with patch.object(dt_mod, "DockerOperator") as MockOp:
            mock_dag = MagicMock()
            mock_dag.dag_id = "test_dag"

            dt_mod.make_skill_radar_task(
                task_id="test_task",
                command="echo hello",
                dag=mock_dag,
                retries=0,
            )

            call_kwargs = MockOp.call_args[1]
            assert call_kwargs["retries"] == 0

    def test_mounts_built(self):
        """Mount builder produces docker.types.Mount objects."""
        from _shared.docker_tasks import _build_mounts
        from docker.types import Mount

        mounts = _build_mounts()
        assert len(mounts) > 0
        for m in mounts:
            assert isinstance(m, Mount)


# ===========================================================================
# DAG import tests — use DagBag for proper Airflow initialization
# ===========================================================================


@pytest.fixture(scope="module")
def dagbag():
    """Load all DAGs from the dags directory (once per module)."""
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(_DAGS_DIR), include_examples=False)
    return bag


class TestDAGImports:
    """Verify all DAG files parse cleanly via Airflow's DagBag."""

    def test_no_import_errors(self, dagbag):
        """All DAG files import without errors."""
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

    def test_adzuna_daily_pipeline_loaded(self, dagbag):
        """adzuna_daily_pipeline DAG is discovered."""
        assert "adzuna_daily_pipeline" in dagbag.dags

    def test_esco_manual_pipeline_loaded(self, dagbag):
        """esco_manual_pipeline DAG is discovered."""
        assert "esco_manual_pipeline" in dagbag.dags

    def test_total_dag_count(self, dagbag):
        """Exactly 2 DAGs are discovered."""
        assert len(dagbag.dags) == 2


# ===========================================================================
# DAG structure tests
# ===========================================================================


class TestDAGStructure:
    """Verify DAG task graphs are correct."""

    @pytest.fixture(scope="class")
    def adzuna_dag(self, dagbag):
        return dagbag.dags["adzuna_daily_pipeline"]

    @pytest.fixture(scope="class")
    def esco_dag(self, dagbag):
        return dagbag.dags["esco_manual_pipeline"]

    def test_adzuna_dag_id(self, adzuna_dag):
        assert adzuna_dag.dag_id == "adzuna_daily_pipeline"

    def test_adzuna_dag_task_count(self, adzuna_dag):
        """Adzuna DAG has exactly 6 tasks."""
        assert len(adzuna_dag.tasks) == 6

    def test_adzuna_dag_task_ids(self, adzuna_dag):
        """Adzuna DAG has the expected task ids."""
        task_ids = {t.task_id for t in adzuna_dag.tasks}
        expected = {
            "adzuna_bronze",
            "validate_adzuna_bronze",
            "adzuna_silver",
            "validate_adzuna_silver",
            "gold_pipeline",
            "validate_gold",
        }
        assert task_ids == expected

    def test_adzuna_dag_linear_chain(self, adzuna_dag):
        """Adzuna DAG follows a strict linear chain."""
        chain = [
            "adzuna_bronze",
            "validate_adzuna_bronze",
            "adzuna_silver",
            "validate_adzuna_silver",
            "gold_pipeline",
            "validate_gold",
        ]
        task_map = {t.task_id: t for t in adzuna_dag.tasks}
        for i in range(len(chain) - 1):
            upstream = task_map[chain[i]]
            downstream = task_map[chain[i + 1]]
            assert downstream.task_id in {t.task_id for t in upstream.downstream_list}

    def test_adzuna_dag_catchup_disabled(self, adzuna_dag):
        assert adzuna_dag.catchup is False

    def test_adzuna_dag_tags(self, adzuna_dag):
        assert "skill-radar" in adzuna_dag.tags
        assert "adzuna" in adzuna_dag.tags

    def test_esco_dag_id(self, esco_dag):
        assert esco_dag.dag_id == "esco_manual_pipeline"

    def test_esco_dag_schedule_none(self, esco_dag):
        """ESCO DAG is manual-only."""
        assert esco_dag.schedule_interval is None

    def test_esco_dag_task_ids(self, esco_dag):
        """ESCO DAG has the expected task ids."""
        task_ids = {t.task_id for t in esco_dag.tasks}
        expected = {
            "validate_esco_landing",
            "esco_bronze",
            "validate_esco_bronze",
            "esco_silver",
            "validate_esco_silver",
            "should_run_gold",
            "gold_pipeline",
            "validate_gold",
        }
        assert task_ids == expected

    def test_esco_dag_core_chain(self, esco_dag):
        """ESCO DAG core chain: landing → bronze → validate → silver → validate."""
        chain = [
            "validate_esco_landing",
            "esco_bronze",
            "validate_esco_bronze",
            "esco_silver",
            "validate_esco_silver",
        ]
        task_map = {t.task_id: t for t in esco_dag.tasks}
        for i in range(len(chain) - 1):
            upstream = task_map[chain[i]]
            downstream = task_map[chain[i + 1]]
            assert downstream.task_id in {t.task_id for t in upstream.downstream_list}

    def test_esco_dag_gold_gated(self, esco_dag):
        """Gold pipeline is gated by should_run_gold."""
        task_map = {t.task_id: t for t in esco_dag.tasks}
        gold_task = task_map["gold_pipeline"]
        upstream_ids = {t.task_id for t in gold_task.upstream_list}
        assert "should_run_gold" in upstream_ids

    def test_esco_dag_has_params(self, esco_dag):
        """ESCO DAG defines required params."""
        param_keys = set(esco_dag.params.keys())
        assert "version" in param_keys
        assert "lang" in param_keys
        assert "run_gold_after" in param_keys
        assert "validate_landing_first" in param_keys

    def test_esco_dag_tags(self, esco_dag):
        assert "skill-radar" in esco_dag.tags
        assert "esco" in esco_dag.tags
