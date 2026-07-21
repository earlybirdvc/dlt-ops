"""Airflow adapter: packaging hygiene, plugin surface, DAG factory, task mechanics.

Airflow is NOT in the dev dependency group. Tests split by requirement:

- Plugin-surface + hygiene tests run in ANY environment (they prove the bare
  install stays healthy with the adapter's entry points registered).
- ``needs_airflow`` tests run only with the ``[airflow]`` extra, e.g.::

    uv run --with "apache-airflow>=2.9,<3" --no-sync pytest tests/test_airflow_runtime.py

- ``needs_no_airflow`` tests assert the clear install-hint errors and run
  (only) in the normal Airflow-less environment.
"""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pendulum
import pytest
from click.testing import CliRunner

# Keep any Airflow import (ours are lazy, inside tests) away from ~/airflow.
os.environ.setdefault("AIRFLOW_HOME", os.path.join(tempfile.gettempdir(), "dltx-test-airflow-home"))

from dlt_ops.airflow import AirflowVariableBackend, airflow_rules  # noqa: E402
from dlt_ops.cli.plugins import plugins as plugins_cli  # noqa: E402
from dlt_ops.discovery import Schedule, validate_sources  # noqa: E402
from dlt_ops.discovery.phase2 import SOURCE_MODULE_NAMESPACE  # noqa: E402
from dlt_ops.plugins import registry as registry_mod  # noqa: E402
from dlt_ops.secrets import SecretBackend, SecretNotFoundError, SecretRequest, setup_secrets  # noqa: E402
from dlt_ops.secrets.setup import resolve_backend  # noqa: E402

AIRFLOW_MISSING = importlib.util.find_spec("airflow") is None

needs_airflow = pytest.mark.skipif(
    AIRFLOW_MISSING, reason="apache-airflow not installed — run with the [airflow] extra"
)
needs_no_airflow = pytest.mark.skipif(
    not AIRFLOW_MISSING, reason="asserts install-hint errors; only meaningful without airflow"
)

WEB_EVENTS_SOURCE = """
    import dlt

    @dlt.resource(name="page_views")
    def page_views():
        yield {"id": 1}

    @dlt.resource(name="sessions")
    def sessions():
        yield {"id": 2}

    @dlt.source(name="web_events")
    def web_events_source():
        return [page_views, sessions]
"""

ORDERS_SOURCE = """
    import dlt

    @dlt.resource(name="orders")
    def orders():
        yield {"id": 1, "created_at": "2024-01-05T00:00:00Z"}

    @dlt.source(name="orders_api")
    def orders_api_source():
        return orders
"""

DYNAMIC_FEED_SOURCE = """
    import dlt

    @dlt.source(name="dyn_feed")
    def dyn_feed_source():
        def build(name):
            return dlt.resource([{"id": 1}], name=name)
        return [build("alpha")]
"""

CANARY_SOURCE = """
    from pathlib import Path

    import dlt

    Path(__file__).with_name("canary.txt").write_text("side effect")

    @dlt.resource(name="rows")
    def rows():
        yield {"id": 1}

    @dlt.source(name="canary_api")
    def canary_api_source():
        return rows
"""

PROJECT_CONFIG = """
    [dlt_ops]
    default_destination = "duckdb"
    default_dataset = "raw_data"

    [sources.web_events.dlt_ops]
    schedule = "@daily"

    [sources.orders_api.dlt_ops]
    schedule = "@2hourly"

    [sources.dyn_feed.dlt_ops]
    schedule = "@manual"

    [sources.canary_api.dlt_ops]
    schedule = "@daily"
"""


@pytest.fixture(autouse=True)
def _isolate_dlt_env(monkeypatch):
    """dlt's PipelineTasksGroup mutates DLT_* env at parse; don't leak across tests.

    The dlt Airflow config provider is disabled so config resolution in the
    airflow-installed environment never consults Airflow Variables (no
    metastore in tests).
    """
    monkeypatch.setenv("PROVIDERS__ENABLE_AIRFLOW_SECRETS", "false")
    for var in ("DLT_DATA_DIR", "DLT_LOCAL_DIR"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def clean_registry():
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


@pytest.fixture
def project(make_project) -> Path:
    return make_project(
        config=PROJECT_CONFIG,
        files={
            "web/source/web_events.py": WEB_EVENTS_SOURCE,
            "orders/source/orders_api.py": ORDERS_SOURCE,
            "dyn/source/dyn_feed.py": DYNAMIC_FEED_SOURCE,
            "canary/source/canary_api.py": CANARY_SOURCE,
        },
    )


class TestPluginSurface:
    """The adapter's entry points must be healthy in EVERY environment."""

    def test_backend_conforms_to_protocol(self):
        assert isinstance(AirflowVariableBackend(), SecretBackend)

    def test_secret_backend_registered_and_loadable(self, clean_registry):
        assert "airflow" in registry_mod.names("secret_backend")
        assert registry_mod.get("secret_backend", "airflow") is AirflowVariableBackend

    def test_validator_provider_registered_and_loadable(self, clean_registry):
        assert "airflow" in registry_mod.names("validators")
        provider = registry_mod.get("validators", "airflow")
        assert provider is airflow_rules

    def test_plugins_doctor_lists_airflow_plugins_healthy(self, clean_registry):
        """The adapter's own plugins never FAIL in doctor (other extras' health is not ours)."""
        result = CliRunner().invoke(plugins_cli, ["doctor"])
        airflow_lines = [line for line in result.output.splitlines() if line.strip().startswith("airflow")]
        assert len(airflow_lines) >= 2, result.output  # secret_backend + validators
        assert all("FAILED" not in line for line in airflow_lines), result.output

    def test_claims_on_airflow_var(self):
        requests = AirflowVariableBackend().secret_requests({"airflow_var": "web-events-key"})
        assert requests == (SecretRequest(ref="web-events-key", key="api_secret_key"),)

    def test_claims_with_custom_key(self):
        requests = AirflowVariableBackend().secret_requests(
            {"airflow_var": "web-events-key", "airflow_var_key": "db_password"}
        )
        assert requests == (SecretRequest(ref="web-events-key", key="db_password"),)

    def test_no_claim_without_airflow_var(self):
        assert AirflowVariableBackend().secret_requests({"schedule": "@daily"}) == ()

    def test_resolve_backend_engages_airflow(self, clean_registry):
        config = {"sources": {"web_events": {"dlt_ops": {"schedule": "@daily", "airflow_var": "k"}}}}
        engagement = resolve_backend("web_events", config)
        assert engagement.name == "airflow"

    def test_resolve_backend_default_without_trigger_key(self, clean_registry):
        config = {"sources": {"web_events": {"dlt_ops": {"schedule": "@daily"}}}}
        assert resolve_backend("web_events", config).name == "secrets_toml"


class TestImportHygiene:
    def test_plugin_surface_pulls_no_airflow_modules(self):
        """Loading the adapter's plugin surface must not import airflow itself."""
        code = (
            "import json, sys\n"
            "import dlt_ops\n"
            "import dlt_ops.airflow\n"
            "from dlt_ops.airflow import AirflowVariableBackend, airflow_rules\n"
            "print(json.dumps(sorted(sys.modules)))\n"
        )
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        loaded = json.loads(proc.stdout)
        offenders = [module for module in loaded if module == "airflow" or module.startswith("airflow.")]
        assert offenders == []

    @needs_no_airflow
    def test_adapter_surface_raises_install_hint(self):
        with pytest.raises(ImportError, match=r"dlt-ops\[airflow\]"):
            from dlt_ops.airflow import build_schedule_dags  # noqa: F401

    @needs_no_airflow
    def test_factory_module_raises_install_hint(self):
        proc = subprocess.run(
            [sys.executable, "-c", "import dlt_ops.airflow.factory"],
            capture_output=True,
            text=True,
        )
        assert proc.returncode != 0
        assert "dlt-ops[airflow]" in proc.stderr

    @needs_no_airflow
    def test_tasks_module_raises_install_hint(self):
        proc = subprocess.run(
            [sys.executable, "-c", "import dlt_ops.airflow.tasks"],
            capture_output=True,
            text=True,
        )
        assert proc.returncode != 0
        assert "dlt-ops[airflow]" in proc.stderr

    @needs_no_airflow
    def test_backend_get_raises_install_hint(self):
        with pytest.raises(ImportError, match=r"dlt-ops\[airflow\]"):
            AirflowVariableBackend().get("some-variable")

    @needs_no_airflow
    def test_rules_inactive_without_airflow(self):
        assert airflow_rules() == ()


@needs_airflow
class TestScheduleCronMap:
    def test_2hourly_materializes_as_cron(self):
        """Airflow has no @2hourly preset; a missing entry would ship the raw
        label and Airflow would reject the DAG at parse time."""
        from dlt_ops.airflow.factory import SCHEDULE_CRON_MAP

        assert SCHEDULE_CRON_MAP[Schedule.TWO_HOURLY.value] == "0 */2 * * *"

    def test_weekly_pinned_to_monday(self):
        """Monday 00:00 UTC closes the ISO week — the Sunday-00:00 preset
        would leave the week's own Sunday uncaptured. Fixed by design."""
        from dlt_ops.airflow.factory import SCHEDULE_CRON_MAP

        assert SCHEDULE_CRON_MAP[Schedule.WEEKLY.value] == "0 0 * * 1"

    def test_schedule_to_airflow_mapping(self):
        from dlt_ops.airflow.factory import schedule_to_airflow

        assert schedule_to_airflow(Schedule.MANUAL) is None
        assert schedule_to_airflow(Schedule.DAILY) == "@daily"
        assert schedule_to_airflow(Schedule.HOURLY) == "@hourly"
        assert schedule_to_airflow(Schedule.MONTHLY) == "@monthly"
        assert schedule_to_airflow(Schedule.TWO_HOURLY) == "0 */2 * * *"
        assert schedule_to_airflow(Schedule.WEEKLY) == "0 0 * * 1"


@needs_airflow
class TestDagFactory:
    def test_one_dag_per_schedule_group(self, project):
        from dlt_ops.airflow.factory import build_schedule_dags

        dags = build_schedule_dags(project)

        assert set(dags) == {"dlt_daily", "dlt_2hourly", "dlt_manual"}
        assert dags["dlt_daily"].schedule_interval == "@daily"
        assert dags["dlt_2hourly"].schedule_interval == "0 */2 * * *"
        assert dags["dlt_manual"].schedule_interval is None

    def test_task_ids_follow_adapter_contract(self, project):
        """{pipeline}.{source}_{resource} — one task per Phase-1 static resource."""
        from dlt_ops.airflow.factory import build_schedule_dags

        dags = build_schedule_dags(project)

        assert set(dags["dlt_daily"].task_ids) == {
            "web_events.web_events_page_views",
            "web_events.web_events_sessions",
            "canary_api.canary_api_rows",
        }
        assert set(dags["dlt_2hourly"].task_ids) == {"orders_api.orders_api_orders"}

    def test_dynamic_only_source_gets_whole_source_task(self, project):
        from dlt_ops.airflow.factory import build_schedule_dags

        dags = build_schedule_dags(project)

        assert set(dags["dlt_manual"].task_ids) == {"dyn_feed.dyn_feed"}

    def test_parse_time_imports_no_source_modules(self, project):
        """The Rule 15 DAG-parse foot-gun: factory must stay Phase-1 (AST) only."""
        from dlt_ops.airflow.factory import build_schedule_dags

        build_schedule_dags(project)

        assert not (project / "canary" / "source" / "canary.txt").exists()
        assert f"{SOURCE_MODULE_NAMESPACE}.canary.source.canary_api" not in sys.modules

    def test_dag_prefix_and_kwargs(self, project):
        from dlt_ops.airflow.factory import build_schedule_dags

        dags = build_schedule_dags(project, dag_prefix="ingest", dag_kwargs={"tags": ["dlt"]})

        assert set(dags) == {"ingest_daily", "ingest_2hourly", "ingest_manual"}
        assert dags["ingest_daily"].tags == {"dlt"} or dags["ingest_daily"].tags == ["dlt"]

    def test_cleanup_task_appended_when_configured(self, project, tmp_path):
        from dlt_ops.airflow.factory import build_schedule_dags

        dags = build_schedule_dags(project, cleanup_data_dir=tmp_path / "scratch")

        for dag in dags.values():
            assert "cleanup_old_dlt_files" in dag.task_ids
            cleanup = dag.get_task("cleanup_old_dlt_files")
            assert cleanup.upstream_task_ids  # runs after the pipeline groups

    def test_no_cleanup_task_by_default(self, project):
        from dlt_ops.airflow.factory import build_schedule_dags

        dags = build_schedule_dags(project)

        assert all("cleanup_old_dlt_files" not in dag.task_ids for dag in dags.values())


@needs_airflow
class TestExecuteUnit:
    """Task-body mechanics over a fake Airflow context (conf JSON contract)."""

    NATIVE_START = pendulum.datetime(2025, 1, 1)
    NATIVE_END = pendulum.datetime(2025, 1, 2)
    KNOWN = ("orders_api", "web_events")

    @pytest.fixture
    def run_calls(self, monkeypatch):
        calls: list[dict] = []

        def fake_run_source(source_name, **kwargs):
            calls.append({"source_name": source_name, **kwargs})

        monkeypatch.setattr("dlt_ops.orchestration.run_source", fake_run_source)
        return calls

    def _patch_context(self, monkeypatch, conf: dict | None) -> None:
        context = {
            "dag_run": SimpleNamespace(conf=conf),
            "data_interval_start": self.NATIVE_START,
            "data_interval_end": self.NATIVE_END,
        }
        monkeypatch.setattr("dlt_ops.airflow.factory.get_current_context", lambda: context)

    def _execute(self, resource="page_views", has_native_window=True):
        from dlt_ops.airflow.factory import _execute_unit

        _execute_unit(
            project_root="/proj",
            source_name="web_events",
            resource=resource,
            known_sources=self.KNOWN,
            has_native_window=has_native_window,
        )

    def test_no_conf_runs_with_native_window(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, None)
        self._execute()

        assert run_calls == [
            {
                "source_name": "web_events",
                "project_root": Path("/proj"),
                "resources": ("page_views",),
                "window": (self.NATIVE_START, self.NATIVE_END),
                "trigger_source": "airflow",
            }
        ]

    def test_manual_schedule_runs_unbounded(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, None)
        self._execute(has_native_window=False)

        assert run_calls[0]["window"] is None

    def test_source_mismatch_skips(self, monkeypatch, run_calls):
        from airflow.exceptions import AirflowSkipException

        self._patch_context(monkeypatch, {"source": "orders_api"})
        with pytest.raises(AirflowSkipException, match="not selected"):
            self._execute()
        assert run_calls == []

    def test_resource_not_selected_skips(self, monkeypatch, run_calls):
        from airflow.exceptions import AirflowSkipException

        self._patch_context(monkeypatch, {"source": "web_events", "resources": ["sessions"]})
        with pytest.raises(AirflowSkipException, match="not selected"):
            self._execute()
        assert run_calls == []

    def test_resource_selected_runs(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, {"source": "web_events", "resources": ["page_views"]})
        self._execute()
        assert run_calls[0]["resources"] == ("page_views",)

    def test_invalid_source_raises(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, {"source": "nonexistent"})
        with pytest.raises(ValueError, match="Invalid source"):
            self._execute()
        assert run_calls == []

    def test_date_overrides_feed_bounds(self, monkeypatch, run_calls):
        self._patch_context(
            monkeypatch,
            {"start_date": "2024-01-01T00:00:00Z", "end_date": "2024-02-01T00:00:00Z"},
        )
        self._execute()

        assert run_calls[0]["window"] == (pendulum.datetime(2024, 1, 1), pendulum.datetime(2024, 2, 1))

    def test_partial_override_keeps_native_edge(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, {"start_date": "2024-06-15T00:00:00Z"})
        self._execute()

        assert run_calls[0]["window"] == (pendulum.datetime(2024, 6, 15), self.NATIVE_END)

    def test_invalid_date_raises(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, {"start_date": "not-a-date"})
        with pytest.raises(ValueError, match="Invalid start_date format"):
            self._execute()
        assert run_calls == []

    def test_whole_source_task_passes_resource_selection_to_runner(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, {"source": "web_events", "resources": ["sessions"]})
        self._execute(resource=None)

        assert run_calls[0]["resources"] == ("sessions",)

    def test_whole_source_task_without_selection_runs_all(self, monkeypatch, run_calls):
        self._patch_context(monkeypatch, None)
        self._execute(resource=None)

        assert run_calls[0]["resources"] is None


@needs_airflow
class TestAirflowVariableBackendGet:
    def test_get_reads_airflow_variable(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_VAR_WEB_EVENTS_KEY", "s3cret-value")
        assert AirflowVariableBackend().get("web_events_key") == "s3cret-value"

    def test_missing_variable_raises_secret_not_found(self, monkeypatch):
        from airflow.models import Variable

        def missing(key, *args, **kwargs):
            raise KeyError(f"Variable {key} does not exist")

        monkeypatch.setattr(Variable, "get", missing)
        with pytest.raises(SecretNotFoundError, match="does not exist"):
            AirflowVariableBackend().get("nope")

    def test_setup_secrets_end_to_end(self, monkeypatch, clean_registry):
        """Claim via airflow_var -> Variable.get -> dlt.secrets, on the real registry."""
        import dlt

        monkeypatch.setenv("AIRFLOW_VAR_WEB_EVENTS_KEY", "t0ps3cret")
        config = {
            "sources": {
                "web_events": {"dlt_ops": {"schedule": "@daily", "airflow_var": "web_events_key"}},
            }
        }
        from dlt_ops.discovery.models import SourceInfo

        source = SourceInfo(
            name="web_events",
            pipeline_name="web",
            path=Path("/proj/web"),
            function_name="web_events_source",
            resources=("page_views",),
            module_stem="web_events",
            source_fn=lambda: None,
        )
        setup_secrets(sources={"web_events": source}, config=config)
        assert dlt.secrets["sources.web_events.api_secret_key"] == "t0ps3cret"


@needs_airflow
class TestAirflowVarRequiredRule:
    SECRET_SOURCE = """
        import dlt

        @dlt.resource(name="rows")
        def rows(api_key=dlt.secrets.value):
            yield {"id": 1}

        @dlt.source(name="billing_api")
        def billing_api_source(api_key=dlt.secrets.value):
            return rows
    """

    def _project(self, make_project, extra_config: str = "") -> Path:
        return make_project(
            config=f"""
                [dlt_ops]
                default_destination = "duckdb"
                default_dataset = "raw_data"
                {extra_config}

                [sources.billing_api.dlt_ops]
                schedule = "@daily"
            """,
            files={"billing/source/billing_api.py": self.SECRET_SOURCE},
        )

    def test_rule_spec_identity(self):
        specs = airflow_rules()
        assert [spec.rule_id for spec in specs] == ["airflow_var_required"]
        assert specs[0].plugin == "airflow"
        assert specs[0].default_on is True

    def test_rule_fires_with_extra_installed(self, make_project, clean_registry):
        errors = validate_sources(self._project(make_project))
        assert any(e.field == "airflow_var" for e in errors), errors

    def test_rule_obeys_rules_knob(self, make_project, clean_registry):
        root = self._project(
            make_project,
            extra_config="""
                [dlt_ops.rules]
                airflow_var_required = false
            """,
        )
        errors = validate_sources(root)
        assert not any(e.field == "airflow_var" for e in errors), errors


@needs_airflow
class TestCleanupTask:
    def _run(self, **kwargs):
        from dlt_ops.airflow.tasks import cleanup_old_dlt_files

        # Call the decorated task's wrapped function directly (no Airflow run).
        cleanup_old_dlt_files.function(**kwargs)

    def test_deletes_old_dlt_entries(self, tmp_path):
        old_dir = tmp_path / "dlt_old"
        old_dir.mkdir()
        (old_dir / "state.json").write_text("{}")
        fresh_dir = tmp_path / "dlt_fresh"
        fresh_dir.mkdir()
        unrelated = tmp_path / "keep.txt"
        unrelated.write_text("keep")
        stale = pendulum.now().subtract(days=10).int_timestamp
        os.utime(old_dir, (stale, stale))

        self._run(data_dir=str(tmp_path), days=3)

        assert not old_dir.exists()
        assert fresh_dir.exists()
        assert unrelated.exists()

    def test_disabled_is_a_no_op(self, tmp_path):
        old_dir = tmp_path / "dlt_old"
        old_dir.mkdir()
        stale = pendulum.now().subtract(days=10).int_timestamp
        os.utime(old_dir, (stale, stale))

        self._run(data_dir=str(tmp_path), days=3, enabled=False)

        assert old_dir.exists()

    def test_missing_dir_raises(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            self._run(data_dir=str(tmp_path / "nope"))


@needs_airflow
@pytest.mark.integration
class TestEndToEndDuckdb:
    def test_execute_unit_loads_rows(self, project, monkeypatch, tmp_path, clean_registry):
        """Full task body against a real project: discover -> secrets -> runner -> duckdb."""
        import duckdb

        monkeypatch.chdir(tmp_path)
        # Fresh pipeline state per test (never ~/.dlt): stale state would pin
        # the duckdb file to a previous run's location.
        monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
        context = {
            "dag_run": SimpleNamespace(conf={"source": "orders_api"}),
            "data_interval_start": pendulum.datetime(2024, 1, 1),
            "data_interval_end": pendulum.datetime(2024, 2, 1),
        }
        monkeypatch.setattr("dlt_ops.airflow.factory.get_current_context", lambda: context)
        from dlt_ops.airflow.factory import _execute_unit

        _execute_unit(
            project_root=str(project),
            source_name="orders_api",
            resource="orders",
            known_sources=("orders_api",),
            has_native_window=False,
        )

        db_file = tmp_path / "orders_api_pipeline.duckdb"
        assert db_file.exists(), f"expected {db_file.name}; found {[p.name for p in tmp_path.glob('*.duckdb')]}"
        with duckdb.connect(str(db_file)) as conn:
            rows = conn.execute("SELECT id FROM raw_data.orders").fetchall()
        assert rows == [(1,)]
