"""Core orchestrator interface: grouping, filtering/window decisions, run entry.

Everything here runs WITHOUT Airflow — the interface is the orchestrator-
neutral half of the X→Y ladder; adapter mechanics live in
tests/test_airflow_runtime.py.
"""

import sys
from pathlib import Path

import pendulum
import pytest

from dlt_ops import orchestration
from dlt_ops.discovery import Schedule
from dlt_ops.discovery.phase2 import SOURCE_MODULE_NAMESPACE
from dlt_ops.orchestration import RunDecision, filtering_decision, resolve_window, run_source, scheduled_sources

PAGE_VIEWS_SOURCE = """
    import dlt

    @dlt.resource(name="page_views")
    def page_views():
        yield {"id": 1}

    @dlt.resource(name="sessions")
    def sessions():
        yield {"id": 1}

    @dlt.source(name="web_events")
    def web_events_source():
        return [page_views, sessions]
"""

ORDERS_SOURCE = """
    import dlt

    @dlt.resource(name="orders")
    def orders():
        yield {"id": 1}

    @dlt.source(name="orders_api")
    def orders_api_source():
        return orders
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
"""


@pytest.fixture
def project(make_project) -> Path:
    return make_project(
        config=PROJECT_CONFIG,
        files={
            "web/source/web_events.py": PAGE_VIEWS_SOURCE,
            "orders/source/orders_api.py": ORDERS_SOURCE,
            "canary/source/canary_api.py": CANARY_SOURCE,
        },
    )


class TestScheduledSources:
    def test_groups_by_schedule_with_manual_fallback(self, project):
        groups = scheduled_sources(project)

        assert {s.name for s in groups[Schedule.DAILY]} == {"web_events"}
        assert {s.name for s in groups[Schedule.TWO_HOURLY]} == {"orders_api"}
        # canary_api has no config section -> MANUAL
        assert {s.name for s in groups[Schedule.MANUAL]} == {"canary_api"}

    def test_grouping_is_phase1_only(self, project):
        """Parse-time safety: grouping never imports project code."""
        scheduled_sources(project)

        assert not (project / "canary" / "source" / "canary.txt").exists()
        assert f"{SOURCE_MODULE_NAMESPACE}.canary.source.canary_api" not in sys.modules

    def test_static_resources_present(self, project):
        groups = scheduled_sources(project)
        (web_events,) = groups[Schedule.DAILY]
        assert web_events.resources == ("page_views", "sessions")


class TestFilteringDecision:
    KNOWN = ("orders_api", "web_events")

    def test_no_selection_runs_everything(self):
        decision = filtering_decision({}, source_name="web_events", resource="page_views")
        assert decision == RunDecision(run=True)

    def test_empty_source_runs_everything(self):
        assert filtering_decision({"source": ""}, source_name="web_events", resource="page_views").run is True

    def test_other_source_skips(self):
        decision = filtering_decision(
            {"source": "orders_api"}, source_name="web_events", resource="page_views", known_sources=self.KNOWN
        )
        assert decision.run is False
        assert "not selected" in decision.reason

    def test_matching_source_no_resources_runs(self):
        decision = filtering_decision(
            {"source": "web_events"}, source_name="web_events", resource="page_views", known_sources=self.KNOWN
        )
        assert decision.run is True

    def test_resource_in_selection_runs(self):
        selection = {"source": "web_events", "resources": ["sessions"]}
        assert filtering_decision(selection, source_name="web_events", resource="sessions").run is True

    def test_resource_not_in_selection_skips(self):
        selection = {"source": "web_events", "resources": ["sessions"]}
        decision = filtering_decision(selection, source_name="web_events", resource="page_views")
        assert decision.run is False
        assert "not selected" in decision.reason

    def test_unknown_source_raises(self):
        with pytest.raises(ValueError, match="Invalid source"):
            filtering_decision(
                {"source": "nonexistent"}, source_name="web_events", resource=None, known_sources=self.KNOWN
            )

    def test_unknown_source_without_known_sources_skips(self):
        """No known_sources supplied = no typo guard; mismatch is a plain skip."""
        decision = filtering_decision({"source": "nonexistent"}, source_name="web_events", resource=None)
        assert decision.run is False

    def test_whole_source_unit_runs_when_source_selected(self):
        """resource=None (whole-source unit): resource narrowing is the caller's job."""
        selection = {"source": "web_events", "resources": ["sessions"]}
        assert filtering_decision(selection, source_name="web_events", resource=None).run is True

    def test_resources_without_source_ignored(self):
        assert filtering_decision({"resources": ["sessions"]}, source_name="web_events", resource="other").run is True


class TestResolveWindow:
    NATIVE = (pendulum.datetime(2025, 1, 1), pendulum.datetime(2025, 1, 2))

    def test_no_overrides_returns_native(self):
        assert resolve_window({}, native=self.NATIVE) == self.NATIVE

    def test_no_overrides_no_native_returns_none(self):
        assert resolve_window({}) is None

    def test_start_override_keeps_native_end(self):
        window = resolve_window({"start_date": "2024-06-15T00:00:00Z"}, native=self.NATIVE)
        assert window == (pendulum.datetime(2024, 6, 15), self.NATIVE[1])

    def test_end_override_keeps_native_start(self):
        window = resolve_window({"end_date": "2024-12-31T23:59:59Z"}, native=self.NATIVE)
        assert window == (self.NATIVE[0], pendulum.datetime(2024, 12, 31, 23, 59, 59))

    def test_both_overrides(self):
        window = resolve_window(
            {"start_date": "2024-01-01T00:00:00Z", "end_date": "2024-02-01T00:00:00Z"},
            native=self.NATIVE,
        )
        assert window == (pendulum.datetime(2024, 1, 1), pendulum.datetime(2024, 2, 1))

    def test_invalid_start_date_raises(self):
        with pytest.raises(ValueError, match="Invalid start_date format"):
            resolve_window({"start_date": "not-a-date"}, native=self.NATIVE)

    def test_invalid_end_date_raises(self):
        with pytest.raises(ValueError, match="Invalid end_date format"):
            resolve_window({"end_date": "not-a-date"}, native=self.NATIVE)

    def test_date_only_string_parses(self):
        window = resolve_window({"start_date": "2024-01-01", "end_date": "2024-02-01"})
        assert window == (pendulum.datetime(2024, 1, 1), pendulum.datetime(2024, 2, 1))

    def test_partial_override_without_native_raises(self):
        with pytest.raises(ValueError, match="both edges"):
            resolve_window({"start_date": "2024-01-01T00:00:00Z"})


class TestRunSource:
    @pytest.fixture
    def captured(self, monkeypatch):
        calls: dict = {}

        def fake_run_pipeline(source, resources=None, **kwargs):
            calls["source"] = source
            calls["resources"] = resources
            calls["kwargs"] = kwargs
            return "pipeline-sentinel"

        def fake_setup_secrets(sources=None, project_root=None, **kwargs):
            calls["secrets_sources"] = sources
            calls["secrets_project_root"] = project_root

        monkeypatch.setattr(orchestration, "run_pipeline", fake_run_pipeline)
        monkeypatch.setattr(orchestration, "setup_secrets", fake_setup_secrets)
        return calls

    def test_delegates_to_runner_with_bounds(self, project, captured):
        window = (pendulum.datetime(2024, 1, 1), pendulum.datetime(2024, 2, 1))
        result = run_source(
            "web_events",
            project_root=project,
            resources=("page_views",),
            window=window,
            trigger_source="airflow",
        )

        assert result == "pipeline-sentinel"
        assert captured["source"].name == "web_events"
        assert captured["source"].is_introspected is True
        assert captured["resources"] == ("page_views",)
        assert captured["kwargs"]["bounds"] == window
        assert captured["kwargs"]["project_root"] == project
        assert captured["kwargs"]["trigger_source"] == "airflow"

    def test_sets_up_secrets_for_the_one_source(self, project, captured):
        run_source("orders_api", project_root=project, trigger_source="airflow")

        assert set(captured["secrets_sources"]) == {"orders_api"}
        assert captured["secrets_project_root"] == project

    def test_unknown_source_raises_lookup_error(self, project, captured):
        with pytest.raises(LookupError, match="Unknown source 'nope'"):
            run_source("nope", project_root=project, trigger_source="airflow")

    def test_import_failure_raises_runtime_error(self, make_project, captured):
        root = make_project(
            files={
                "broken/source/broken_api.py": """
                    import dlt

                    raise RuntimeError("boom at import")

                    @dlt.source(name="broken_api")
                    def broken_api_source():
                        return []
                """,
            },
        )
        with pytest.raises(RuntimeError, match="failed to import"):
            run_source("broken_api", project_root=root, trigger_source="airflow")
