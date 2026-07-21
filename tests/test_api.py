"""Tests for the public API surface and the discovery API."""

import json
import subprocess
import sys

import pytest

import dlt_ops
from dlt_ops import Schedule
from dlt_ops.discovery.api import get_source_resources, get_source_schedules

PUBLIC_API = [
    "AlertSink",
    "AssertionContext",
    "AssertionFailedError",
    "AssertionType",
    "DestinationAdapter",
    "DriftFinding",
    "ReconcileResult",
    "RuleSpec",
    "Schedule",
    "SecretBackend",
    "SourceConfig",
    "SourceInfo",
    "ValidationContext",
    "ValidationError",
    "Validator",
    "cleanup_checkpoints",
    "detect_removal",
    "discover_sources",
    "drop_unknown_nulls",
    "extract_model_column_names",
    "list_checkpoints",
    "reconcile_all",
    "reconcile_source",
    "register",
    "validate_sources",
    "with_checkpoints",
]

LAZY_RECONCILER_NAMES = [
    "DriftFinding",
    "ReconcileResult",
    "detect_removal",
    "reconcile_all",
    "reconcile_source",
]

EAGER_NAMES = [name for name in PUBLIC_API if name not in LAZY_RECONCILER_NAMES]


class TestPublicApiSurface:
    def test_all_matches_documented_surface(self):
        assert dlt_ops.__all__ == PUBLIC_API

    @pytest.mark.parametrize("name", EAGER_NAMES)
    def test_eager_names_importable(self, name):
        assert getattr(dlt_ops, name) is not None

    @pytest.mark.parametrize("name", LAZY_RECONCILER_NAMES)
    def test_lazy_reconciler_names_listed_in_dir(self, name):
        assert name in dir(dlt_ops)

    @pytest.mark.parametrize("name", LAZY_RECONCILER_NAMES)
    def test_lazy_reconciler_names_resolve(self, name):
        """The reconciler runs on the DestinationAdapter boundary.

        The names stay lazy so `import dlt_ops` keeps its import-time
        budget, but resolving them now returns real objects — and pulls no
        alerting or warehouse SDK into the process.
        """
        assert getattr(dlt_ops, name) is not None
        forbidden = ("sentry_sdk", "google.cloud")
        offenders = [
            module for module in sys.modules if any(module == pkg or module.startswith(f"{pkg}.") for pkg in forbidden)
        ]
        assert offenders == []

    def test_unknown_attribute_raises_attribute_error(self):
        with pytest.raises(AttributeError, match="not_a_public_name"):
            _ = dlt_ops.not_a_public_name

    def test_import_pulls_no_heavy_dependencies(self):
        """`import dlt_ops` must not import EE/orchestrator/cloud modules."""
        code = "import json, sys, dlt_ops; print(json.dumps(sorted(sys.modules)))"
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        loaded = json.loads(proc.stdout)
        forbidden = ("airflow", "sentry_sdk", "google.cloud")
        offenders = [
            module for module in loaded if any(module == pkg or module.startswith(f"{pkg}.") for pkg in forbidden)
        ]
        assert offenders == []


DEMO_PROJECT_CONFIG = """
    [dlt_ops]
    default_destination = "duckdb"

    [sources.events_api.dlt_ops]
    schedule = "@daily"

    [sources.orders_api.dlt_ops]
    schedule = "@weekly"
"""

DEMO_PROJECT_FILES = {
    "events/source/events_api.py": """
        import dlt

        @dlt.resource(name="events")
        def events():
            yield {"id": 1}

        @dlt.source(name="events_api")
        def events_api_source():
            return events
        """,
    "orders/source/orders_api.py": """
        import dlt

        @dlt.resource(name="orders")
        def orders():
            yield {"id": 1}

        @dlt.source(name="orders_api")
        def orders_api_source():
            return orders
        """,
}


@pytest.fixture
def demo_project_cwd(make_project, monkeypatch):
    """A neutral project tree with cwd inside it — the no-arg discovery API
    resolves the root by walking up from cwd."""
    root = make_project(config=DEMO_PROJECT_CONFIG, files=DEMO_PROJECT_FILES)
    nested = root / "events" / "source"
    monkeypatch.chdir(nested)
    return root


def test_get_source_schedules_returns_dict(demo_project_cwd):
    """get_source_schedules returns {source_name: schedule_value} dict."""
    result = get_source_schedules()
    assert result == {"events_api": "@daily", "orders_api": "@weekly"}


def test_get_source_resources_returns_dict(demo_project_cwd):
    """get_source_resources returns {source_name: (resources...)} dict."""
    result = get_source_resources()
    assert result == {"events_api": ("events",), "orders_api": ("orders",)}


@pytest.mark.integration
def test_get_source_schedules_integration(demo_project_cwd):
    """Integration: every discovered schedule is a valid Schedule value."""
    schedules = get_source_schedules()
    valid_schedules = {s.value for s in Schedule}
    assert len(schedules) > 0, "No configured sources found"
    for name, sched in schedules.items():
        assert sched in valid_schedules, f"{name} has invalid schedule: {sched}"


@pytest.mark.integration
def test_get_source_resources_integration(demo_project_cwd):
    """Integration: resources are non-empty tuples."""
    resources = get_source_resources()
    assert len(resources) > 0, "No sources found"
    for name, res in resources.items():
        assert isinstance(res, tuple), f"{name} resources should be tuple"
        assert len(res) > 0, f"{name} should have at least one resource"
