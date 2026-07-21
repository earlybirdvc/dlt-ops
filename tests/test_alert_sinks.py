"""Alert-sink plugin axis: config-driven resolution, fan-out isolation, enforcement, Sentry sink.

Covers ``[dlt_ops] alert_sinks`` resolution through the plugin
registry (zero-config default = the core ``logging`` sink), the
``MultiAlertSink`` fan-out with per-sink isolation, the ``validate`` +
Tier-2-preflight enforcement of configured sink names, the flush-at-exit
contract, and — when ``sentry-sdk`` is importable — the Sentry sink's
byte-compatible fingerprint/tag/context scheme against captured event dicts.

The Sentry tests are skipped in the credential-free default lane; run them
once via ``uv run --with sentry-sdk pytest tests/test_alert_sinks.py``.

``RecordingAlertSink`` below doubles as the reference third-party sink: a
distribution registers a class under the ``dlt_ops.alert_sink``
entry-point group; sink resolution constructs it with the project's
``[dlt_ops.alert_sink.<name>]`` options as keyword arguments, and the
instance receives every reconciler event plus a ``flush`` on entry-point exit.
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
import logging
import types
from datetime import UTC, datetime
from textwrap import dedent
from typing import Any, ClassVar

import attrs
import pytest

from dlt_ops.config import DEFAULT_ALERT_SINKS, ProjectConfig, load_project_config
from dlt_ops.discovery.models import ValidationContext
from dlt_ops.discovery.validators.config import validate_alert_sinks
from dlt_ops.plugins import registry as registry_mod
from dlt_ops.preflight import PluginLoadFailedError, PluginNotRegisteredError, check_alert_sinks, run_preflight
from dlt_ops.reconciler import _emission as emission_mod
from dlt_ops.reconciler import additive as additive_mod
from dlt_ops.reconciler import removal as removal_mod
from dlt_ops.reconciler.models import DriftFinding, DriftKind
from tests.test_reconciler import (
    ORDER_ITEM_LIVE,
    FakeQueryRunner,
    FakeSchemaFetcher,
    OrderItemModel,
    _cols,
    _make_source,
    _project_config,
)

_SENTRY_MISSING = importlib.util.find_spec("sentry_sdk") is None
requires_sentry = pytest.mark.skipif(
    _SENTRY_MISSING,
    reason="sentry-sdk not installed — run via `uv run --with sentry-sdk pytest tests/test_alert_sinks.py`",
)

FAKE_DSN = "https://abc123@sentry.invalid/42"


# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


class RecordingAlertSink:
    """Reference third-party sink: entry-point-registered, kwargs-configured.

    A real plugin would ship this class in its own distribution with::

        [project.entry-points."dlt_ops.alert_sink"]
        recording = "acme_alerts:RecordingAlertSink"

    and users enable it with ``[dlt_ops] alert_sinks = ["recording"]``;
    any ``[dlt_ops.alert_sink.recording]`` table arrives as ``options``.
    """

    instances: ClassVar[list["RecordingAlertSink"]] = []

    def __init__(self, **options: Any) -> None:
        self.options = options
        self.drifts: list[DriftFinding] = []
        self.errors: list[tuple[str, str | None, str]] = []
        self.flushes = 0
        RecordingAlertSink.instances.append(self)

    def emit_drift(self, finding: DriftFinding) -> None:
        self.drifts.append(finding)

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None:
        self.errors.append((source_name, resource_name, context))

    def flush(self, timeout: float = 2.0) -> None:
        self.flushes += 1


class BoomAlertSink:
    """Sink whose drift emission always raises — isolation-test subject."""

    def emit_drift(self, finding: DriftFinding) -> None:
        raise RuntimeError("transport down")

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None:
        return None

    def flush(self, timeout: float = 2.0) -> None:
        return None


@pytest.fixture(autouse=True)
def clean_registry():
    """Fresh plugin-registry scan per test — entry-point fakes must not leak."""
    registry_mod._reset_for_tests()
    RecordingAlertSink.instances.clear()
    yield
    registry_mod._reset_for_tests()
    RecordingAlertSink.instances.clear()


@pytest.fixture
def extra_entry_points(monkeypatch: pytest.MonkeyPatch):
    """Overlay fake entry points ON TOP of the real installed metadata.

    Returns an ``add(axis, name, value, dist)`` hook; the real entry points
    (including the package's own ``logging`` sink) stay visible.
    """
    real_entry_points = importlib.metadata.entry_points
    extras: list[importlib.metadata.EntryPoint] = []

    def fake_entry_points(*, group: str) -> tuple[importlib.metadata.EntryPoint, ...]:
        return tuple(real_entry_points(group=group)) + tuple(ep for ep in extras if ep.group == group)

    monkeypatch.setattr(registry_mod, "entry_points", fake_entry_points)

    def add(axis: str, name: str, value: str, dist: str) -> None:
        ep = importlib.metadata.EntryPoint(name=name, value=value, group=f"dlt_ops.{axis}")
        vars(ep).update(dist=types.SimpleNamespace(name=dist))
        extras.append(ep)

    return add


def _make_finding(**overrides: Any) -> DriftFinding:
    base: dict[str, Any] = dict(
        kind=DriftKind.ADDITIVE,
        pipeline_name="orders",
        source_name="orders_api",
        resource_name="order_items",
        columns=("surprise_column",),
        inferred_types=("VARCHAR",),
        sample_values={"surprise_column": ["hello"]},
        first_seen_at=datetime(2026, 7, 3, 12, 0, 0, tzinfo=UTC),
        reproduce_sql='SELECT "surprise_column" FROM "raw"."order_items" LIMIT 5',
    )
    base.update(overrides)
    return DriftFinding(**base)


def _sink_config(*names: str, options: dict[str, dict[str, Any]] | None = None) -> ProjectConfig:
    return attrs.evolve(_project_config(), alert_sinks=tuple(names), alert_sink_options=options or {})


def _validation_ctx(tmp_path, dlt_ops_table: dict[str, Any]) -> ValidationContext:
    return ValidationContext(sources={}, config={"dlt_ops": dlt_ops_table}, project_root=tmp_path)


# ---------------------------------------------------------------------------
# Config keys
# ---------------------------------------------------------------------------


class TestConfigKeys:
    def test_alert_sinks_list_and_per_sink_options_parse(self, tmp_path):
        (tmp_path / ".dlt").mkdir()
        (tmp_path / ".dlt" / "config.toml").write_text(
            dedent(
                """
                [dlt_ops]
                alert_sinks = ["logging", "sentry"]

                [dlt_ops.alert_sink.sentry]
                environment = "prod"
                """
            ),
            encoding="utf-8",
        )
        config = load_project_config(tmp_path)
        assert config.alert_sinks == ("logging", "sentry")
        assert config.alert_sink_options == {"sentry": {"environment": "prod"}}
        assert config.unknown_keys == ()

    def test_unset_key_is_none_and_default_is_logging(self, tmp_path):
        (tmp_path / ".dlt").mkdir()
        (tmp_path / ".dlt" / "config.toml").write_text("[dlt_ops]\n", encoding="utf-8")
        config = load_project_config(tmp_path)
        assert config.alert_sinks is None
        assert config.alert_sink_options == {}
        assert DEFAULT_ALERT_SINKS == ("logging",)


# ---------------------------------------------------------------------------
# Resolution + fan-out
# ---------------------------------------------------------------------------


class TestSinkResolution:
    def test_zero_config_default_is_core_logging_sink(self, caplog):
        """No config at all: the core `logging` sink resolves and emits."""
        resolved = emission_mod.resolve_sink(None, dry_run=False, project_config=ProjectConfig())
        assert isinstance(resolved, emission_mod.MultiAlertSink)
        assert [(name, type(sink)) for name, sink in resolved.sinks] == [
            ("logging", emission_mod.LoggingAlertSink),
        ]
        with caplog.at_level(logging.WARNING, logger=emission_mod.logger.name):
            emission_mod.emit_findings(resolved, [_make_finding()])
        assert any("orders.orders_api.order_items" in record.getMessage() for record in caplog.records)

    def test_entry_point_registered_sink_constructed_with_options(self, extra_entry_points):
        extra_entry_points("alert_sink", "recording", "tests.test_alert_sinks:RecordingAlertSink", "acme-alerts")
        config = _sink_config("recording", options={"recording": {"environment": "prod"}})

        resolved = emission_mod.resolve_sink(None, dry_run=False, project_config=config)
        finding = _make_finding()
        emission_mod.emit_findings(resolved, [finding])

        [instance] = RecordingAlertSink.instances
        assert instance.options == {"environment": "prod"}
        assert instance.drifts == [finding]

    def test_all_configured_sinks_receive_every_event(self, extra_entry_points, caplog):
        extra_entry_points("alert_sink", "recording", "tests.test_alert_sinks:RecordingAlertSink", "acme-alerts")
        config = _sink_config("logging", "recording")

        resolved = emission_mod.resolve_sink(None, dry_run=False, project_config=config)
        finding = _make_finding()
        with caplog.at_level(logging.WARNING, logger=emission_mod.logger.name):
            emission_mod.emit_findings(resolved, [finding])
        resolved.emit_error(RuntimeError("boom"), source_name="orders_api", context="fetch_schemas")

        [instance] = RecordingAlertSink.instances
        assert instance.drifts == [finding]
        assert instance.errors == [("orders_api", None, "fetch_schemas")]
        assert any("schema drift" in record.getMessage() for record in caplog.records)

    def test_unresolvable_configured_sinks_fall_back_to_logging(self, caplog):
        with caplog.at_level(logging.ERROR, logger=emission_mod.logger.name):
            resolved = emission_mod.resolve_sink(None, dry_run=False, project_config=_sink_config("ghost"))
        assert [(name, type(sink)) for name, sink in resolved.sinks] == [
            ("logging", emission_mod.LoggingAlertSink),
        ]
        assert any("falling back to the core logging sink" in record.getMessage() for record in caplog.records)

    def test_explicitly_empty_list_disables_emission(self):
        resolved = emission_mod.resolve_sink(None, dry_run=False, project_config=_sink_config())
        assert isinstance(resolved, emission_mod.MultiAlertSink)
        assert resolved.sinks == ()

    def test_dry_run_suppresses_all_configured_sinks(self, extra_entry_points):
        extra_entry_points("alert_sink", "recording", "tests.test_alert_sinks:RecordingAlertSink", "acme-alerts")
        resolved = emission_mod.resolve_sink(None, dry_run=True, project_config=_sink_config("recording"))
        assert isinstance(resolved, emission_mod.NullAlertSink)
        assert RecordingAlertSink.instances == []


class TestFanOutIsolation:
    def test_raising_sink_is_isolated_and_reported(self, extra_entry_points):
        """A raising sink never blocks the others; the failure is reported
        through the fan-out's own error path under the detector context."""
        extra_entry_points("alert_sink", "boom", "tests.test_alert_sinks:BoomAlertSink", "acme-boom")
        extra_entry_points("alert_sink", "recording", "tests.test_alert_sinks:RecordingAlertSink", "acme-alerts")
        resolved = emission_mod.resolve_sink(None, dry_run=False, project_config=_sink_config("boom", "recording"))

        finding = _make_finding()
        emission_mod.emit_findings(resolved, [finding])

        [instance] = RecordingAlertSink.instances
        assert instance.drifts == [finding]
        assert instance.errors == [("orders_api", "order_items", "emit_drift")]

    def test_reconcile_result_unaffected_by_raising_sink(self, extra_entry_points):
        extra_entry_points("alert_sink", "boom", "tests.test_alert_sinks:BoomAlertSink", "acme-boom")
        extra_entry_points("alert_sink", "recording", "tests.test_alert_sinks:RecordingAlertSink", "acme-alerts")
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE + _cols("surprise_column")})

        result = additive_mod.reconcile_source(
            "orders_api",
            dry_run=False,
            fetcher=fetcher,
            runner=FakeQueryRunner(sample_rows=[("v",)]),
            dataset="raw",
            sources={"orders_api": source},
            project_config=_sink_config("boom", "recording"),
        )

        assert result.error is None
        assert [f.columns for f in result.findings] == [("surprise_column",)]
        [instance] = RecordingAlertSink.instances
        assert instance.drifts == list(result.findings)
        assert instance.flushes == 1

    def test_emit_error_and_flush_never_raise(self):
        class TerminalBoom:
            def emit_drift(self, finding):
                raise RuntimeError("boom")

            def emit_error(self, exc, *, source_name, resource_name=None, context):
                raise RuntimeError("boom")

            def flush(self, timeout=2.0):
                raise RuntimeError("boom")

        recording = RecordingAlertSink()
        multi = emission_mod.MultiAlertSink([("boom", TerminalBoom()), ("recording", recording)])

        multi.emit_error(RuntimeError("original"), source_name="orders_api", context="fetch_schemas")
        multi.flush()

        assert recording.errors == [("orders_api", None, "fetch_schemas")]
        assert recording.flushes == 1


class TestFlushAtEntryPointExit:
    def test_reconcile_source_flushes_config_resolved_sinks(self, extra_entry_points):
        extra_entry_points("alert_sink", "recording", "tests.test_alert_sinks:RecordingAlertSink", "acme-alerts")

        result = additive_mod.reconcile_source(
            "missing_source",
            dry_run=False,
            sources={},
            project_config=_sink_config("recording"),
        )

        assert result.error is not None
        [instance] = RecordingAlertSink.instances
        assert instance.flushes == 1

    def test_detect_removal_flushes_config_resolved_sinks(self, extra_entry_points):
        extra_entry_points("alert_sink", "recording", "tests.test_alert_sinks:RecordingAlertSink", "acme-alerts")

        result = removal_mod.detect_removal(
            "missing_source",
            dry_run=False,
            sources={},
            project_config=_sink_config("recording"),
        )

        assert result.error is not None
        [instance] = RecordingAlertSink.instances
        assert instance.flushes == 1


# ---------------------------------------------------------------------------
# Enforcement: validate + Tier-2 preflight
# ---------------------------------------------------------------------------


class TestEnforcement:
    def test_validate_passes_when_key_unset_or_sinks_registered(self, tmp_path):
        assert validate_alert_sinks(_validation_ctx(tmp_path, {})) == []
        assert validate_alert_sinks(_validation_ctx(tmp_path, {"alert_sinks": ["logging"]})) == []

    def test_validate_flags_unregistered_sink(self, tmp_path):
        [error] = validate_alert_sinks(_validation_ctx(tmp_path, {"alert_sinks": ["ghost"]}))
        assert "'ghost'" in error.message
        assert "plugins doctor" in error.message

    def test_validate_flags_sink_that_fails_to_load(self, tmp_path, extra_entry_points):
        extra_entry_points("alert_sink", "broken", "totally_absent_module_xyz:Sink", "acme-broken")
        [error] = validate_alert_sinks(_validation_ctx(tmp_path, {"alert_sinks": ["broken"]}))
        assert "failed to load" in error.message

    def test_validate_flags_malformed_key(self, tmp_path):
        [error] = validate_alert_sinks(_validation_ctx(tmp_path, {"alert_sinks": "logging"}))
        assert "list of sink-name strings" in error.message

    def test_preflight_passes_when_unset_or_registered(self):
        check_alert_sinks(ProjectConfig())
        check_alert_sinks(ProjectConfig(alert_sinks=("logging",)))

    def test_preflight_fails_on_unregistered_sink(self):
        with pytest.raises(PluginNotRegisteredError, match="ghost"):
            check_alert_sinks(ProjectConfig(alert_sinks=("ghost",)))

    def test_preflight_fails_on_sink_that_fails_to_load(self, extra_entry_points):
        extra_entry_points("alert_sink", "broken", "totally_absent_module_xyz:Sink", "acme-broken")
        with pytest.raises(PluginLoadFailedError, match="broken"):
            check_alert_sinks(ProjectConfig(alert_sinks=("broken",)))
        # The soft-fail record lands in the registry for `plugins doctor`.
        assert any(f.axis == "alert_sink" and f.name == "broken" for f in registry_mod.failures())

    def test_run_preflight_wires_the_alert_sink_check(self):
        with pytest.raises(PluginNotRegisteredError, match="ghost"):
            run_preflight(destination="duckdb", project_config=ProjectConfig(alert_sinks=("ghost",)))

    @pytest.mark.skipif(not _SENTRY_MISSING, reason="sentry-sdk installed — the sentry sink constructs in this lane")
    def test_configured_sentry_without_extra_fails_both_tiers(self, tmp_path):
        """The `sentry` entry point ships in core metadata and its module
        imports without the extra (so `plugins doctor` stays healthy), but
        constructing the configured sink raises — both tiers surface it."""
        [error] = validate_alert_sinks(_validation_ctx(tmp_path, {"alert_sinks": ["sentry"]}))
        assert "failed to load" in error.message
        assert "[sentry] extra" in error.message
        with pytest.raises(PluginLoadFailedError, match=r"requires the \[sentry\] extra"):
            check_alert_sinks(ProjectConfig(alert_sinks=("sentry",)))


# ---------------------------------------------------------------------------
# Sentry sink (run via `uv run --with sentry-sdk pytest tests/test_alert_sinks.py`)
# ---------------------------------------------------------------------------


@requires_sentry
class TestSentrySink:
    @pytest.fixture
    def captured_events(self, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
        """Route the sink's isolated Client through a capturing transport.

        The sink's own construction path runs (isolated `sentry_sdk.Client`,
        `default_integrations=False`); only the network transport is replaced
        by a function collecting event dicts.
        """
        import sentry_sdk

        import dlt_ops.sentry as sentry_mod

        events: list[dict[str, Any]] = []
        real_client = sentry_sdk.Client

        def capturing_client(**kwargs: Any) -> Any:
            return real_client(transport=events.append, **kwargs)

        monkeypatch.setattr(sentry_sdk, "Client", capturing_client)
        monkeypatch.setattr(sentry_mod, "_dsn_from_secrets", lambda: FAKE_DSN)
        return events

    def test_dsn_read_through_dlt_secrets_key(self):
        """The DSN flows through dlt.secrets under [alert_sinks.sentry] only."""
        import dlt

        import dlt_ops.sentry as sentry_mod

        dlt.secrets["alert_sinks.sentry.dsn"] = FAKE_DSN
        assert sentry_mod._dsn_from_secrets() == FAKE_DSN

    def test_dsn_unset_sink_inert_with_warning(self, monkeypatch, caplog):
        import dlt_ops.sentry as sentry_mod

        monkeypatch.setattr(sentry_mod, "_dsn_from_secrets", lambda: None)
        sink = sentry_mod.SentryAlertSink()
        with caplog.at_level(logging.INFO, logger=sentry_mod.logger.name):
            sink.emit_drift(_make_finding())
            sink.emit_error(RuntimeError("boom"), source_name="orders_api", context="probe")
            sink.flush()
        assert sink._client is None
        assert any(
            "Sentry sink inert" in record.getMessage() and record.levelno == logging.WARNING
            for record in caplog.records
        )
        assert any("drift finding" in record.getMessage() for record in caplog.records)

    def test_drift_event_fingerprint_tags_context_byte_compatible(self, captured_events):
        from dlt_ops.sentry import SentryAlertSink

        sink = SentryAlertSink(environment="staging")
        sink.emit_drift(_make_finding())
        sink.flush()

        [event] = captured_events
        assert event["fingerprint"] == ["schema-drift", "orders", "orders_api", "order_items"]
        assert event["tags"] == {
            "pipeline": "orders",
            "source": "orders_api",
            "resource": "order_items",
            "drift_type": "additive",
        }
        assert event["contexts"]["schema_drift"] == {
            "columns": ["surprise_column"],
            "inferred_types": ["VARCHAR"],
            "first_seen_at": "2026-07-03T12:00:00+00:00",
            "sample_values": {"surprise_column": ["hello"]},
            "reproduce_sql": 'SELECT "surprise_column" FROM "raw"."order_items" LIMIT 5',
        }
        assert event["level"] == "warning"
        assert event["message"] == (
            "Schema drift (additive): orders.orders_api.order_items — 1 column(s): surprise_column"
        )
        assert event["environment"] == "staging"

    def test_removal_drift_shares_fingerprint_with_drift_type_tag(self, captured_events):
        from dlt_ops.sentry import SentryAlertSink

        sink = SentryAlertSink()
        sink.emit_drift(_make_finding(kind=DriftKind.REMOVAL))
        sink.flush()

        [event] = captured_events
        # Additive + removal collapse into one Issue by design; the tag
        # disambiguates.
        assert event["fingerprint"] == ["schema-drift", "orders", "orders_api", "order_items"]
        assert event["tags"]["drift_type"] == "removal"

    def test_error_event_fingerprint_tags_context_byte_compatible(self, captured_events):
        from dlt_ops.sentry import SentryAlertSink

        sink = SentryAlertSink()
        sink.emit_error(RuntimeError("boom"), source_name="orders_api", context="fetch_schemas")
        sink.emit_error(
            ValueError("bad model"),
            source_name="orders_api",
            resource_name="order_items",
            context="detect_resource_drift",
        )
        sink.flush()

        source_level, resource_level = captured_events
        assert source_level["fingerprint"] == [
            "schema-drift-reconciler-error",
            "orders_api",
            "<source-level>",
            "fetch_schemas",
        ]
        assert source_level["tags"] == {"source": "orders_api", "reconciler_context": "fetch_schemas"}
        assert source_level["contexts"]["reconciler_error"] == {
            "context": "fetch_schemas",
            "resource": None,
            "exception_type": "RuntimeError",
            "exception_message": "boom",
        }
        assert source_level["level"] == "error"
        assert source_level["exception"]["values"][0]["type"] == "RuntimeError"

        assert resource_level["fingerprint"] == [
            "schema-drift-reconciler-error",
            "orders_api",
            "order_items",
            "detect_resource_drift",
        ]
        assert resource_level["tags"]["resource"] == "order_items"

    def test_logging_and_sentry_both_receive_findings(self, captured_events, caplog):
        """`alert_sinks = ["logging", "sentry"]` end-to-end through the real
        registry entry points, with the Sentry transport mocked."""
        config = _sink_config("logging", "sentry", options={"sentry": {"environment": "test"}})
        resolved = emission_mod.resolve_sink(None, dry_run=False, project_config=config)
        assert [name for name, _ in resolved.sinks] == ["logging", "sentry"]

        with caplog.at_level(logging.WARNING, logger=emission_mod.logger.name):
            emission_mod.emit_findings(resolved, [_make_finding()])
        resolved.flush()

        assert any("schema drift" in record.getMessage() for record in caplog.records)
        [event] = captured_events
        assert event["fingerprint"] == ["schema-drift", "orders", "orders_api", "order_items"]
        assert event["environment"] == "test"
