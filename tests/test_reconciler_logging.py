"""Logging contract for errors the reconciler hands to an alert sink.

When an error goes to ``sink.emit_error``, the sink owns the report. The code
that caught the error logs it once at WARNING, with the exception text in the
message and without a traceback (``exc_info``). A process-wide logging
integration that turns ERROR records with tracebacks into events would
otherwise report the same failure a second time.

Errors that never reach a sink keep their ERROR record with a traceback; the
control test pins one of them.
"""

from __future__ import annotations

import logging
import types
from typing import Any

import attrs
import pytest

from dlt_ops.reconciler import _emission as emission_mod
from dlt_ops.reconciler import additive as additive_mod
from dlt_ops.reconciler import common as common_mod
from dlt_ops.reconciler import removal as removal_mod
from dlt_ops.reconciler.models import DriftKind
from tests.test_reconciler import (
    ORDER_ITEM_LIVE,
    FakeQueryRunner,
    FakeSchemaFetcher,
    OrderItemModel,
    RecordingSink,
    _make_finding,
    _make_source,
    _project_config,
)

RECONCILER_LOGGER = "dlt_ops.reconciler"
SENTRY_LOGGER = "dlt_ops.sentry"


def _assert_one_warning(caplog: pytest.LogCaptureFixture, logger_prefix: str, exc_text: str) -> None:
    """Exactly one record at WARNING or above under ``logger_prefix``: a WARNING carrying the text, no traceback."""
    records = [r for r in caplog.records if r.name.startswith(logger_prefix) and r.levelno >= logging.WARNING]
    assert len(records) == 1, [(r.name, r.levelname, r.getMessage()) for r in records]
    [record] = records
    assert record.levelno == logging.WARNING, record.levelname
    assert record.exc_info is None
    assert exc_text in record.getMessage()


def _detector_kwargs(source: Any, sink: RecordingSink, overrides: dict[str, Any]) -> dict[str, Any]:
    """Injected fakes for a run where only the overridden dependency fails."""
    kwargs: dict[str, Any] = dict(
        dry_run=False,
        fetcher=FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE}),
        runner=FakeQueryRunner(),
        dataset="raw",
        sources={source.name: source},
        project_config=_project_config(),
        sink=sink,
    )
    kwargs.update(overrides)
    return kwargs


def _run_additive(source: Any, *, sink: RecordingSink, **overrides: Any) -> Any:
    return additive_mod.reconcile_source(source.name, **_detector_kwargs(source, sink, overrides))


def _run_removal(source: Any, *, sink: RecordingSink, **overrides: Any) -> Any:
    return removal_mod.detect_removal(source.name, **_detector_kwargs(source, sink, overrides))


DETECTORS = [
    pytest.param(_run_additive, id="additive"),
    pytest.param(_run_removal, id="removal"),
]


class _BoomFetcher:
    def fetch(self, refs):
        raise RuntimeError("schema fetch boom")


# ---------------------------------------------------------------------------
# Shared driver
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("run", "context"),
    [
        pytest.param(_run_additive, "detect_resource_drift", id="additive"),
        pytest.param(_run_removal, "detect_removal", id="removal"),
    ],
)
def test_failing_source_build_logs_one_warning(run, context, caplog):
    """A failing ``source_fn`` passes through ``resource_pydantic_model`` unlogged; the per-resource catch logs it once."""

    def _boom():
        raise RuntimeError("source build boom")

    source = attrs.evolve(_make_source(resources={"order_items": OrderItemModel}), source_fn=_boom)
    sink = RecordingSink()

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        result = run(source, sink=sink)

    assert result.error is None
    assert sink.errors == [("orders_api", "order_items", context)]
    _assert_one_warning(caplog, RECONCILER_LOGGER, "source build boom")


@pytest.mark.parametrize("run", DETECTORS)
def test_discovery_failure_logs_one_warning(run, caplog, monkeypatch, tmp_path):
    def _boom(root):
        raise RuntimeError("discovery boom")

    monkeypatch.setattr(common_mod, "discover_sources", _boom)
    source = _make_source(resources={"order_items": OrderItemModel})
    sink = RecordingSink()

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        result = run(source, sink=sink, sources=None, project_root=tmp_path)

    assert result.error is not None
    assert sink.errors == [("orders_api", None, "discover_sources")]
    _assert_one_warning(caplog, RECONCILER_LOGGER, "discovery boom")


@pytest.mark.parametrize("run", DETECTORS)
def test_open_destination_failure_logs_one_warning(run, caplog, monkeypatch):
    from dlt_ops.reconciler import _adapters

    def _boom(source_name, destination, dataset):
        raise RuntimeError("destination boom")

    monkeypatch.setattr(_adapters, "destination_defaults", _boom)
    source = _make_source(resources={"order_items": OrderItemModel})
    sink = RecordingSink()

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        result = run(
            source,
            sink=sink,
            runner=None,
            fetcher=None,
            project_config=_project_config(default_destination="duckdb"),
        )

    assert result.error is not None
    assert sink.errors == [("orders_api", None, "open_destination")]
    _assert_one_warning(caplog, RECONCILER_LOGGER, "destination boom")


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("run", DETECTORS)
def test_schema_fetch_failure_logs_one_warning(run, caplog):
    source = _make_source(resources={"order_items": OrderItemModel})
    sink = RecordingSink()

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        result = run(source, sink=sink, fetcher=_BoomFetcher())

    assert result.error is not None
    assert sink.errors == [("orders_api", None, "fetch_schemas")]
    _assert_one_warning(caplog, RECONCILER_LOGGER, "schema fetch boom")


@pytest.mark.parametrize(
    ("run", "module", "context"),
    [
        pytest.param(_run_additive, additive_mod, "detect_resource_drift", id="additive"),
        pytest.param(_run_removal, removal_mod, "detect_removal", id="removal"),
    ],
)
def test_per_resource_failure_logs_one_warning(run, module, context, caplog, monkeypatch):
    def _boom(source, resource_name):
        raise RuntimeError("model lookup boom")

    monkeypatch.setattr(module, "resource_pydantic_model", _boom)
    source = _make_source(resources={"order_items": OrderItemModel})
    sink = RecordingSink()

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        result = run(source, sink=sink)

    assert result.error is None
    assert sink.errors == [("orders_api", "order_items", context)]
    _assert_one_warning(caplog, RECONCILER_LOGGER, "model lookup boom")


def test_reconcile_all_discovery_failure_logs_one_warning(caplog, monkeypatch, tmp_path):
    def _boom(root):
        raise RuntimeError("sweep discovery boom")

    monkeypatch.setattr(additive_mod, "discover_sources", _boom)
    sink = RecordingSink()

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        results = additive_mod.reconcile_all(project_root=tmp_path, project_config=_project_config(), sink=sink)

    assert results == []
    assert sink.errors == [("<all>", None, "discover_sources")]
    _assert_one_warning(caplog, RECONCILER_LOGGER, "sweep discovery boom")


# ---------------------------------------------------------------------------
# Emission and the Sentry sink
# ---------------------------------------------------------------------------


class _DriftFailsSink(RecordingSink):
    def emit_drift(self, finding):
        raise RuntimeError("transport down")


def test_emit_drift_failure_logs_one_warning(caplog):
    sink = _DriftFailsSink()

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        emission_mod.emit_findings(sink, [_make_finding(kind=DriftKind.REMOVAL)])

    assert sink.errors == [("orders_api", "order_items", "emit_drift_removal")]
    _assert_one_warning(caplog, RECONCILER_LOGGER, "transport down")


def test_inert_sentry_sink_logs_the_error_as_one_warning(caplog, monkeypatch):
    """With no DSN the Sentry sink only logs; that record is the report, so it is a WARNING without a traceback."""
    import dlt_ops.sentry as sentry_mod

    # The inert path never touches the SDK, so a stand-in module keeps this test in the default lane.
    monkeypatch.setattr(sentry_mod, "_require_sentry_sdk", lambda: types.SimpleNamespace())
    monkeypatch.setattr(sentry_mod, "_dsn_from_secrets", lambda: None)
    sink = sentry_mod.SentryAlertSink()

    with caplog.at_level(logging.DEBUG, logger=SENTRY_LOGGER):
        sink.emit_error(RuntimeError("coverage query boom"), source_name="orders_api", context="detect_removal")

    errors = [r for r in caplog.records if r.name.startswith(SENTRY_LOGGER) and r.levelno >= logging.ERROR]
    assert errors == []
    reports = [
        r for r in caplog.records if r.name.startswith(SENTRY_LOGGER) and "coverage query boom" in r.getMessage()
    ]
    assert len(reports) == 1
    assert reports[0].levelno == logging.WARNING
    assert reports[0].exc_info is None


def test_failing_emit_error_still_logs_with_traceback(caplog):
    """Control: when the sink cannot take the error, the log record is the only report and keeps its traceback."""

    class _AllFailSink(_DriftFailsSink):
        def emit_error(self, exc, *, source_name, resource_name=None, context):
            raise RuntimeError("error transport down")

    with caplog.at_level(logging.DEBUG, logger=RECONCILER_LOGGER):
        emission_mod.emit_findings(_AllFailSink(), [_make_finding()])

    [record] = [r for r in caplog.records if "emit_error itself failed" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None
