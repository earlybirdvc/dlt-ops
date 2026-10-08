"""Tests for the run-window helpers in ``dlt_ops.incremental``."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any

import dlt
import pendulum
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_ops import WINDOW_CURSOR_PATH, resolve_incremental_window
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.schema_contracts import CANONICAL_SCHEMA_CONTRACT
from tests.test_runner import PROJECT_CONFIG, _query, _table_columns, make_source_info

_COLUMNS = {
    "id": {"data_type": "bigint", "nullable": False},
    "name": {"data_type": "text", "nullable": True},
}
_EPOCH = pendulum.datetime(1970, 1, 1, tz="UTC")
_START = pendulum.datetime(2024, 2, 1, tz="UTC")
_END = pendulum.datetime(2024, 3, 1, tz="UTC")
_ROWS = [{"id": 1, "name": "a"}, {"id": 2, "name": None}, {"id": 3, "name": "c"}]


@pytest.fixture(autouse=True)
def _isolate_run_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DLT_INTERVAL_START", raising=False)
    monkeypatch.delenv("DLT_INTERVAL_END", raising=False)


def _frozen_pipeline(name: str) -> Any:
    """Create the table first: dlt lets the first load of a new table set its columns."""
    pipeline = dlt.pipeline(pipeline_name=name, destination="duckdb", dataset_name="analytics")

    @dlt.resource(name="events", columns=_COLUMNS, schema_contract=CANONICAL_SCHEMA_CONTRACT)
    def seed():
        yield [{"id": 0, "name": "seed"}]

    pipeline.run(seed())
    return pipeline


def _cursor_resource(cursor_path: str):
    @dlt.resource(name="events", columns=_COLUMNS, schema_contract=CANONICAL_SCHEMA_CONTRACT)
    def events(
        cursor=dlt.sources.incremental(cursor_path, initial_value=_EPOCH, on_cursor_value_missing="include"),
    ):
        yield _ROWS

    return events


def _count(pipeline: Any) -> int:
    return _query(pipeline, "SELECT count(*) FROM analytics.events")[0][0]


class TestWindowCursor:
    def test_frozen_table_accepts_rows_and_gains_no_column(self):
        pipeline = _frozen_pipeline("window_frozen")
        pipeline.run(_cursor_resource(WINDOW_CURSOR_PATH)())
        assert "_window_cursor" not in _table_columns(pipeline, "analytics", "events")
        assert _count(pipeline) == 1 + len(_ROWS)

    def test_binds_the_external_interval(self, monkeypatch):
        monkeypatch.setenv("DLT_INTERVAL_START", "2024-02-01T00:00:00Z")
        monkeypatch.setenv("DLT_INTERVAL_END", "2024-03-01T00:00:00Z")
        seen: list[tuple[Any, Any]] = []

        @dlt.resource(name="events")
        def events(
            cursor=dlt.sources.incremental(
                WINDOW_CURSOR_PATH,
                initial_value=_EPOCH,
                on_cursor_value_missing="include",
                allow_external_schedulers=True,
            ),
        ):
            seen.append((cursor.start_value, cursor.end_value))
            yield _ROWS

        pipeline = dlt.pipeline(pipeline_name="window_interval", destination="duckdb", dataset_name="analytics")
        pipeline.extract(events())
        assert seen == [(_START, _END)]

    def test_bare_cursor_name_fails_the_frozen_table(self):
        """Control: the bare name adds a column, which the freeze contract rejects.

        If this test fails on a future dlt because the run no longer raises,
        the JSONPath spelling of ``WINDOW_CURSOR_PATH`` may no longer be needed.
        """
        pipeline = _frozen_pipeline("bare_frozen")
        with pytest.raises(PipelineStepFailed, match=r"_window_cursor.*frozen"):
            pipeline.run(_cursor_resource("_window_cursor")())


def _cursor(start_value: Any = None, end_value: Any = None) -> Any:
    return SimpleNamespace(start_value=start_value, end_value=end_value)


class TestResolveIncrementalWindow:
    def test_interval_wins_over_config_bounds(self):
        window = resolve_incremental_window(_cursor(_START, _END), "2023-01-01T00:00:00Z", "2023-02-01T00:00:00Z")
        assert window == (_START, _END)

    def test_config_bounds_with_end(self):
        window = resolve_incremental_window(_cursor(), "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z")
        assert window == (_START, _END)

    def test_config_start_without_end_is_open(self):
        assert resolve_incremental_window(_cursor(), "2024-02-01T00:00:00Z", None) == (_START, None)

    def test_config_start_wins_over_cursor_start(self):
        window = resolve_incremental_window(_cursor(start_value=_EPOCH), "2024-02-01T00:00:00Z", None)
        assert window == (_START, None)

    def test_cursor_start_fallback(self):
        assert resolve_incremental_window(_cursor(start_value=_START), None, None) == (_START, None)

    def test_string_cursor_start_is_parsed(self):
        since, until = resolve_incremental_window(_cursor(start_value="2024-02-01T00:00:00Z"), None, None)
        assert isinstance(since, dt.datetime)
        assert since == _START
        assert until is None

    def test_nothing_available_raises(self):
        with pytest.raises(ValueError, match="No incremental window available"):
            resolve_incremental_window(_cursor(), None, None)

    def test_duration_config_start_is_rejected(self):
        with pytest.raises(ValueError, match="incremental_start_value"):
            resolve_incremental_window(_cursor(), "P1D", None)

    def test_unparseable_config_start_is_rejected(self):
        with pytest.raises(ValueError, match="incremental_start_value"):
            resolve_incremental_window(_cursor(), "not-a-date", None)

    def test_lookback_moves_interval_start_only(self):
        window = resolve_incremental_window(_cursor(_START, _END), None, None, lookback_hours=2)
        assert window == (_START - dt.timedelta(hours=2), _END)

    def test_lookback_moves_cursor_start(self):
        window = resolve_incremental_window(_cursor(start_value=_START), None, None, lookback_hours=2)
        assert window == (_START - dt.timedelta(hours=2), None)

    @pytest.mark.parametrize("lookback_hours", [0, -1])
    def test_non_positive_lookback_changes_nothing(self, lookback_hours):
        window = resolve_incremental_window(_cursor(_START, _END), None, None, lookback_hours=lookback_hours)
        assert window == (_START, _END)

    def test_config_end_without_start_is_ignored(self):
        window = resolve_incremental_window(_cursor(start_value=_EPOCH), None, "2024-03-01T00:00:00Z")
        assert window == (_EPOCH, None)

    def test_string_cursor_bounds_are_parsed(self):
        since, until = resolve_incremental_window(_cursor("2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"))
        assert isinstance(since, dt.datetime)
        assert isinstance(until, dt.datetime)
        assert (since, until) == (_START, _END)

    def test_lookback_moves_string_cursor_start(self):
        window = resolve_incremental_window(_cursor("2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"), lookback_hours=2)
        assert window == (_START - dt.timedelta(hours=2), _END)

    def test_numeric_cursor_bound_is_rejected(self):
        with pytest.raises(TypeError, match="start_value"):
            resolve_incremental_window(_cursor(100, 200))

    def test_zero_cursor_end_still_counts_as_bounded(self):
        with pytest.raises(TypeError, match="start_value"):
            resolve_incremental_window(_cursor(-1, 0), "2024-02-01T00:00:00Z")


def _window_source(name: str, windows: list[tuple[Any, Any]], lookback_hours: int):
    @dlt.resource(name="events")
    def events(
        cursor=dlt.sources.incremental(WINDOW_CURSOR_PATH, initial_value=_EPOCH, on_cursor_value_missing="include"),
    ):
        windows.append(resolve_incremental_window(cursor, None, None, lookback_hours=lookback_hours))
        yield _ROWS

    return dlt.source(lambda: events, name=name)()


class TestRunnerWindow:
    def test_bounds_reach_the_window(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        windows: list[tuple[Any, Any]] = []
        info = make_source_info("window_bounded", lambda: _window_source("window_bounded", windows, 2))
        pipeline = run_pipeline(info, project_root=root, bounds=(_START, _END))
        assert windows == [(_START - dt.timedelta(hours=2), _END)]
        assert "_window_cursor" not in _table_columns(pipeline, "analytics", "events")
        assert _count(pipeline) == len(_ROWS)

    def test_plain_run_uses_initial_value(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        windows: list[tuple[Any, Any]] = []
        info = make_source_info("window_plain", lambda: _window_source("window_plain", windows, 0))
        run_pipeline(info, project_root=root)
        assert windows == [(_EPOCH, None)]
