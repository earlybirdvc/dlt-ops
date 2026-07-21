"""Trace persistence in the runner.

The trace pipeline targets the SAME resolved destination + dataset as the run
(no hardcoded destination), and a trace failure never fails the run.
dlt.pipeline is mocked; resolution rides a tmp-path project (conftest's
make_project) with DuckDB defaults.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from dlt_ops.discovery.runner import run_pipeline
from tests.test_runner import PROJECT_CONFIG, make_source_info

_WORKER_ENV_VARS = ("NORMALIZE__WORKERS", "LOAD__WORKERS", "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS")


@pytest.fixture(autouse=True)
def _restore_worker_env():
    saved = {var: os.environ.get(var) for var in _WORKER_ENV_VARS}
    yield
    for var, value in saved.items():
        if value is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = value


def _mock_main_pipeline(tmp_path) -> MagicMock:
    pipeline = MagicMock()
    pipeline.working_dir = str(tmp_path)
    pipeline.last_trace = MagicMock()
    pipeline.last_trace.asdict.return_value = {"started_at": "2026-01-22"}
    pipeline.last_trace.last_extract_info = None
    pipeline.last_trace.last_normalize_info = None
    pipeline.last_trace.last_load_info = None
    return pipeline


def _mock_source_info():
    """SourceInfo whose source_fn yields a mock source instance (never extracted)."""
    instance = MagicMock()
    instance.resources.keys.return_value = ["resource1"]
    instance.selected_resources = {}
    return make_source_info("test_source", lambda: instance)


def test_trace_targets_resolved_destination_and_dataset(tmp_path, make_project):
    """The _dlt_traces pipeline reuses the run's resolved destination + dataset."""
    root = make_project(config=PROJECT_CONFIG)
    main_pipeline = _mock_main_pipeline(tmp_path)
    trace_pipeline = MagicMock()
    created: list[dict] = []

    def track_pipeline(*args, **kwargs):
        created.append(kwargs)
        return trace_pipeline if kwargs.get("pipeline_name") == "_dlt_traces" else main_pipeline

    with patch("dlt_ops.discovery.runner.dlt.pipeline", side_effect=track_pipeline):
        with patch("dlt_ops.discovery.runner.dlt.resource") as mock_resource:
            run_pipeline(_mock_source_info(), project_root=root)

    by_name = {kwargs["pipeline_name"]: kwargs for kwargs in created}
    assert set(by_name) == {"test_source_pipeline", "_dlt_traces"}
    # Both pipelines target the config-resolved destination + dataset — no
    # hardcoded destination anywhere on the trace path.
    for kwargs in by_name.values():
        assert kwargs["destination"] == "duckdb"
        assert kwargs["dataset_name"] == "analytics"

    assert trace_pipeline.run.called, "Trace pipeline.run should be called"
    call_kwargs = mock_resource.call_args.kwargs
    assert call_kwargs.get("name") == "_dlt_trace"
    assert call_kwargs.get("max_table_nesting") == 0


def test_trace_failure_never_fails_the_run(tmp_path, make_project, caplog):
    root = make_project(config=PROJECT_CONFIG)
    main_pipeline = _mock_main_pipeline(tmp_path)
    trace_pipeline = MagicMock()
    trace_pipeline.run.side_effect = Exception("Trace write failed")

    def track_pipeline(*args, **kwargs):
        return trace_pipeline if kwargs.get("pipeline_name") == "_dlt_traces" else main_pipeline

    with patch("dlt_ops.discovery.runner.dlt.pipeline", side_effect=track_pipeline):
        with patch("dlt_ops.discovery.runner.dlt.resource"):
            result = run_pipeline(_mock_source_info(), project_root=root)

    assert result is main_pipeline
    assert "non-fatal" in caplog.text.lower() or "failed to persist" in caplog.text.lower()
