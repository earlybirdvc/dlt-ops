"""Tests for the `init` scaffold verb.

Covers: layout production (marker + pipeline dirs), marker semantics
(generated root accepted by find_project_root/discover, empty project passes
validate), overwrite refusal (byte-identical config after a failed re-run,
no --force), and the --example fixture source end-to-end (list, validate, run
into local DuckDB via the real CLI).
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from dlt_ops.cli._init_templates import (
    DEFAULT_PIPELINE_NAME,
    EXAMPLE_DATASET,
    EXAMPLE_RESOURCE_MODULE,
    EXAMPLE_ROW_COUNT,
    EXAMPLE_SOURCE_SECTION,
)
from dlt_ops.cli.cli import cli
from dlt_ops.config import PROJECT_MARKER, RESOURCE_DIR, SOURCE_DIR, find_project_root
from dlt_ops.discovery import discover

_WORKER_ENV_VARS = ("NORMALIZE__WORKERS", "LOAD__WORKERS", "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS")


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _init(runner: CliRunner, root: Path, *args: str):
    result = runner.invoke(cli, ["init", str(root), *args])
    assert result.exit_code == 0, result.output
    return result


class TestScaffoldLayout:
    def test_creates_marker_secrets_and_pipeline_dirs(self, runner, tmp_path):
        root = tmp_path / "demo"
        _init(runner, root)

        marker = root / PROJECT_MARKER
        data = tomllib.loads(marker.read_text())
        table = data["dlt_ops"]
        # Real (uncommented) duckdb default so the scaffold never fails its
        # own first validate/run; the dataset default stays a commented hint.
        assert table["default_destination"] == "duckdb"
        assert "default_dataset" not in table
        assert "[sources.my_api]" in marker.read_text()  # commented worked example

        assert (root / PROJECT_MARKER.parent / "secrets.toml").is_file()
        assert (root / DEFAULT_PIPELINE_NAME / SOURCE_DIR / ".gitkeep").is_file()
        assert (root / DEFAULT_PIPELINE_NAME / RESOURCE_DIR / ".gitkeep").is_file()

    def test_root_defaults_to_cwd(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(cli, ["init"])
        assert result.exit_code == 0, result.output
        assert (tmp_path / PROJECT_MARKER).is_file()

    def test_custom_pipeline_name(self, runner, tmp_path):
        root = tmp_path / "demo"
        _init(runner, root, "--pipeline", "web_events")
        assert (root / "web_events" / SOURCE_DIR).is_dir()
        assert (root / "web_events" / RESOURCE_DIR).is_dir()
        assert not (root / DEFAULT_PIPELINE_NAME).exists()

    @pytest.mark.parametrize("bad_name", ["_hidden", ".dotted", "has space", "common"])
    def test_rejects_pipeline_names_discovery_would_skip(self, runner, tmp_path, bad_name):
        root = tmp_path / "demo"
        result = runner.invoke(cli, ["init", str(root), "--pipeline", bad_name])
        assert result.exit_code == 2
        assert not root.exists()  # nothing scaffolded on a rejected name

    def test_help_offers_no_force(self, runner):
        result = runner.invoke(cli, ["init", "--help"])
        assert result.exit_code == 0, result.output
        assert "--force" not in result.output
        assert "--example" in result.output

    def test_next_steps_hint(self, runner, tmp_path):
        result = _init(runner, tmp_path / "demo")
        assert "pipeline validate" in result.output
        assert f"{SOURCE_DIR}/<section>.py" in result.output


class TestMarkerSemantics:
    def test_generated_root_detected_from_subdirectory(self, runner, tmp_path):
        root = tmp_path / "demo"
        _init(runner, root)
        assert find_project_root(start=root / DEFAULT_PIPELINE_NAME / SOURCE_DIR) == root

    def test_empty_project_discovers_zero_sources(self, runner, tmp_path):
        root = tmp_path / "demo"
        _init(runner, root)
        assert discover(root) == {}

    def test_init_then_validate_exits_zero(self, runner, tmp_path, monkeypatch):
        """The acceptance path: `init demo && cd demo && pipeline validate`."""
        root = tmp_path / "demo"
        _init(runner, root)
        monkeypatch.chdir(root)
        result = runner.invoke(cli, ["pipeline", "validate"])
        assert result.exit_code == 0, result.output
        assert "validated successfully" in result.output


class TestOverwriteRefusal:
    def test_rerun_fails_loudly_and_config_is_byte_identical(self, runner, tmp_path):
        root = tmp_path / "demo"
        _init(runner, root)
        marker = root / PROJECT_MARKER
        before = marker.read_bytes()

        result = runner.invoke(cli, ["init", str(root)])
        assert result.exit_code == 1
        assert "already exists" in result.output
        assert str(PROJECT_MARKER) in result.output
        assert marker.read_bytes() == before

    def test_refuses_any_existing_config_even_without_marker_table(self, runner, tmp_path):
        """A .dlt/config.toml without [dlt_ops] is still user config —
        never touched, and nothing else gets scaffolded around it."""
        root = tmp_path / "demo"
        marker = root / PROJECT_MARKER
        marker.parent.mkdir(parents=True)
        marker.write_text("# hand-written dlt config, not a dlt-ops marker\n")
        before = marker.read_bytes()

        result = runner.invoke(cli, ["init", str(root)])
        assert result.exit_code == 1
        assert marker.read_bytes() == before
        assert not (root / DEFAULT_PIPELINE_NAME).exists()

    def test_existing_pipeline_dirs_are_fine(self, runner, tmp_path):
        root = tmp_path / "demo"
        keep = root / DEFAULT_PIPELINE_NAME / SOURCE_DIR / "keep.py"
        keep.parent.mkdir(parents=True)
        keep.write_text("# user file\n")
        _init(runner, root)
        assert keep.read_text() == "# user file\n"
        assert (root / PROJECT_MARKER).is_file()


class TestExampleSource:
    @pytest.fixture
    def example_root(self, runner, tmp_path) -> Path:
        root = tmp_path / "demo"
        _init(runner, root, "--example")
        return root

    @pytest.fixture
    def _isolated_run_env(self, tmp_path, monkeypatch):
        """Keep dlt state in tmp_path and restore worker-tuning env vars
        (run_pipeline writes them via apply_dlt_overrides)."""
        saved = {var: os.environ.get(var) for var in _WORKER_ENV_VARS}
        monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
        yield
        for var, value in saved.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value

    def test_example_files_scaffolded(self, example_root):
        source_file = example_root / DEFAULT_PIPELINE_NAME / SOURCE_DIR / f"{EXAMPLE_SOURCE_SECTION}.py"
        resource_file = example_root / DEFAULT_PIPELINE_NAME / RESOURCE_DIR / f"{EXAMPLE_RESOURCE_MODULE}.py"
        assert source_file.is_file()
        assert resource_file.is_file()
        # Module stem = config section = explicit decorator name (rules 3-5).
        assert f'@dlt.source(name="{EXAMPLE_SOURCE_SECTION}")' in source_file.read_text()
        assert f"def {EXAMPLE_SOURCE_SECTION}_source" in source_file.read_text()
        assert "columns=Event" in resource_file.read_text()  # rule 14
        data = tomllib.loads((example_root / PROJECT_MARKER).read_text())
        assert data["sources"][EXAMPLE_SOURCE_SECTION]["dlt_ops"]["schedule"] == "@daily"  # rules 6-7

    def test_pipeline_list_shows_example(self, runner, example_root):
        result = runner.invoke(cli, ["--root", str(example_root), "pipeline", "list"])
        assert result.exit_code == 0, result.output
        assert EXAMPLE_SOURCE_SECTION in result.output
        assert "@daily" in result.output

    def test_example_passes_validate(self, runner, example_root, monkeypatch):
        monkeypatch.chdir(example_root)
        result = runner.invoke(cli, ["pipeline", "validate"])
        assert result.exit_code == 0, result.output
        assert "validated successfully" in result.output

    def test_example_run_loads_rows_into_duckdb(self, runner, example_root, monkeypatch, _isolated_run_env):
        monkeypatch.chdir(example_root)
        result = runner.invoke(cli, ["pipeline", "run", "-s", EXAMPLE_SOURCE_SECTION, "-y"])
        assert result.exit_code == 0, result.output

        db_file = example_root / f"{EXAMPLE_SOURCE_SECTION}_pipeline.duckdb"
        assert db_file.is_file()
        con = duckdb.connect(str(db_file))
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {EXAMPLE_DATASET}.events").fetchone()[0]
            assert count == EXAMPLE_ROW_COUNT
            runs = con.execute(f"SELECT status, records_loaded FROM {EXAMPLE_DATASET}._dlt_ops_runs").fetchall()
            assert runs == [("completed", EXAMPLE_ROW_COUNT)]
        finally:
            con.close()
