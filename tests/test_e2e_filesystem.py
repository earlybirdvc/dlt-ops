"""End-to-end acceptance for the core capability tier, driven through the real CLI.

The permanent credential-free anchor for the core tier: a project whose
destination is the local ``filesystem`` (a destination dlt resolves but for
which no ``DestinationAdapter`` is registered), exercised end to end with a
``file://`` ``bucket_url`` under tmp_path — zero credentials, zero network. The
thesis this lane makes executable: dlt-ops runs everywhere dlt runs, with
adapter-gated features degrading loudly, never the core loop.

Each numbered behavior is its own test method so a failure localizes to one
verb. Every method scaffolds the same project shape (one checkpoint-free source
emitting a fixed row count, ``columns=`` model, import-safe) with the
scenario's config tweak, so the config variants (a failing ``min_rows_per_load``
gate, the strict knob) stay independent instead of fighting over one shared
tree. Behaviors covered:

1. ``validate``          — core-mode warning surfaces, naming the dark features.
2. ``run -y``            — the loop completes, rows land as files, one core-mode
                           WARNING, the ledger skips at INFO (never ERROR).
3. ``run`` + tripped ``min_rows_per_load`` — the assertion gate fires before
                           load; nothing lands.
4. ``status``            — the fourth ledger state, ``unsupported``, in text and
                           JSON.
5. ``clean``             — ``--local-only`` works; remote cleanup is refused.
6. ``reconcile``         — refused with the capability message.
7. ``backfill``          — refused at preflight, before any chunk state work.
8. ``run`` + ``require_destination_adapter = true`` — the same run now fails at
                           preflight.

Harness (mirrors ``tests/test_e2e_example.py``): CliRunner invoked with cwd =
project so both the dlt-ops config chain and dlt's own provider reading
the local ``bucket_url`` resolve against the scaffolded tree; an autouse socket
guard fails any INET connect, so the offline guarantee is enforced, not assumed.
Log-based assertions (the run-start WARNING, the ledger-skip INFO) go through
``caplog``: click captures the command's stdout, but the runner emits those on
the logging channel.
"""

from __future__ import annotations

import contextlib
import json
import logging
import socket
from pathlib import Path

import pytest
from click.testing import CliRunner

from dlt_ops.assertions.models import AssertionFailedError
from dlt_ops.cli.cli import cli
from dlt_ops.destinations import ADAPTER_GATED_FEATURES

# `filesystem` resolves in core dlt (no SDK needed for a local bucket_url) but
# has no registered DestinationAdapter — the adapter-less lane covering the
# core tier permanently.
DESTINATION = "filesystem"
SOURCE = "web_events"
RESOURCE = "events"
DATASET = "web_events_raw"
ROW_COUNT = 3

# A checkpoint-free source (checkpoints are adapter-gated — they would refuse
# the core-tier run at preflight), with a `columns=` model and no import-time
# I/O, so the project passes every other rule and only the capability warning
# remains.
FS_SOURCE = """\
import dlt
import pydantic


class Event(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    name: str


@dlt.resource(name="events", columns=Event, write_disposition="append")
def events():
    yield [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]


@dlt.source(name="web_events")
def web_events_source():
    return events
"""


@pytest.fixture(autouse=True)
def _offline_env(tmp_path, monkeypatch):
    """Per-test isolation plus a hard offline guard.

    dlt state stays under tmp_path and telemetry is off so the guard stays
    quiet by design; any INET/INET6 connect fails the test — the local
    filesystem destination touches no socket, so a network attempt is a
    regression in the lane's offline guarantee. AF_UNIX stays allowed.
    """
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    attempts: list[tuple] = []
    real_connect = socket.socket.connect

    def guarded_connect(self, address):
        if self.family in (socket.AF_INET, socket.AF_INET6):
            attempts.append((self.family, address))
            raise RuntimeError(f"network access attempted in the offline filesystem lane: {address!r}")
        return real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    yield
    assert attempts == [], f"network connect attempted during the offline filesystem lane: {attempts}"


def _scaffold_fs_project(tmp_path, monkeypatch, *, assertions: str = "", strict: bool = False) -> tuple[Path, Path]:
    """Write a filesystem-destination project under tmp_path; return (root, bucket).

    ``assertions`` is appended verbatim under the source's ``[...dlt_ops]``
    table (an assertions sub-table for the failing-gate scenario); ``strict``
    sets ``require_destination_adapter`` for the strict-knob scenario.

    The per-test bucket lives inside the project, but its location reaches dlt
    through the ``DESTINATION__FILESYSTEM__BUCKET_URL`` env var rather than a
    ``[destination.filesystem]`` config-toml section: dlt caches its config-toml
    providers process-globally, so a toml value from the first test would leak
    to every later test in the same process, whereas the env provider is read
    live. A real project would set it in secrets/config toml.
    """
    root = tmp_path / "project"
    (root / ".dlt").mkdir(parents=True)
    bucket = root / "bucket"
    bucket.mkdir()
    monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", str(bucket))
    knob = "require_destination_adapter = true\n" if strict else ""
    (root / ".dlt" / "config.toml").write_text(
        "[dlt_ops]\n"
        f'default_destination = "{DESTINATION}"\n'
        f'default_dataset = "{DATASET}"\n'
        f"{knob}"
        "\n"
        f"[sources.{SOURCE}.dlt_ops]\n"
        'schedule = "@daily"\n'
        f"{assertions}",
        encoding="utf-8",
    )
    src_dir = root / SOURCE / "source"
    src_dir.mkdir(parents=True)
    (src_dir / f"{SOURCE}.py").write_text(FS_SOURCE, encoding="utf-8")
    return root, bucket


def _invoke(root: Path, *args: str):
    """Run the CLI with cwd = project (root walking + dlt's local bucket_url read)."""
    with contextlib.chdir(root):
        return CliRunner().invoke(cli, list(args))


@pytest.mark.integration
class TestCoreTierFilesystemEndToEnd:
    """The offline filesystem lane: the core tier's executable definition of done."""

    def test_1_validate_surfaces_the_core_mode_warning(self, tmp_path, monkeypatch):
        """The destination_capability finding is a warning: both runs name the
        destination and every dark feature, and only --strict blocks on it —
        default validate reports core mode without gating (degrade-by-default)."""
        root, _bucket = _scaffold_fs_project(tmp_path, monkeypatch)

        default = _invoke(root, "pipeline", "validate")
        assert default.exit_code == 0, default.output
        assert "warning(s)" in default.output

        strict = _invoke(root, "pipeline", "validate", "--strict")
        assert strict.exit_code == 1, strict.output
        assert "error(s)" in strict.output

        for output in (default.output, strict.output):
            assert SOURCE in output
            assert f"'{DESTINATION}'" in output
            assert "core mode" in output
            for feature in ADAPTER_GATED_FEATURES:
                assert feature in output

    def test_2_run_lands_files_warns_once_and_skips_ledger(self, tmp_path, monkeypatch, caplog):
        """The core loop completes: rows and the trace land as files, exactly one
        WARNING names the destination and every gated feature, and the runs
        ledger skips at INFO — never an ERROR, because nothing is broken."""
        root, bucket = _scaffold_fs_project(tmp_path, monkeypatch)

        with caplog.at_level(logging.INFO):
            result = _invoke(root, "pipeline", "run", "-s", SOURCE, "-y")

        assert result.exit_code == 0, result.output
        assert "core (no adapter:" in result.output
        assert list(bucket.glob(f"{DATASET}/{RESOURCE}/*")), "resource rows must land as files"
        assert list(bucket.glob(f"{DATASET}/_dlt_trace/*")), "trace must persist normally in core mode"

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "core mode" in r.getMessage()]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert f"'{DESTINATION}'" in message
        for feature in ADAPTER_GATED_FEATURES:
            assert feature in message

        assert "runs ledger skipped" in caplog.text
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR and r.name.startswith("dlt_ops")]

    def test_3_failing_min_rows_aborts_before_load(self, tmp_path, monkeypatch):
        """Assertions gate identically on the core tier: a min_rows_per_load
        demanding more than the source emits fails the run before load, so no
        data files land."""
        assertions = f"\n[sources.{SOURCE}.dlt_ops.assertions.{RESOURCE}]\nmin_rows_per_load = {ROW_COUNT + 1}\n"
        root, bucket = _scaffold_fs_project(tmp_path, monkeypatch, assertions=assertions)

        result = _invoke(root, "pipeline", "run", "-s", SOURCE, "-y")

        assert result.exit_code == 1
        assert isinstance(result.exception, AssertionFailedError)
        assert "min_rows_per_load" in str(result.exception)
        assert RESOURCE in str(result.exception)
        assert not list(bucket.glob(f"{DATASET}/{RESOURCE}/*")), "no rows may land when the gate fails before load"

    def test_4_status_reports_ledger_unsupported(self, tmp_path, monkeypatch):
        """status gains a fourth state: a core-mode destination cannot carry a
        ledger, so it reads 'unsupported' (a capability fact) — distinct in text
        and JSON from 'unreadable' (outage) and 'missing' (never ran)."""
        root, _bucket = _scaffold_fs_project(tmp_path, monkeypatch)
        reason = f"destination '{DESTINATION}' has no DestinationAdapter (core mode)"

        text = _invoke(root, "pipeline", "status")
        assert text.exit_code == 0, text.output
        assert f"! ledger unsupported: {reason}" in text.output

        js = _invoke(root, "pipeline", "status", "--json")
        assert js.exit_code == 0, js.output
        entries = {entry["source"]: entry for entry in json.loads(js.output)}
        assert entries[SOURCE]["ledger"] == "unsupported"
        assert entries[SOURCE]["error"] == reason
        assert list(entries[SOURCE]) == ["source", "ledger", "error", "runs"]

    def test_5_clean_local_only_works_remote_refused(self, tmp_path, monkeypatch):
        """Remote cleanup is adapter-gated and refuses with the capability
        message; --local-only never resolves the destination, so it keeps
        working in core mode."""
        root, _bucket = _scaffold_fs_project(tmp_path, monkeypatch)
        run = _invoke(root, "pipeline", "run", "-s", SOURCE, "-y")
        assert run.exit_code == 0, run.output

        remote = _invoke(root, "pipeline", "clean", "-s", SOURCE, "--remote-only", "--auto-approve")
        assert remote.exit_code != 0
        assert f"'{DESTINATION}'" in remote.output
        assert "DestinationAdapter" in remote.output
        assert "--local-only" in remote.output

        local = _invoke(root, "pipeline", "clean", "-s", SOURCE, "--local-only", "--auto-approve")
        assert local.exit_code == 0, local.output
        assert "Cleanup complete" in local.output

    def test_6_reconcile_refused_with_capability_message(self, tmp_path, monkeypatch):
        """reconcile is SQL-bound: on a core-mode destination it errors per
        source with the capability message, and the verb exits non-zero."""
        root, _bucket = _scaffold_fs_project(tmp_path, monkeypatch)

        result = _invoke(root, "pipeline", "reconcile", "-s", SOURCE)

        assert result.exit_code == 1, result.output
        assert f"'{DESTINATION}'" in result.output
        assert "core mode" in result.output

    def test_7_backfill_refused_at_preflight_before_chunk_state(self, tmp_path, monkeypatch):
        """Chunk state in _dlt_backfills IS the gated feature, so a core-mode
        destination is refused at preflight — with the backfill-specific
        message, before any chunk math or state work."""
        root, bucket = _scaffold_fs_project(tmp_path, monkeypatch)

        result = _invoke(
            root,
            "pipeline",
            "backfill",
            SOURCE,
            "--from",
            "2024-01-01T00:00:00Z",
            "--to",
            "2024-01-08T00:00:00Z",
            "--chunk",
            "7d",
        )

        assert result.exit_code == 1, result.output
        assert f"'{DESTINATION}'" in result.output
        assert "backfill (chunk state in _dlt_backfills)" in result.output
        assert not list(bucket.rglob("*.jsonl*")), "nothing may touch the destination on a refused backfill"

    def test_8_strict_knob_turns_the_run_into_a_preflight_failure(self, tmp_path, monkeypatch):
        """[dlt_ops] require_destination_adapter = true makes adapter
        absence fatal: the same run that succeeds by default now fails at
        preflight, before extract, and nothing lands."""
        root, bucket = _scaffold_fs_project(tmp_path, monkeypatch, strict=True)

        result = _invoke(root, "pipeline", "run", "-s", SOURCE, "-y")

        assert result.exit_code != 0
        message = str(result.exception)
        assert f"'{DESTINATION}'" in message
        assert "require_destination_adapter" in message
        assert not list(bucket.glob(f"{DATASET}/{RESOURCE}/*")), "no rows may land when preflight refuses"
