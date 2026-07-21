"""`dlt-ops pipeline status` — recent runs per source from the run ledger.

Reads ``_dlt_ops_runs`` via the DestinationAdapter boundary across every
destination the project's sources reference (the ledger is per-destination —
CR1-4 locked location). The verb never fails on a broken ledger path, but the
three absence states stay distinct so an outage can't masquerade as an empty
history and a capability gap can't masquerade as an outage: a missing ledger
table reads as "no runs recorded", an unresolvable destination/dataset or an
unreachable/unreadable ledger reads as "ledger unreadable" with the reason,
and a destination running in core mode (no DestinationAdapter, so no ledger
can exist there) reads as "ledger unsupported".
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import click

from dlt_ops.cli._common import _discover_with_progress, resolve_cli_project_root
from dlt_ops.config import ProjectConfigError, load_project_config, resolve_dataset, resolve_destination
from dlt_ops.destinations import UnregisteredDestinationError
from dlt_ops.discovery import discover
from dlt_ops.runs.reader import fetch_runs
from dlt_ops.runs.writer import RunStatus, pipeline_name_for_source, summarize_error

if TYPE_CHECKING:
    from dlt_ops.runs.reader import RunRecord

# Typed dict[str, str]: RunStatus members ARE str, and `run.status` arrives
# as the ledger's plain string.
_STATUS_COLORS: dict[str, str] = {RunStatus.COMPLETED: "green", RunStatus.FAILED: "red", RunStatus.RUNNING: "yellow"}

# Per-source ledger states in the machine-readable output.
_LEDGER_OK = "ok"
_LEDGER_MISSING = "missing"
_LEDGER_UNREADABLE = "unreadable"
_LEDGER_UNSUPPORTED = "unsupported"


def _format_ts(value: object) -> str:
    if value is None:
        return "-"
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")  # type: ignore[attr-defined]
    return str(value)


def _print_run_rows(runs: list[RunRecord]) -> None:
    for run in runs:
        status_display = click.style(f"{run.status:<10}", fg=_STATUS_COLORS.get(run.status, "white"))
        records = "-" if run.records_loaded is None else str(run.records_loaded)
        click.echo(
            f"  {status_display} {_format_ts(run.started_at):<20} {_format_ts(run.completed_at):<20} "
            f"{records:<9} {run.trigger_source:<10} {run.resource_name or '-':<15} {run.run_id[:12]}"
        )
        if run.error_summary:
            click.echo(click.style(f"    ✗ {run.error_summary}", fg="red"))


@click.command()
@click.option("--resource", "-r", "resource_name", help="Only runs scoped to this resource")
@click.option(
    "--limit",
    "-n",
    type=click.IntRange(min=1),
    default=10,
    show_default=True,
    help="Last N runs per source",
)
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.pass_context
def status(ctx: click.Context, resource_name: str | None, limit: int, output_json: bool) -> None:
    """Show recent runs per source from the _dlt_ops_runs ledger.

    The ledger lives where the data lands: one table per destination +
    dataset, written at every run/backfill start and end. Sources that never
    ran (no ledger table yet) are listed with no runs; a ledger the CLI
    cannot read (unresolved destination/dataset, unreachable destination) is
    reported as "ledger unreadable" with the reason; a destination with no
    DestinationAdapter runs in core mode — no ledger can exist there, so it
    is reported as "ledger unsupported" (a capability fact, not a fault).
    The verb itself never fails on a broken ledger path. Note the ledger is
    written best-effort: a run whose ledger write failed is a real run with
    no row here.

    --json emits one object per source, keys stable across states:
    {"source", "ledger" ("ok"|"missing"|"unreadable"|"unsupported"),
    "error" (the unreadable/unsupported reason), "runs"}.

    Examples:
        dlt-ops pipeline status
        dlt-ops pipeline status --resource orders
        dlt-ops pipeline status --limit 5 --json
    """
    project_root = resolve_cli_project_root(ctx)
    # --json keeps stdout machine-parseable: no progress indicator.
    sources = discover(project_root) if output_json else _discover_with_progress(project_root, discover)

    try:
        project_config = load_project_config(project_root)
    except ProjectConfigError as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        sys.exit(1)

    # One read per source pipeline, not per (destination, dataset) pair:
    # file-based destinations (DuckDB) key the physical database on the
    # pipeline name, and the source_section filter keeps sources sharing a
    # physical destination disjoint — the merge never duplicates rows.
    # None = ledger table absent (source never ran); an entry in
    # `ledger_errors` = no ledger rows could be read, with `ledger_states`
    # recording whether that is an outage (unreadable) or a core-mode
    # destination that cannot carry a ledger (unsupported).
    runs_by_source: dict[str, list[RunRecord] | None] = {}
    ledger_errors: dict[str, str] = {}
    ledger_states: dict[str, str] = {}
    for name, src in sorted(sources.items()):
        try:
            destination = resolve_destination(src.config, project_config)
            dataset = resolve_dataset(src.config, project_config)
        except ProjectConfigError as e:
            runs_by_source[name] = None
            ledger_errors[name] = str(e)
            ledger_states[name] = _LEDGER_UNREADABLE
            continue
        try:
            runs_by_source[name] = fetch_runs(
                pipeline_name_for_source(name),
                destination,
                dataset,
                source_section=name,
                resource_name=resource_name,
                limit=limit,
            )
        except UnregisteredDestinationError:
            # Core mode: the ledger cannot exist on this destination — a
            # capability fact, kept distinct from an outage.
            runs_by_source[name] = None
            ledger_errors[name] = f"destination {destination!r} has no DestinationAdapter (core mode)"
            ledger_states[name] = _LEDGER_UNSUPPORTED
        except Exception as exc:
            # Unreachable destination / broken ledger path: status stays a
            # read-only diagnostic — report the degraded read path, don't fail.
            runs_by_source[name] = None
            ledger_errors[name] = summarize_error(exc)
            ledger_states[name] = _LEDGER_UNREADABLE

    def _ledger_state(name: str) -> str:
        if name in ledger_states:
            return ledger_states[name]
        return _LEDGER_MISSING if runs_by_source[name] is None else _LEDGER_OK

    if output_json:
        import json

        data = [
            {
                "source": name,
                "ledger": _ledger_state(name),
                "error": ledger_errors.get(name),
                "runs": [run.as_dict() for run in (runs_by_source[name] or [])],
            }
            for name in sorted(runs_by_source)
        ]
        click.echo(json.dumps(data, indent=2))
        return

    if not runs_by_source:
        click.echo(click.style("No sources found", fg="yellow"))
        return

    click.echo()
    for name in sorted(runs_by_source):
        runs = runs_by_source[name]
        click.echo(click.style(f"Source: {name}", bold=True))
        state = _ledger_state(name)
        # Unsupported renders dim (a capability fact), unreadable yellow (a fault).
        if state == _LEDGER_UNSUPPORTED:
            click.echo(click.style(f"  ! ledger unsupported: {ledger_errors[name]}", dim=True))
            click.echo()
            continue
        if state == _LEDGER_UNREADABLE:
            click.echo(click.style(f"  ! ledger unreadable: {ledger_errors[name]}", fg="yellow"))
            click.echo()
            continue
        if not runs:
            click.echo(click.style("  no runs recorded", dim=True))
            click.echo()
            continue
        click.echo(
            click.style(
                f"  {'Status':<10} {'Started':<20} {'Completed':<20} {'Records':<9} {'Trigger':<10} "
                f"{'Resource':<15} Run ID",
                dim=True,
            )
        )
        _print_run_rows(runs)
        click.echo()
