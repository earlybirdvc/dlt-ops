from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import click

from dlt_ops.cli._common import _discover_with_progress, resolve_cli_project_root
from dlt_ops.cli.backfill import backfill
from dlt_ops.cli.status import status
from dlt_ops.config import (
    ProjectConfigError,
    UnresolvedDatasetError,
    UnresolvedDestinationError,
    load_project_config,
    resolve_dataset,
    resolve_destination,
)
from dlt_ops.destinations import ADAPTER_GATED_FEATURES, UnregisteredDestinationError, has_adapter
from dlt_ops.discovery import Schedule, discover, validate_sources
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.discovery.validator import check_unknown_rule_ids, load_rule_specs, resolve_rules

if TYPE_CHECKING:
    from dlt_ops.reconciler.models import ReconcileResult


@click.group()
@click.pass_context
def pipeline(ctx: click.Context) -> None:
    """Manage dlt pipelines - discover, run, validate.

    The project root comes from the top-level --root option (or is found by
    walking up from cwd to the nearest .dlt/config.toml).
    """
    ctx.ensure_object(dict)


@pipeline.command("list")
@click.option("--schedule", "-s", "filter_schedule", help="Filter by schedule (@hourly, @daily, etc.)")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.pass_context
def list_sources(ctx: click.Context, filter_schedule: str | None, output_json: bool) -> None:
    """List available dlt sources.

    Uses the Phase-1 static scan: source modules are never imported, so a
    module-level side effect (or bug) cannot fire here. Resource counts come
    from the static approximation; `validate`/`run` resolve the live list.

    Examples:
        dlt-ops pipeline list
        dlt-ops pipeline list --schedule @daily
        dlt-ops pipeline list --json
    """
    project_root = resolve_cli_project_root(ctx)
    sources = _discover_with_progress(project_root, discover)

    if not sources:
        click.echo(click.style("No sources found", fg="yellow"))
        return

    if output_json:
        import json

        data = [
            {
                "name": name,
                "pipeline": src.pipeline_name,
                "function": src.function_name,
                "config_section": src.config_section,
                "schedule": src.config.schedule.value if src.config else None,
                "airflow_var": src.config.airflow_var if src.config else None,
                "resources": list(src.resources),
            }
            for name, src in sorted(sources.items())
        ]
        click.echo(json.dumps(data, indent=2))
        return

    # Filter by schedule if specified
    if filter_schedule:
        try:
            target_schedule = Schedule.from_string(filter_schedule)
        except ValueError as e:
            click.echo(click.style(f"Error: {e}", fg="red"))
            sys.exit(1)

        sources = {name: src for name, src in sources.items() if src.config and src.config.schedule == target_schedule}

    click.echo()
    click.echo(click.style(f"Found {len(sources)} source(s)", fg="green", bold=True))
    click.echo()

    # Header
    click.echo(click.style(f"{'Name':<30} {'Pipeline':<15} {'Schedule':<10} {'Resources':<8}", bold=True))
    click.echo(click.style("-" * 70, dim=True))

    for name in sorted(sources.keys()):
        src = sources[name]
        schedule = src.config.schedule.value if src.config else click.style("-", fg="yellow")
        resource_count = len(src.resources)

        if src.config:
            schedule_display = click.style(schedule, fg="green")
        else:
            schedule_display = click.style("-", fg="yellow")

        click.echo(f"{name:<30} {src.pipeline_name:<15} {schedule_display:<19} {resource_count}")

    click.echo()
    # The count is the import-free Phase-1 approximation: a resource shared under a pipeline's
    # resource/ is attributed to every source in that pipeline, so a source that uses only some of
    # them reads high here. validate/run resolve each source's live list (Phase-2 introspection).
    click.echo(
        click.style(
            "Resource counts are a static, import-free estimate — a resource shared under a pipeline's "
            "resource/ counts toward every source in that pipeline; validate/run resolve the actual per-source list.",
            dim=True,
        )
    )
    click.echo()


@pipeline.command()
@click.option("--source", "-s", "source_name", help="Source name (interactive if not provided)")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.pass_context
def resources(ctx: click.Context, source_name: str | None, output_json: bool) -> None:
    """List resources for a source.

    Uses the Phase-1 static scan: source modules are never imported. The
    resource list is a static approximation (declared @dlt.resource
    functions); dynamically built resources resolve in `validate`/`run`.

    Examples:
        dlt-ops pipeline resources -s github_events
        dlt-ops pipeline resources -s my_api --json
        dlt-ops pipeline resources  # interactive selection
    """
    project_root = resolve_cli_project_root(ctx)
    sources = _discover_with_progress(project_root, discover)

    # Interactive source selection if not provided
    if not source_name:
        source_names = sorted(sources.keys())
        click.echo()
        click.echo(click.style("Select a source:", bold=True))

        for i, name in enumerate(source_names, 1):
            src = sources[name]
            schedule = src.config.schedule.value if src.config else "-"
            click.echo(f"  {click.style(str(i), fg='cyan')}. {name} ({schedule}, {len(src.resources)} resources)")

        click.echo()
        choice = click.prompt(
            "Enter number",
            type=click.IntRange(1, len(source_names)),
            default=1,
        )
        source_name = source_names[choice - 1]
        click.echo()

    if source_name not in sources:
        click.echo(click.style(f"Error: Unknown source '{source_name}'", fg="red"))
        click.echo()
        click.echo("Available sources:")
        for name in sorted(sources.keys()):
            click.echo(f"  {click.style(name, fg='cyan')}")
        sys.exit(1)

    src = sources[source_name]

    if output_json:
        import json

        click.echo(json.dumps(list(src.resources), indent=2))
        return

    click.echo()
    click.echo(click.style("Source: ", dim=True) + click.style(source_name, bold=True))
    click.echo(click.style("Pipeline: ", dim=True) + src.pipeline_name)
    click.echo(click.style("Function: ", dim=True) + src.function_name)
    click.echo(click.style("Config: ", dim=True) + f"[sources.{src.config_section}]")

    if src.config:
        click.echo(click.style("Schedule: ", dim=True) + click.style(src.config.schedule.value, fg="green"))
        if src.config.airflow_var:
            click.echo(click.style("Airflow Variable: ", dim=True) + src.config.airflow_var)
    click.echo()

    click.echo(click.style(f"Resources ({len(src.resources)}):", bold=True))
    for name in sorted(src.resources):
        click.echo(f"  {click.style('•', fg='cyan')} {name}")

    click.echo()


@pipeline.command()
@click.option("--source", "-s", "source_name", help="Source name (required in non-interactive mode)")
@click.option("--resource", "-r", "resource_names", multiple=True, help="Resource(s) to run. Omit for all.")
@click.option("--dataset", "-d", "dataset_name", help="Dataset override. Default: resolved from .dlt/config.toml")
@click.option("--normalize-workers", "-n", type=int, help="Parallel normalize workers")
@click.option("--load-workers", "-l", type=int, help="Parallel load workers (default: 3 local, 15 Airflow)")
@click.option("--file-max-items", "-f", type=int, help="Max rows per normalized file")
@click.option("--interactive", "-I", is_flag=True, help="Interactive resource selection")
@click.option("--yes", "-y", is_flag=True, help="Non-interactive mode, skip confirmations")
@click.pass_context
def run(
    ctx: click.Context,
    source_name: str | None,
    resource_names: tuple[str, ...],
    dataset_name: str | None,
    normalize_workers: int | None,
    load_workers: int | None,
    file_max_items: int | None,
    interactive: bool,
    yes: bool,
) -> None:
    """Run a dlt pipeline.

    Examples:
        dlt-ops pipeline run -s github_events
        dlt-ops pipeline run -s my_api -r orders -r customers
        dlt-ops pipeline run -s github_events --dataset raw_events
        dlt-ops pipeline run -s my_api -n 4 -f 1000000
        dlt-ops pipeline run -s my_api -l 2  # reduce load workers
        dlt-ops pipeline run -I  # interactive mode
        dlt-ops pipeline run -s github_events -y  # non-interactive (for scripts)
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s|[%(levelname)s]|%(name)s|%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    project_root = resolve_cli_project_root(ctx)
    sources = _discover_with_progress(project_root)

    # Non-interactive mode validation
    if yes and not source_name:
        click.echo(click.style("Error: --source/-s required in non-interactive mode (--yes)", fg="red"))
        sys.exit(1)

    # Interactive source selection if not provided
    if not source_name and not yes:
        source_names = sorted(sources.keys())
        click.echo()
        click.echo(click.style("Select a source to run:", bold=True))

        for i, name in enumerate(source_names, 1):
            src = sources[name]
            schedule = src.config.schedule.value if src.config else "-"
            click.echo(f"  {click.style(str(i), fg='cyan')}. {name} ({schedule}, {len(src.resources)} resources)")

        click.echo()
        choice = click.prompt(
            "Enter number",
            type=click.IntRange(1, len(source_names)),
            default=1,
        )
        source_name = source_names[choice - 1]
        interactive = True  # Enable interactive resource selection too
        click.echo()

    if source_name not in sources:
        click.echo(click.style(f"Error: Unknown source '{source_name}'", fg="red"))
        click.echo()
        click.echo("Available sources:")
        for name in sorted(sources.keys()):
            click.echo(f"  {click.style(name, fg='cyan')}")
        sys.exit(1)

    src = sources[source_name]

    # Interactive resource selection (skip if --yes)
    if interactive and not resource_names and not yes:
        resource_list = sorted(src.resources)
        click.echo(click.style(f"Resources in {source_name}:", bold=True))
        click.echo()

        for i, name in enumerate(resource_list, 1):
            click.echo(f"  {click.style(str(i), fg='cyan')}. {name}")

        click.echo()
        click.echo(click.style("Enter resource numbers (comma-separated) or 'all':", dim=True))
        selection = click.prompt("Selection", default="all")

        if selection.lower() != "all":
            try:
                indices = [int(x.strip()) for x in selection.split(",")]
                resource_names = tuple(resource_list[i - 1] for i in indices if 1 <= i <= len(resource_list))
            except (ValueError, IndexError):
                click.echo(click.style("Invalid selection, running all resources", fg="yellow"))
                resource_names = ()

        click.echo()

    # Resolve destination + dataset through the config chain (project default
    # -> per-source override); an explicit --dataset outranks both. No silent
    # fallback: an unresolved destination/dataset is a config error.
    try:
        project_config = load_project_config(project_root)
        destination = resolve_destination(src.config, project_config)
        resolved_dataset = dataset_name or resolve_dataset(src.config, project_config)
    except ProjectConfigError as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        sys.exit(1)

    click.echo()
    click.echo(click.style("Pipeline Configuration", bold=True))
    click.echo(click.style("-" * 40, dim=True))
    click.echo(f"  Source: {click.style(source_name, fg='green')}")
    click.echo(f"  Function: {src.function_name}")

    if resource_names:
        click.echo(f"  Resources: {', '.join(resource_names)}")
    else:
        click.echo(f"  Resources: {click.style('all', fg='cyan')} ({len(src.resources)} total)")

    click.echo(f"  Destination: {click.style(destination, fg='cyan')}")
    if dataset_name:
        click.echo(f"  Dataset: {resolved_dataset}")
    else:
        click.echo(f"  Dataset: {resolved_dataset} " + click.style("(from .dlt/config.toml)", dim=True))

    if has_adapter(destination):
        click.echo(f"  Capabilities: {click.style('full', fg='green')}")
    else:
        click.echo(
            f"  Capabilities: {click.style('core', fg='yellow')} "
            + click.style(f"(no adapter: {', '.join(ADAPTER_GATED_FEATURES)} unavailable)", dim=True)
        )

    click.echo()

    # Confirm before running (skip if --yes)
    if not yes:
        if not click.confirm(click.style("Start pipeline?", bold=True), default=True):
            click.echo(click.style("Aborted", fg="yellow"))
            return

    click.echo()
    click.echo(click.style("Starting pipeline...", fg="cyan", bold=True))
    click.echo()

    run_pipeline(
        source=src,
        resources=resource_names or None,
        project_root=project_root,
        destination=destination,
        dataset_name=resolved_dataset,
        normalize_workers=normalize_workers,
        load_workers=load_workers,
        file_max_items=file_max_items,
    )


def _show_resolved_rules(project_root: Path) -> None:
    """Print every known rule with resolved on/off state and origin; no discovery runs.

    Soft-failed rule providers are listed as unavailable with the load error.
    Unknown rule IDs configured in [dlt_ops.rules] exit 1 (typo guard).
    """
    try:
        project_config = load_project_config(project_root)
    except ProjectConfigError as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        sys.exit(1)

    assembly = load_rule_specs()
    resolved = resolve_rules(project_config, assembly)

    click.echo()
    click.echo(click.style(f"Resolved rules ({len(assembly.specs)}):", bold=True))
    width = max((len(spec.rule_id) for spec in assembly.specs), default=0)
    for spec in assembly.specs:
        enabled = resolved[spec.rule_id]
        state = click.style("on ", fg="green") if enabled else click.style("off", fg="yellow")
        click.echo(f"  {spec.rule_id:<{width}}  {state}  {spec.plugin}")

    if assembly.failures:
        click.echo()
        click.echo(click.style("Unavailable rule providers:", fg="yellow", bold=True))
        for failure in assembly.failures:
            click.echo(f"  {failure.provider}: {failure.error}")

    unknown = check_unknown_rule_ids(project_config.rules, assembly.known_ids)
    if unknown:
        click.echo()
        click.echo(
            click.style(
                f"✗ unknown rule id(s) in [dlt_ops.rules]: {', '.join(unknown)}",
                fg="red",
                bold=True,
            )
        )
        click.echo(f"  valid rule ids: {', '.join(sorted(assembly.known_ids))}")
        click.echo()
        sys.exit(1)
    click.echo()


@pipeline.command()
@click.option("--strict", is_flag=True, help="Treat warnings as errors")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.option(
    "--show-resolved-rules",
    is_flag=True,
    help="Print every known rule with resolved on/off state and origin, then exit",
)
@click.pass_context
def validate(ctx: click.Context, strict: bool, output_json: bool, show_resolved_rules: bool) -> None:
    """Validate the project against the rule framework.

    Runs the resolved rule set over the discovered sources and
    .dlt/config.toml (config sections, schedules, schema contracts, naming,
    column hints, ...). Rules resolve from the registry defaults overlaid by
    [dlt_ops.rules] (missing = on, false = off; unknown rule IDs are
    errors); per-source exemptions live in
    [sources.<X>.dlt_ops.rule_exemptions] with a mandatory reason.
    Source modules import inside the Phase-2 sandbox first: modules that fail
    to import and modules that violate Rule 15 (network I/O or disk writes at
    import time; opt out via [dlt_ops.rules] import_safety = false) are
    reported as errors. Findings are errors or warnings; warnings only fail
    the run with --strict.

    Examples:
        dlt-ops pipeline validate
        dlt-ops pipeline validate --strict
        dlt-ops pipeline validate --json
        dlt-ops pipeline validate --show-resolved-rules
    """
    project_root = resolve_cli_project_root(ctx)

    if show_resolved_rules:
        _show_resolved_rules(project_root)
        return

    with click.progressbar(
        length=1,
        label=click.style("Validating sources", fg="cyan"),
        show_eta=False,
        show_percent=False,
        fill_char=click.style("█", fg="cyan"),
        empty_char="░",
    ) as bar:
        errors = validate_sources(project_root, strict=strict)
        bar.update(1)

    if output_json:
        import json

        data = [
            {
                "source": e.source_name,
                "field": e.field,
                "message": e.message,
                "is_warning": e.is_warning,
            }
            for e in errors
        ]
        click.echo(json.dumps(data, indent=2))
        if any(not e.is_warning for e in errors) or (strict and errors):
            sys.exit(1)
        return

    if not errors:
        click.echo()
        click.echo(click.style("✓ All sources validated successfully", fg="green", bold=True))
        click.echo()
        return

    # Separate errors and warnings
    real_errors = [e for e in errors if not e.is_warning]
    warnings = [e for e in errors if e.is_warning]

    click.echo()

    if warnings:
        click.echo(click.style(f"⚠ {len(warnings)} warning(s):", fg="yellow", bold=True))
        for w in warnings:
            click.echo(f"  [{click.style(w.source_name, fg='yellow')}] {w.field}: {w.message}")
        click.echo()

    if real_errors:
        click.echo(click.style(f"✗ {len(real_errors)} error(s):", fg="red", bold=True))
        for e in real_errors:
            click.echo(f"  [{click.style(e.source_name, fg='red')}] {e.field}: {e.message}")
        click.echo()
        sys.exit(1)

    if strict and warnings:
        click.echo(click.style("✗ --strict: warnings treated as errors", fg="red", bold=True))
        click.echo()
        sys.exit(1)


def _remote_clean_refusal(destination: str | None) -> click.ClickException:
    """Refusal for remote cleanup on a core-mode destination; local cleanup stays available."""
    return click.ClickException(
        f"destination {destination!r} has no DestinationAdapter — remote cleanup needs one (core mode). "
        f"Clean local state with --local-only, or register an adapter; see docs/reference/destinations.md."
    )


@pipeline.command()
@click.option("-s", "--source", "source_name", required=True, help="Source name to clean")
@click.option("-r", "--resource", "resources", multiple=True, help="Specific resources (default: all)")
@click.option("--local-only", is_flag=True, help="Clean local cache only")
@click.option("--remote-only", is_flag=True, help="Clean remote destination tables only")
@click.option("--dataset", "dataset_name", help="Dataset override. Default: resolved from .dlt/config.toml")
@click.option("--auto-approve", is_flag=True, help="Skip confirmation (for programmatic runs)")
@click.option("--dry-run", is_flag=True, help="Show what would be cleaned without executing")
@click.pass_context
def clean(
    ctx: click.Context,
    source_name: str,
    resources: tuple[str, ...],
    local_only: bool,
    remote_only: bool,
    dataset_name: str | None,
    auto_approve: bool,
    dry_run: bool,
) -> None:
    """Clean pipeline state and data tables.

    Works without local state (identifies tables from the destination schema
    or source discovery).

    Examples:
        dlt-ops pipeline clean -s my_api
        dlt-ops pipeline clean -s my_api -r orders
        dlt-ops pipeline clean -s my_api --local-only --auto-approve
        dlt-ops pipeline clean -s my_api --dry-run
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s|%(name)s|%(message)s", datefmt="%H:%M:%S")

    # Validate flags
    if local_only and remote_only:
        click.echo(click.style("Error: Cannot use --local-only and --remote-only together", fg="red"))
        sys.exit(1)

    # Default: clean both
    clean_local = not remote_only
    clean_remote = not local_only

    project_root = resolve_cli_project_root(ctx)
    sources = _discover_with_progress(project_root)

    # Validate source exists
    if source_name not in sources:
        click.echo(click.style(f"Error: Unknown source '{source_name}'", fg="red"))
        click.echo()
        click.echo("Available sources:")
        for name in sorted(sources.keys()):
            click.echo(f"  {click.style(name, fg='cyan')}")
        sys.exit(1)

    src = sources[source_name]

    # Validate resources
    resource_list = list(resources) if resources else None
    if resource_list:
        available = set(src.resources)
        missing = set(resource_list) - available
        if missing:
            click.echo(click.style(f"Error: Unknown resources: {missing}", fg="red"))
            click.echo()
            click.echo("Available resources:")
            for res in sorted(src.resources):
                click.echo(f"  {click.style(res, fg='cyan')}")
            sys.exit(1)

    # Destination/dataset resolution through the config chain (project default
    # -> per-source override); an explicit --dataset outranks both. No silent
    # fallback: remote cleanup with an unresolved destination/dataset is a
    # config error.
    destination = None
    if clean_remote:
        try:
            project_config = load_project_config(project_root)
            if dataset_name is None:
                dataset_name = resolve_dataset(src.config, project_config)
            destination = resolve_destination(src.config, project_config)
        except UnresolvedDatasetError as e:
            raise click.ClickException("No dataset configured: set [dlt_ops].default_dataset or pass --dataset") from e
        except UnresolvedDestinationError as e:
            raise click.ClickException(
                "No destination configured: set [dlt_ops].default_destination in .dlt/config.toml"
            ) from e

    # Imported lazily: cleanup pulls dlt pipeline + destination-boundary
    # machinery the read-only verbs never need.
    from dlt_ops.discovery.cleanup import CleanupUnsupportedError, clean_pipeline, get_cleanup_plan

    # Build cleanup plan (uses 3-tier table mapping fallback)
    try:
        plan = get_cleanup_plan(
            source=src,
            resources=resource_list,
            local=clean_local,
            remote=clean_remote,
            dataset_name=dataset_name,
            destination=destination,
        )
    except UnregisteredDestinationError as e:
        raise _remote_clean_refusal(destination) from e
    except CleanupUnsupportedError as e:
        raise click.ClickException(str(e)) from e

    # Display cleanup plan
    click.echo()
    click.echo(click.style("Cleanup Plan:", fg="cyan", bold=True))
    click.echo()
    click.echo(f"  Source:    {click.style(source_name, bold=True)}")
    click.echo(f"  Pipeline:  {plan['pipeline_name']}")

    if resource_list:
        click.echo(f"  Resources: {', '.join(resource_list)}")
    else:
        click.echo(f"  Resources: {click.style('all', fg='yellow')} ({len(src.resources)} total)")

    click.echo()

    if clean_local:
        if plan["is_full"]:
            status = "" if plan["local_exists"] else click.style(" (not found)", dim=True)
            click.echo(f"  Local:  {plan['working_dir']}{status}")
        else:
            click.echo("  Local:  update state.json + schema (keep working dir)")

    if clean_remote:
        click.echo(f"  Remote: {click.style(dataset_name, bold=True)}")

        if plan["data_tables"]:
            table_count = len(plan["data_tables"])
            table_preview = ", ".join(plan["data_tables"][:5])
            click.echo(f"          - {table_count} data table(s): {table_preview}")
            if table_count > 5:
                click.echo(f"            {click.style(f'... and {table_count - 5} more', dim=True)}")

        click.echo(f"          - {len(plan['target_resources'])} resource state(s)")

        if plan["is_full"]:
            click.echo(f"          - system tables: DELETE rows from {', '.join(plan['system_tables'])}")
        else:
            click.echo("          - state: surgical update (remove resource entries)")

        click.echo(f"          - checkpoints for {len(plan['target_resources'])} resource(s)")

    click.echo()

    # Dry-run: show plan and exit
    if dry_run:
        click.echo(click.style("Dry-run mode: no changes will be made", fg="blue", bold=True))
        return

    # User confirmation
    if not auto_approve:
        if not click.confirm(
            click.style("This will permanently delete the above. Continue?", fg="yellow", bold=True),
            default=False,
        ):
            click.echo(click.style("Aborted", fg="red"))
            raise click.Abort()

    click.echo()
    click.echo(click.style("Cleaning...", fg="cyan"))
    click.echo()

    # Execute cleanup
    try:
        result = clean_pipeline(
            source=src,
            resources=resource_list,
            local=clean_local,
            remote=clean_remote,
            dataset_name=dataset_name,
            destination=destination,
        )

        # Show success summary
        if result["local"]:
            click.echo(click.style("Local:", fg="green", bold=True))
            for item in result["local"]:
                click.echo(f"  - {item}")

        if result["remote"]:
            click.echo(click.style("Remote:", fg="green", bold=True))
            for item in result["remote"]:
                click.echo(f"  - {item}")

        if not result["local"] and not result["remote"]:
            click.echo(click.style("Nothing to clean", dim=True))

        click.echo()
        click.echo(click.style("Cleanup complete", fg="green", bold=True))

    except UnregisteredDestinationError as e:
        raise _remote_clean_refusal(destination) from e
    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        raise click.Abort() from e


def _print_reconcile_result(result: ReconcileResult, scan_suffix: str = "") -> None:
    """Render one ReconcileResult as a compact per-source block.

    ``scan_suffix`` labels non-default scans (e.g. ``" (removal)"``) so the
    additive and removal blocks for one source stay distinguishable.
    """
    duration_s = result.duration_ms / 1000.0
    header = (
        click.style("Source: ", dim=True)
        + click.style(result.source_name, fg="cyan", bold=True)
        + (click.style(scan_suffix, fg="magenta") if scan_suffix else "")
        + click.style("  |  ", dim=True)
        + click.style(f"Findings: {len(result.findings)}", bold=True)
        + click.style("  |  ", dim=True)
        + f"Duration: {duration_s:.2f}s"
    )
    click.echo(header)

    if result.error is not None:
        click.echo(click.style(f"  ✗ Reconciler error: {result.error}", fg="red"))
        return

    if not result.findings:
        click.echo(click.style("  ✓ No drift", fg="green"))
        return

    for finding in result.findings:
        drift_style = "yellow" if str(finding.kind) == "additive" else "magenta"
        click.echo(
            "  "
            + click.style("•", fg="cyan")
            + " "
            + click.style(finding.resource_name, bold=True)
            + ": "
            + click.style(f"{finding.kind} drift", fg=drift_style)
            + f" ({len(finding.columns)} column(s))"
        )
        preview_cols = ", ".join(finding.columns[:5])
        if len(finding.columns) > 5:
            preview_cols += ", …"
        click.echo(click.style("      Columns: ", dim=True) + preview_cols)
        click.echo(click.style("      First seen: ", dim=True) + finding.first_seen_at.isoformat())


@pipeline.command()
@click.option("-s", "--source", "source_name", help="Source name to reconcile")
@click.option("--all", "all_sources", is_flag=True, help="Reconcile every discovered source")
@click.option(
    "--include-removal",
    is_flag=True,
    help="Also run removal-drift detection (needs [dlt_ops] load_timestamp_column)",
)
@click.option("--dry-run", is_flag=True, help="Skip alert emission; print findings only")
@click.pass_context
def reconcile(
    ctx: click.Context,
    source_name: str | None,
    all_sources: bool,
    include_removal: bool,
    dry_run: bool,
) -> None:
    """Detect schema drift (live destination schema vs declared model).

    The default scan is additive: columns present in the destination that the
    declared Pydantic model doesn't know about. --include-removal adds the
    removal scan — a windowed non-null-coverage diff that catches model
    columns whose data went dark; it needs [dlt_ops]
    load_timestamp_column as the time axis and is skipped with a warning when
    that key is unset.

    Runs against every source regardless of contract mode. Drifted resources
    surface an alert event (fingerprint ["schema-drift", pipeline, source,
    resource]; additive + removal on one resource collapse into one issue)
    unless --dry-run is set.

    Examples:
        dlt-ops pipeline reconcile -s github_events
        dlt-ops pipeline reconcile --all
        dlt-ops pipeline reconcile --all --include-removal
        dlt-ops pipeline reconcile -s github_events --dry-run
        dlt-ops pipeline reconcile --all --dry-run
    """
    # --source and --all are mutually exclusive; exactly one must be supplied.
    if bool(source_name) == bool(all_sources):
        click.echo(
            click.style("Error: exactly one of --source/-s or --all is required", fg="red"),
            err=True,
        )
        click.echo(ctx.get_usage(), err=True)
        sys.exit(2)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s|[%(levelname)s]|%(name)s|%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if dry_run:
        click.echo(click.style("Dry-run: alert emission suppressed", fg="blue", bold=True))
        click.echo()

    # Honour the top-level --root option so `dlt-ops --root <path>
    # pipeline reconcile ...` uses the caller's tree instead of walking up
    # from cwd. Matches how the other pipeline subcommands read the same
    # context object.
    project_root = resolve_cli_project_root(ctx)

    # Imported lazily so `dlt-ops --help` and the read-only verbs keep
    # their import-time budget — the reconciler pulls pipeline/adapter
    # machinery the rest of the CLI never needs.
    from dlt_ops.reconciler import detect_removal, reconcile_all, reconcile_source

    if all_sources:
        additive_results = reconcile_all(dry_run=dry_run, project_root=project_root)
    else:
        # The mutually-exclusive check above proves source_name is a non-empty
        # str on this branch; the assert narrows for the type checker.
        assert source_name is not None
        additive_results = [reconcile_source(source_name, dry_run=dry_run, project_root=project_root)]

    # Interleave per source: a source's removal block prints right under its
    # additive block, matching how the shared fingerprint collapses them.
    results: list[tuple[ReconcileResult, str]] = []
    for additive in additive_results:
        results.append((additive, ""))
        if include_removal:
            removal = detect_removal(additive.source_name, dry_run=dry_run, project_root=project_root)
            results.append((removal, " (removal)"))

    any_error = False
    for i, (result, scan_suffix) in enumerate(results):
        if i > 0:
            click.echo()
        _print_reconcile_result(result, scan_suffix)
        for warning in getattr(result, "warnings", ()):
            click.echo(click.style(f"  ! {warning}", fg="yellow"))
        if result.error is not None:
            any_error = True

    if any_error:
        sys.exit(1)


pipeline.add_command(backfill)
pipeline.add_command(status)
