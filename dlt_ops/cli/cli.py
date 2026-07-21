"""CLI for dlt_ops utilities."""

import sys
from pathlib import Path

import click
from dlt_ops.checkpoints import DEFAULT_CHECKPOINT_TABLE, cleanup_checkpoints, list_checkpoints
from dlt_ops.cli.init import init
from dlt_ops.cli.pipeline import pipeline
from dlt_ops.cli.plugins import plugins


def _force_utf8_output() -> None:
    """Status glyphs (✓ ✗ ⚠ █) are outside cp1252, which Python picks for a
    non-tty stdout on Windows — encoding them raises and takes the command down
    after its work already succeeded. Only safe because this is the entry point.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            pass


@click.group()
@click.option(
    "--root",
    "-r",
    "project_root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Project root (holds .dlt/config.toml). Default: walk up from cwd.",
)
@click.pass_context
def cli(ctx: click.Context, project_root: Path | None):
    """dlt-ops — opinionated project layout and toolchain for dlt pipelines."""
    _force_utf8_output()
    ctx.ensure_object(dict)
    if project_root:
        ctx.obj["project_root"] = project_root


cli.add_command(init)
cli.add_command(pipeline)
cli.add_command(plugins)


@cli.group()
def checkpoints():
    """Manage checkpoints for dlt pipelines."""
    pass


@checkpoints.command()
@click.option(
    "--pipeline",
    "-p",
    required=True,
    help="The dlt pipeline_name (source <X> runs as '<X>_pipeline'), not the dlt-ops source name.",
)
@click.option(
    "--resource",
    "-r",
    help="Resource name (optional, cleans all resources if not specified)",
)
@click.option(
    "--table",
    "-t",
    default=DEFAULT_CHECKPOINT_TABLE,
    help="Checkpoint table name",
)
def cleanup(pipeline, resource, table):
    """Clean up checkpoints for a pipeline or resource.

    Examples:
        dlt-ops checkpoints cleanup --pipeline my_api_pipeline
        dlt-ops checkpoints cleanup --pipeline my_api_pipeline --resource orders
    """
    try:
        cleanup_checkpoints(
            pipeline_name=pipeline,
            resource_name=resource,
            checkpoint_table=table,
        )

        target = f"pipeline '{pipeline}'"
        if resource:
            target += f", resource '{resource}'"

        click.echo(click.style(f"✓ Cleaned up checkpoints for {target}", fg="green"))
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e}", fg="red"), err=True)
        raise click.Abort() from e


@checkpoints.command()
@click.option(
    "--pipeline",
    "-p",
    required=True,
    help="The dlt pipeline_name (source <X> runs as '<X>_pipeline'), not the dlt-ops source name.",
)
@click.option(
    "--table",
    "-t",
    default=DEFAULT_CHECKPOINT_TABLE,
    help="Checkpoint table name",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["table", "json"], case_sensitive=False),
    default="table",
    help="Output format",
)
def list(pipeline, table, format):
    """List checkpoints for a pipeline.

    Examples:
        dlt-ops checkpoints list --pipeline my_api_pipeline
        dlt-ops checkpoints list --pipeline my_api_pipeline --format json
    """
    try:
        results = list_checkpoints(
            pipeline_name=pipeline,
            checkpoint_table=table,
        )

        if not results:
            click.echo(click.style("No checkpoints found", fg="yellow"))
            return

        if format == "json":
            import json

            # Convert datetime objects to strings
            data = [
                {key: value.isoformat() if hasattr(value, "isoformat") else value for key, value in row.items()}
                for row in results
            ]
            click.echo(json.dumps(data, indent=2))
        else:
            # Table format
            click.echo(f"\nFound {len(results)} checkpoint(s):\n")

            # Header
            click.echo(
                click.style(
                    f"{'Resource':<20} {'RunID':<10} {'Checkpoint':<25} {'Pages':<8} {'Records':<10} {'Status':<10} {'Created':<20}",
                    bold=True,
                )
            )
            click.echo("-" * 120)

            # Rows
            for row in results:
                resource_name = row["resource_name"]
                run_id = row["run_id"]
                checkpoint_value = row["checkpoint_value"]
                page_number = row["page_number"]
                records_processed = row["records_processed"]
                status = row["status"]
                created_at = row["created_at"]

                # Truncate long values for display
                checkpoint_display = (
                    str(checkpoint_value)[:22] + "..." if len(str(checkpoint_value)) > 25 else str(checkpoint_value)
                )
                run_id_display = str(run_id)[:8] if run_id else "-"

                # Color code status
                if status == "active":
                    status_display = click.style(status, fg="yellow")
                elif status == "completed":
                    status_display = click.style(status, fg="green")
                else:
                    status_display = status

                click.echo(
                    f"{resource_name:<20} {run_id_display:<10} {checkpoint_display:<25} {page_number:<8} "
                    f"{records_processed:<10} {status_display:<10} {created_at}"
                )

            click.echo()

    except Exception as e:
        click.echo(click.style(f"✗ Error: {e}", fg="red"), err=True)
        raise click.Abort() from e


if __name__ == "__main__":
    cli()
