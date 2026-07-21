"""`dlt-ops init` — scaffold a valid project layout.

The layout is mandatory (discovery refuses anything else), so the package
must be able to produce it: `init` creates the implicit project marker
(.dlt/config.toml with a [dlt_ops] table) plus one starter pipeline
directory. `--example` adds a runnable fixture-based source that passes
`pipeline validate` and loads rows into local DuckDB with no credentials.

Safety: an existing .dlt/config.toml is never touched — re-running `init`
on an initialized root fails loudly. There is deliberately no --force;
deleting user config is `rm`'s job.
"""

import sys
from pathlib import Path

import click

from dlt_ops.cli._init_templates import (
    DEFAULT_PIPELINE_NAME,
    EXAMPLE_RESOURCE_MODULE,
    EXAMPLE_SOURCE_SECTION,
    SECRETS_TOML,
    render_config_toml,
    render_example_resource_module,
    render_example_source_module,
)
from dlt_ops.config import PROJECT_MARKER, RESOURCE_DIR, SOURCE_DIR


def _check_pipeline_name(name: str) -> None:
    """Reject pipeline directory names discovery would never scan.

    Discovery excludes no directory by name: a leading ``.`` or ``_`` is the
    whole name-based rule, and both are covered here — a leading ``.`` already
    fails ``isidentifier()``.
    """
    if not name.isidentifier() or name.startswith("_"):
        raise click.BadParameter(
            f"{name!r} is not a valid pipeline directory name: it must be a "
            f"Python identifier not starting with '_' (discovery skips the rest).",
            param_hint="--pipeline",
        )


def _write_if_absent(path: Path, content: str, created: list[str], root: Path) -> None:
    """Write `content` to `path` unless it already exists (never clobber)."""
    if path.exists():
        return
    path.write_text(content, encoding="utf-8")
    created.append(str(path.relative_to(root)))


@click.command()
@click.argument("root", type=click.Path(file_okay=False, path_type=Path), default=".")
@click.option(
    "--pipeline",
    "-p",
    "pipeline_name",
    default=DEFAULT_PIPELINE_NAME,
    show_default=True,
    help="Name of the starter pipeline directory.",
)
@click.option(
    "--example",
    "with_example",
    is_flag=True,
    help="Add a runnable fixture-based example source (inline rows, local DuckDB, no network).",
)
def init(root: Path, pipeline_name: str, with_example: bool) -> None:
    """Scaffold a dlt-ops project at ROOT (default: current directory).

    Creates the project marker (.dlt/config.toml with a [dlt_ops]
    table), an empty .dlt/secrets.toml, and one starter pipeline directory
    with the mandatory source/ and resource/ subdirectories.

    Examples:
        dlt-ops init
        dlt-ops init demo --pipeline web_events
        dlt-ops init demo --example
    """
    _check_pipeline_name(pipeline_name)

    marker = root / PROJECT_MARKER
    if marker.exists():
        click.echo(
            click.style(
                f"Error: {marker} already exists — refusing to overwrite it. "
                f"There is no --force; remove the file yourself to re-initialize.",
                fg="red",
            ),
            err=True,
        )
        sys.exit(1)

    created: list[str] = []
    marker.parent.mkdir(parents=True, exist_ok=True)
    example_section = EXAMPLE_SOURCE_SECTION if with_example else None
    _write_if_absent(marker, render_config_toml(example_section=example_section), created, root)
    _write_if_absent(root / PROJECT_MARKER.parent / "secrets.toml", SECRETS_TOML, created, root)

    source_dir = root / pipeline_name / SOURCE_DIR
    resource_dir = root / pipeline_name / RESOURCE_DIR
    for directory in (source_dir, resource_dir):
        directory.mkdir(parents=True, exist_ok=True)
        created.append(f"{directory.relative_to(root)}/")

    if with_example:
        _write_if_absent(source_dir / f"{EXAMPLE_SOURCE_SECTION}.py", render_example_source_module(), created, root)
        _write_if_absent(
            resource_dir / f"{EXAMPLE_RESOURCE_MODULE}.py", render_example_resource_module(), created, root
        )
    else:
        # Keep the mandatory-but-empty layout dirs alive in version control.
        for directory in (source_dir, resource_dir):
            _write_if_absent(directory / ".gitkeep", "", created, root)

    click.echo()
    click.echo(click.style(f"✓ Initialized dlt-ops project at {root.resolve()}", fg="green", bold=True))
    click.echo()
    for item in created:
        click.echo(f"  {click.style(item, fg='cyan')}")
    click.echo()

    click.echo(click.style("Next steps:", bold=True))
    step = 1
    if with_example:
        click.echo(f"  {step}. Try the example: dlt-ops pipeline run -s {EXAMPLE_SOURCE_SECTION} -y")
        step += 1
    else:
        click.echo(
            f"  {step}. Write a source: {pipeline_name}/{SOURCE_DIR}/<section>.py "
            f"(module stem = [sources.<section>] in .dlt/config.toml)"
        )
        step += 1
    click.echo(f"  {step}. Validate the project: dlt-ops pipeline validate")
    click.echo()
