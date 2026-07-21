"""Shared CLI plumbing used by every verb module (pipeline, status, backfill)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import click

from dlt_ops.config import ProjectConfigError, find_project_root
from dlt_ops.discovery import discover_sources

if TYPE_CHECKING:
    from collections.abc import Callable

    from dlt_ops.discovery import SourceInfo


def resolve_cli_project_root(ctx: click.Context) -> Path:
    """Resolve the project root from the click context or by walking up from cwd.

    The CLI's fatal path around ``dlt_ops.config.find_project_root``:
    prints the typed error (which carries the ``dlt-ops init`` hint on a
    miss) and ``sys.exit(1)``.
    """
    try:
        return find_project_root(explicit=ctx.obj.get("project_root"))
    except ProjectConfigError as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        sys.exit(1)


def _discover_with_progress(
    project_root: Path,
    discover_fn: Callable[[Path], dict[str, SourceInfo]] = discover_sources,
) -> dict[str, SourceInfo]:
    """Discover sources with a progress indicator.

    Read-only verbs (list/resources) pass Phase-1 ``discover`` — pure AST,
    never imports source code. Execution verbs (run/clean) keep the default
    composite ``discover_sources``, which attaches imported callables.
    """
    with click.progressbar(
        length=1,
        label=click.style("Discovering sources", fg="cyan"),
        show_eta=False,
        show_percent=False,
        fill_char=click.style("█", fg="cyan"),
        empty_char="░",
    ) as bar:
        sources = discover_fn(project_root)
        bar.update(1)
    return sources
