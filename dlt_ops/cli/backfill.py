"""`dlt-ops pipeline backfill` — chunked, resumable backfill (CR1-3 locked).

Splits ``[--from, --to)`` into sequential chunks; each chunk is its own
``run_pipeline`` call with injected bounds, a deterministic per-chunk
``run_id``, and its own ``_dlt_ops_runs`` row
(``trigger_source="backfill"``). Chunk progress lives in ``_dlt_backfills``
(see ``dlt_ops.runs.backfill_state``): re-running the same
``--from --to --chunk`` triple skips completed chunks, retries failed ones,
and continues pending ones. Concurrent invocations on the same backfill
coordinate via optimistic CAS claiming only; a chunk another worker holds is
skipped here and reported — the invocation exits non-zero, because it did not
cover the window it was asked to cover.

Locked semantics enforced before any chunk runs:
- bounds parsable, timezone-aware (naive rejected), normalized to UTC;
- chunk size > 0, simple ``<N>d`` / ``<N>h`` / ``<N>m`` forms only;
- source import-safe (Phase-2 introspect verdict, Rule 15);
- every selected resource declares an incremental cursor (Tier-2 preflight
  condition 5 — without one the injected interval is silently ignored);
- destination at full tier — chunk state in ``_dlt_backfills`` is
  adapter-gated, so a destination with no registered ``DestinationAdapter``
  is refused at preflight (Tier-2 condition 2), before any chunk math.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import attrs
import click

from dlt_ops.cli._common import _discover_with_progress, resolve_cli_project_root
from dlt_ops.config import (
    ProjectConfigError,
    load_project_config,
    load_raw_config,
    resolve_dataset,
    resolve_destination,
)
from dlt_ops.discovery import discover, introspect
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.preflight import PreflightError, run_preflight
from dlt_ops.runs.backfill_state import (
    BackfillStateError,
    ChunkStatus,
    backfill_id_for,
    chunk_id_for,
    chunk_run_id,
    default_claim_token,
    open_backfill_state,
)
from dlt_ops.runs.writer import TriggerSource, record_counts_from_trace, summarize_error

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from dlt_ops.discovery import SourceInfo

logger = logging.getLogger(__name__)

_CHUNK_RE = re.compile(r"^(\d+)([dhm])$")
_CHUNK_UNITS = {"d": "days", "h": "hours", "m": "minutes"}
_CHUNK_FORMS_HINT = "simple <N>d / <N>h / <N>m forms only (e.g. 7d, 24h, 30m)"


class BackfillUsageError(ValueError):
    """Backfill arguments violate the locked execution contract; nothing ran."""


class BackfillChunkError(RuntimeError):
    """A chunk run failed; it is marked ``failed`` and the invocation stops."""


def parse_utc_timestamp(raw: str, option: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize to UTC; naive inputs rejected.

    Raises:
        BackfillUsageError: unparsable, or no explicit timezone offset.
    """
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise BackfillUsageError(
            f"{option} {raw!r} is not a parsable ISO-8601 timestamp (e.g. 2024-01-01T00:00:00Z)"
        ) from exc
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise BackfillUsageError(
            f"{option} {raw!r} is timezone-naive; pass an explicit offset "
            f"(e.g. {raw}Z or {raw}+00:00) — bounds are normalized to UTC"
        )
    return parsed.astimezone(UTC)


def parse_chunk_interval(raw: str) -> tuple[timedelta, str]:
    """Parse ``--chunk`` into ``(timedelta, canonical label)``.

    Only the simple forms are accepted — ISO-8601 durations, week/second
    units, fractions, and whitespace are rejected loudly so two spellings of
    one interval can't fork into two backfill ids.

    Raises:
        BackfillUsageError: not a ``<N>d`` / ``<N>h`` / ``<N>m`` form, or N == 0.
    """
    match = _CHUNK_RE.fullmatch(raw.strip())
    if match is None:
        raise BackfillUsageError(f"invalid --chunk {raw!r}: {_CHUNK_FORMS_HINT}")
    count, unit = int(match.group(1)), match.group(2)
    if count <= 0:
        raise BackfillUsageError(f"invalid --chunk {raw!r}: chunk size must be > 0")
    return timedelta(**{_CHUNK_UNITS[unit]: count}), f"{count}{unit}"


def check_window(window_from: datetime, window_to: datetime) -> None:
    """Reject empty/inverted ``[from, to)`` windows.

    Raises:
        BackfillUsageError: ``window_from`` is not strictly before ``window_to``.
    """
    if window_from >= window_to:
        raise BackfillUsageError(
            f"empty window: --from {window_from.isoformat()} must be before --to {window_to.isoformat()} "
            f"(bounds are [from, to))"
        )


def compute_chunks(window_from: datetime, window_to: datetime, chunk: timedelta) -> list[tuple[datetime, datetime]]:
    """Split ``[window_from, window_to)`` into consecutive ``[from, to)`` chunks.

    The last chunk is clamped to ``window_to`` when the window doesn't divide
    evenly, so chunk bounds never extend past the requested window.
    """
    chunks: list[tuple[datetime, datetime]] = []
    start = window_from
    while start < window_to:
        end = min(start + chunk, window_to)
        chunks.append((start, end))
        start = end
    return chunks


@attrs.frozen
class BackfillSummary:
    """Per-invocation chunk tally.

    ``lost`` counts chunks another worker's token held that were still not
    ``completed`` when the invocation ended — the window is not covered and
    this invocation cannot say when it will be, which is why a non-zero
    ``lost`` denies the command its clean exit (see :func:`backfill`). A chunk
    another worker did finish counts as ``skipped``: covered is covered,
    whoever ran it. A claim that never applied is not counted here at all —
    it raises :class:`~dlt_ops.runs.backfill_state.BackfillStateError` and
    stops the invocation, because then *nobody* holds that chunk.
    """

    backfill_id: str
    total: int
    completed: int
    skipped: int
    lost: int

    @property
    def window_covered(self) -> bool:
        """Every chunk of the window is accounted for by this invocation."""
        return self.completed + self.skipped == self.total


def _check_import_safe(source: SourceInfo) -> None:
    """Rule 15 entry gate — the Phase-2 introspect verdict must be clean.

    Raises:
        BackfillUsageError: the module failed to import, or violated import
            safety (network I/O / disk writes at import time).
    """
    if not source.is_introspected:
        raise BackfillUsageError(
            f"source {source.name!r} failed to import: {source.import_error or 'not introspected'}"
        )
    if source.import_violations:
        findings = ", ".join(f"{v.kind} ({v.event}: {v.target})" for v in source.import_violations)
        raise BackfillUsageError(
            f"source {source.name!r} violates import safety (Rule 15) at import time: {findings}. "
            f"Fix the module or opt out via [dlt_ops.rules] import_safety = false."
        )


def execute_backfill(
    source: SourceInfo,
    *,
    project_root: Path,
    window_from: datetime,
    window_to: datetime,
    chunk: timedelta,
    chunk_label: str,
    claimed_by: str | None = None,
    run_fn: Callable[..., Any] = run_pipeline,
    echo: Callable[[str], None] = click.echo,
) -> BackfillSummary:
    """Run one backfill invocation: validate, seed chunk state, execute sequentially.

    Enforcement (import safety, config-chain resolution, Tier-2 preflight
    including the incremental-cursor and destination-capability conditions —
    backfill declares its chunk state as adapter-gated, so an adapter-less
    destination is refused here) all happens before any chunk runs or any
    state row is written. Chunks execute sequentially; a chunk another worker
    already holds is counted in ``lost`` and skipped, a chunk failure marks the
    row ``failed`` and stops the invocation — re-running the same triple
    resumes. A claim that never applied stops the invocation too: no worker
    holds that chunk, so continuing would leave its window unbackfilled.

    Args:
        source: Introspected SourceInfo (import-safe, callable attached).
        project_root: Project root holding .dlt/config.toml.
        window_from: Inclusive UTC window start.
        window_to: Exclusive UTC window end.
        chunk: Chunk interval.
        chunk_label: Canonical ``--chunk`` spelling (backfill-id input).
        claimed_by: Worker token for CAS claiming; ``host:pid`` when None.
        run_fn: Chunk executor (run_pipeline signature); injectable for tests.
        echo: Progress sink (``chunk N/total`` lines).

    Raises:
        BackfillUsageError: window empty/inverted or source import-unsafe.
        PreflightError: a Tier-2 condition is violated (incl. missing
            incremental cursor, and a destination with no registered
            DestinationAdapter — chunk state needs one).
        ProjectConfigError: destination/dataset don't resolve.
        BackfillChunkError: a chunk run raised; state marked ``failed``.
        BackfillStateError: a chunk's CAS claim never applied — a destination
            failure that would otherwise skip that chunk's time window.
    """
    check_window(window_from, window_to)
    _check_import_safe(source)
    project_config = load_project_config(project_root)
    destination = resolve_destination(source.config, project_config)
    dataset = resolve_dataset(source.config, project_config)
    run_preflight(
        destination=destination,
        project_config=project_config,
        source=source.source_fn(),
        bounds=(window_from, window_to),
        raw_config=load_raw_config(project_root),
        source_section=source.name,
        adapter_required_for="backfill (chunk state in _dlt_backfills)",
    )

    chunks = compute_chunks(window_from, window_to, chunk)
    backfill_id = backfill_id_for(source.name, window_from, window_to, chunk_label)
    token = claimed_by or default_claim_token()
    total = len(chunks)
    completed = skipped = 0
    lost_chunk_ids: list[str] = []

    with open_backfill_state(source.name, destination, dataset) as state:
        state.ensure_table()
        state.seed_chunks(
            backfill_id=backfill_id,
            chunks=chunks,
            backfill_from=window_from,
            backfill_to=window_to,
            chunk_size=chunk_label,
        )
        status_by_chunk = {row.chunk_id: row.status for row in state.fetch_chunks(backfill_id)}

        for index, (chunk_from, chunk_to) in enumerate(chunks):
            chunk_id = chunk_id_for(index)
            label = f"chunk {index + 1}/{total} [{chunk_from.isoformat()} → {chunk_to.isoformat()})"
            if status_by_chunk.get(chunk_id) == ChunkStatus.COMPLETED:
                skipped += 1
                echo(f"{label}: already completed, skipping")
                continue
            if not state.claim(backfill_id, chunk_id, claimed_by=token):
                lost_chunk_ids.append(chunk_id)
                echo(f"{label}: held by another worker, NOT run by this invocation")
                continue
            state.mark_running(backfill_id, chunk_id, claimed_by=token)
            run_id = chunk_run_id(source.name, chunk_from, chunk_to)
            echo(f"{label}: running (run_id={run_id})")
            try:
                pipeline = run_fn(
                    source,
                    project_root=project_root,
                    destination=destination,
                    dataset_name=dataset,
                    bounds=(chunk_from, chunk_to),
                    run_id=run_id,
                    backfill_id=backfill_id,
                    trigger_source=TriggerSource.BACKFILL,
                )
            except Exception as exc:
                state.mark_failed(backfill_id, chunk_id, claimed_by=token)
                raise BackfillChunkError(
                    f"{label} failed: {summarize_error(exc)}. Completed chunks are recorded — "
                    f"re-run the same --from/--to/--chunk to resume."
                ) from exc
            except BaseException:
                # KeyboardInterrupt / SystemExit skip the handler above. `running`
                # sits outside the CAS target set, so a row abandoned there can
                # never be reclaimed and the chunk is stranded for good. Demote it
                # to `failed` — which a re-run does reclaim — then let the
                # interrupt through untouched.
                state.mark_failed(backfill_id, chunk_id, claimed_by=token)
                raise
            _, records_loaded = record_counts_from_trace(getattr(pipeline, "last_trace", None))
            state.mark_completed(backfill_id, chunk_id, claimed_by=token, records_loaded=records_loaded)
            completed += 1
            echo(f"{label}: completed ({records_loaded if records_loaded is not None else '?'} records)")

        # A chunk held elsewhere may have finished while this invocation worked
        # through the rest of the window. Re-read once so `lost` means "still
        # not covered by anyone" — the only thing worth denying a clean exit —
        # rather than "was busy the moment I looked at it".
        lost = len(lost_chunk_ids)
        if lost_chunk_ids:
            final_status = {row.chunk_id: row.status for row in state.fetch_chunks(backfill_id)}
            covered = [c for c in lost_chunk_ids if final_status.get(c) == ChunkStatus.COMPLETED]
            skipped += len(covered)
            lost -= len(covered)

    return BackfillSummary(backfill_id=backfill_id, total=total, completed=completed, skipped=skipped, lost=lost)


def _introspect_all(project_root: Path) -> dict[str, SourceInfo]:
    """Two-phase discovery WITH verdicts — unlike the composite, sources whose
    module failed to import (or violated Rule 15) stay in the map so backfill
    can reject them with the actual reason instead of "unknown source"."""
    return introspect(project_root, discover(project_root))


@click.command()
@click.argument("source_name", metavar="SOURCE")
@click.option(
    "--from",
    "window_from_raw",
    required=True,
    metavar="TIMESTAMP",
    help="Window start, inclusive. ISO-8601 with explicit timezone offset; normalized to UTC.",
)
@click.option(
    "--to",
    "window_to_raw",
    required=True,
    metavar="TIMESTAMP",
    help="Window end, exclusive ([from, to)). ISO-8601 with explicit timezone offset.",
)
@click.option(
    "--chunk",
    "chunk_raw",
    required=True,
    metavar="INTERVAL",
    help=f"Chunk size: {_CHUNK_FORMS_HINT}.",
)
@click.pass_context
def backfill(ctx: click.Context, source_name: str, window_from_raw: str, window_to_raw: str, chunk_raw: str) -> None:
    """Backfill a source over [--from, --to) in sequential chunks.

    Each chunk runs the pipeline with injected [chunk_from, chunk_to) bounds
    and writes its own _dlt_ops_runs row. Progress is tracked in
    _dlt_backfills on the source's own destination: re-running the same
    --from/--to/--chunk triple skips completed chunks, retries failed ones,
    and continues pending ones. Concurrent invocations coordinate via the
    state table — every chunk executes exactly once.

    Exit code 0 means this invocation covered every chunk of the window
    (completed here, or already completed by an earlier run). Chunks another
    worker holds are reported and exit non-zero: they did not run here, so this
    invocation cannot vouch for the window.

    Timestamps must carry an explicit timezone offset (naive inputs are
    rejected); the source's selected resources must declare an incremental
    cursor, or the injected bounds would be silently ignored; and the
    destination must have a registered DestinationAdapter — chunk state is
    adapter-gated, so a core-mode destination is refused before anything runs.

    Examples:
        dlt-ops pipeline backfill github_events --from 2024-01-01T00:00:00Z --to 2025-01-01T00:00:00Z --chunk 7d
        dlt-ops pipeline backfill my_api --from 2024-06-01T00:00:00+00:00 --to 2024-06-02T00:00:00+00:00 --chunk 6h
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s|[%(levelname)s]|%(name)s|%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        window_from = parse_utc_timestamp(window_from_raw, "--from")
        window_to = parse_utc_timestamp(window_to_raw, "--to")
        chunk, chunk_label = parse_chunk_interval(chunk_raw)
        check_window(window_from, window_to)
    except BackfillUsageError as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        sys.exit(1)

    project_root = resolve_cli_project_root(ctx)
    sources = _discover_with_progress(project_root, _introspect_all)

    if source_name not in sources:
        click.echo(click.style(f"Error: Unknown source '{source_name}'", fg="red"), err=True)
        click.echo()
        click.echo("Available sources:")
        for name in sorted(sources.keys()):
            click.echo(f"  {click.style(name, fg='cyan')}")
        sys.exit(1)

    click.echo()
    try:
        summary = execute_backfill(
            sources[source_name],
            project_root=project_root,
            window_from=window_from,
            window_to=window_to,
            chunk=chunk,
            chunk_label=chunk_label,
        )
    except (BackfillUsageError, PreflightError, ProjectConfigError) as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        sys.exit(1)
    except (BackfillChunkError, BackfillStateError) as e:
        click.echo(click.style(f"Error: {e}", fg="red"), err=True)
        sys.exit(1)

    tally = (
        f"Backfill {summary.backfill_id}: {summary.completed} completed, {summary.skipped} skipped, "
        f"{summary.lost} claimed elsewhere ({summary.total} chunks)"
    )
    click.echo()
    if summary.window_covered:
        click.echo(click.style(tally, fg="green", bold=True))
        return

    # Chunks held by another worker never ran here, so this invocation cannot
    # report the window as backfilled — green + exit 0 would tell an operator
    # the window is covered when it may not be.
    click.echo(click.style(tally, fg="yellow", bold=True))
    click.echo(
        click.style(
            f"Error: {summary.lost} of {summary.total} chunk(s) were held by another worker and did not run "
            f"here, so this invocation did not cover the whole window. Re-run the same --from/--to/--chunk "
            f"once the other worker has finished — it exits 0 only when every chunk is completed.",
            fg="red",
        ),
        err=True,
    )
    sys.exit(1)
