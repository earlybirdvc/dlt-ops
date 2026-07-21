---
description: Task guide — kill a paginated extract mid-run, watch the checkpoint survive in _dlt_custom_checkpoints, resume without re-extracting the window, then count what landed to see why checkpoint resume is deliberately not a full replay.
---

# Resume a failed run from a checkpoint

This guide kills a paginated extract partway through, watches the checkpoint survive in the destination, and resumes without re-extracting the window — then counts exactly what landed, because checkpoint resume is deliberately not a full replay. It uses the repository's example project and its built-in fault-injection hook; the [checkpoints concept page](../concepts/checkpoints.md) covers the semantics this guide exercises.

**Prerequisites**

- A checkout of the [dlt-ops repository](https://github.com/earlybirdvc/dlt-ops) — the guide runs its `examples/basic_project`.
- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md). DuckDB is full tier, so checkpoints persist to `_dlt_custom_checkpoints`.

**Steps at a glance**

1. [Start from the example project](#1-start-from-the-example-project)
2. [Kill the API mid-pagination](#2-kill-the-api-mid-pagination)
3. [Inspect the surviving checkpoint](#3-inspect-the-surviving-checkpoint)
4. [Re-run to resume](#4-re-run-to-resume)
5. [Read both outcomes in the ledger](#5-read-both-outcomes-in-the-ledger)
6. [Count what actually landed — resume is not a replay](#6-count-what-actually-landed-resume-is-not-a-replay)

![Terminal recording of the checkpoint-resume flow: a run with GITHUB_EVENTS_FAIL_AFTER_PAGE=3 dies mid-pagination, checkpoints list shows the surviving active checkpoint, the re-run resumes from it instead of restarting the window, and pipeline status keeps the failed and completed outcomes side by side](../assets/terminal/checkpoint-resume.gif)

*Steps 1–5 as one real recorded run — regenerate it with `tapes/render.sh` from a repository checkout.*

## 1. Start from the example project

**The example project is self-contained — copy it out of the checkout and work there:**

```bash
cp -R examples/basic_project /tmp/dlt-demo
cd /tmp/dlt-demo
```

Its `events` resource (in `github_events/resource/events.py`) pairs an incremental cursor with checkpoints — `@with_checkpoints` sits under `@dlt.resource`, closest to the function:

```python
@dlt.resource(
    name="events",
    columns=Event,
    primary_key="id",
    write_disposition="append",
    schema_contract={"tables": "evolve", "columns": "freeze", "data_type": "freeze"},
)
@with_checkpoints(cursor_field="occurred_at", frequency=2)
def events(occurred_at=dlt.sources.incremental("occurred_at", initial_value=EVENTS_INITIAL_TIMESTAMP)):
    for page in FixtureClient("events.jsonl").pages(since=occurred_at.start_value):
        yield page
```

**Know the arithmetic before you break anything:**

- The fixture holds 24 rows, one per hour, but `initial_value` is `2026-01-01T00:00`, so the incremental window covers the 20 rows with ids 5–24 (`00:00`–`19:00`).
- The fixture client serves 3 rows per page, and `frequency=2` persists a checkpoint to the destination's `_dlt_custom_checkpoints` table every second page.
- The client also reads one environment variable, `GITHUB_EVENTS_FAIL_AFTER_PAGE` — the stand-in for an API dying mid-pagination.
- The source carries a second resource, `actors` (5 rows, full refresh), which matters later when you read record counts.

## 2. Kill the API mid-pagination

**Run with the fault-injection variable set so the "API" dies after page 3:**

```bash
GITHUB_EVENTS_FAIL_AFTER_PAGE=3 dlt-ops pipeline run -s github_events_api -y
```

```text
[events] Run isolation: value=2026-01-01T00:00:00+00:00, run_id=2db0bb0b60653f75
[events] Checkpoint saved: page 2, 6 records, value: 2026-01-01T05:00:00+00:00
RuntimeError: injected API failure after page 3 (GITHUB_EVENTS_FAIL_AFTER_PAGE)
```

The run exits 1 at the extract step. Three pages were served (9 rows), one checkpoint was written — after page 2, recording the page's maximum cursor value `05:00` — and then the "API" died.

Because the failure happened during extract, dlt discards the partial package: nothing from this run reached the `events` table. The checkpoint did reach the destination, though — it is written in its own SQL statement, outside dlt's load transaction, which is the entire point.

## 3. Inspect the surviving checkpoint

**List the checkpoints for this pipeline — one `active` row survived the failed run:**

```bash
dlt-ops checkpoints list --pipeline github_events_api_pipeline
```

```text
Found 1 checkpoint(s):

Resource             RunID      Checkpoint                Pages    Records    Status     Created
------------------------------------------------------------------------------------------------------------------------
events               2db0bb0b   2026-01-01T05:00:00+00:00 2        6          active 2026-07-16 19:59:53.687949+00:00
```

`active` means "a run over this window did not finish — resume from here". The `RunID` is not random: it hashes the incremental window's start value, so an hourly run and a backfill chunk over a different window keep separate resume points instead of poisoning each other's — [checkpoints](../concepts/checkpoints.md) covers the isolation rule.

## 4. Re-run to resume

**Run the same command without the fault:**

```bash
dlt-ops pipeline run -s github_events_api -y
```

```text
[events] Run isolation: value=2026-01-01T00:00:00+00:00, run_id=2db0bb0b60653f75
[events] Resuming from checkpoint: 2026-01-01T05:00:00+00:00 (adjusted: 2026-01-01 04:59:59+00:00)
[events] Checkpoint saved: page 2, 6 records, value: 2026-01-01T10:00:00+00:00
[events] Checkpoint saved: page 4, 12 records, value: 2026-01-01T16:00:00+00:00
[events] Checkpoints marked as completed
[events] Cleaned up checkpoints older than 7 days
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

On entry the decorator found the active checkpoint for this window and overrode the incremental's start value with the checkpoint minus a one-second safety overlap (`adjusted: 04:59:59`), so a cursor tie exactly at the checkpoint cannot be skipped. The resumed extract paged through the remaining 15 rows instead of all 20, checkpointed twice along the way, and on success flipped its checkpoint rows to `completed` — visible in a second `checkpoints list`:

```text
Found 3 checkpoint(s):

Resource             RunID      Checkpoint                Pages    Records    Status     Created
------------------------------------------------------------------------------------------------------------------------
events               2db0bb0b   2026-01-01T16:00:00+00:00 4        12         completed 2026-07-16 20:00:17.829975+00:00
events               2db0bb0b   2026-01-01T10:00:00+00:00 2        6          completed 2026-07-16 20:00:17.819601+00:00
events               2db0bb0b   2026-01-01T05:00:00+00:00 2        6          completed 2026-07-16 19:59:53.687949+00:00
```

Completed rows self-prune after `cleanup_days` (7 by default) on later successful runs. `dlt-ops checkpoints cleanup --pipeline github_events_api_pipeline` is the manual version of the same housekeeping: it deletes the rows a successful run already marked `completed`, and nothing else. `active` rows are live resume state — the row a crashed or still-running extract resumes from — so they are kept and reported at WARNING with a count, not silently skipped. Narrow the scope with `--resource <r>`.

`--include-active` is the destructive form, and the only one that can cost you data movement: it deletes every checkpoint row in scope regardless of status, so each affected resource restarts its window from the beginning and re-extracts everything the previous run already loaded. Reach for it deliberately — to abandon a resume point that is poisoned, or to clear state for a pipeline dropped outside `dlt-ops`.

## 5. Read both outcomes in the ledger

**`status` shows the failed attempt and the successful resume side by side:**

```bash
dlt-ops pipeline status
```

```text
Source: github_events_api
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-16 20:00:17  2026-07-16 20:00:18  20        cli        -               e56b1d3f6c75
  failed     2026-07-16 19:59:53  2026-07-16 19:59:53  -         cli        -               92235a30f022
    ✗ PipelineStepFailed: Pipeline execution failed at `step=extract` ... injected API failure after page 3 (GITHUB_EVENTS_FAIL_AFTER_PAGE)

Source: github_events_full
  no runs recorded
```

The [runs ledger](../concepts/runs-ledger.md) keeps the failed attempt next to the successful resume. `Records 20` for the resumed run counts both resources — 15 `events` rows plus the 5-row `actors` full refresh — and the untouched sibling source honestly reports `no runs recorded` rather than an empty table.

## 6. Count what actually landed — resume is not a replay

**Now the part worth internalizing.** The window holds 20 event rows; count what is in the destination:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events_api_pipeline.duckdb", read_only=True)
con.sql("SET TimeZone = 'UTC'")
print(con.sql("SELECT count(*) AS events_loaded, min(id) AS min_id, max(id) AS max_id, min(occurred_at) AS earliest FROM github_events_raw.events"))
PY
```

```text
┌───────────────┬────────┬────────┬──────────────────────────┐
│ events_loaded │ min_id │ max_id │         earliest         │
├───────────────┼────────┼────────┼──────────────────────────┤
│            15 │     10 │     24 │ 2026-01-01 05:00:00+00   │
└───────────────┴────────┴────────┴──────────────────────────┘
```

Fifteen rows, not twenty. Ids 5–9 (`00:00`–`04:00`) were extracted by the failed run, never loaded (dlt dropped the partial package), and sit *below* the checkpoint — so the resume, which trusts the checkpoint rather than checking the destination, never went back for them. After the successful run they are also below dlt's advanced incremental cursor, so no later scheduled run requests them either.

!!! warning "Resume is lossy in the extract-failure case — by design"
    A checkpoint records extraction progress, not load outcomes. When a run dies during extract, everything it extracted below the checkpoint is skipped on resume — that is the designed trade of resume cost against completeness, and [checkpoints](../concepts/checkpoints.md) specifies it per failure domain (a run that dies during normalize or load loses nothing: the completed package stays pending and dlt retries it). The recovery path for a skipped span is a windowed [backfill](backfill.md) over it — on this example source you would first give `actors` a cursor or split it out, since backfill refuses sources with cursor-less resources. `frequency` is the dial: checkpoint more often and resume gets cheaper while the skippable span grows; checkpoint rarely and it is the reverse. Use checkpoints where re-extraction is the real pain — rate-limited or paid APIs, multi-hour paginations — and reach for backfill where every row is load-bearing.

## Troubleshooting: `@with_checkpoints` on top of `@dlt.resource`

**The decorator order is enforced, not conventional.** Applied on top of `@dlt.resource` it would silently replace the resource with a plain generator — so it raises at import time instead, and `validate` surfaces it before any run does:

```text
✗ 4 error(s):
  [github_events_api] import: source module github_events_api.py: module raised at import: TypeError: @with_checkpoints must be applied under @dlt.resource, not on top of it: decorate the plain generator function and let @dlt.resource wrap the result. Applied on top, it replaces the DltResource with a plain generator function, dropping the resource's name, write disposition, and hints.
  [github_events_full] import: source module github_events_full.py: module raised at import: TypeError: @with_checkpoints must be applied under @dlt.resource, not on top of it: ...
  [github_events_api] validation_coverage: reduced rule coverage: source 'github_events_api' failed Phase-2 introspection, so it is absent from the introspected source set every source-inspecting rule iterates — those rules did not run for it. Its config, schema, resource and assertion findings are unknown, not clean. Fix the 'import' finding reported for this source to restore full coverage.
  [github_events_full] validation_coverage: reduced rule coverage: source 'github_events_full' failed Phase-2 introspection, so it is absent from the introspected source set every source-inspecting rule iterates — those rules did not run for it. Its config, schema, resource and assertion findings are unknown, not clean. Fix the 'import' finding reported for this source to restore full coverage.
```

Every module importing the broken resource reports the same error — the example's two sources share `resource/events.py`, so both fail at once. Each also gets a `validation_coverage` error, because a source excluded from Phase 2 is skipped by every rule that iterates sources: four findings, one root cause. Put `@with_checkpoints` directly on the generator function and let `@dlt.resource` wrap the result.

## Where next

- [Checkpoints](../concepts/checkpoints.md) — the full semantics: what a checkpoint claims, per-failure-domain behavior, the run-isolation rule
- [Backfill a window](backfill.md) — the recovery path for a skipped span, and per-chunk checkpoint namespaces
- [Destinations and tiers](../concepts/destinations-and-tiers.md) — why checkpoints need a full-tier destination
