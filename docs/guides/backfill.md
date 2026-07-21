---
description: Task guide — backfill a window in resumable daily chunks with dlt-ops pipeline backfill, verify each chunk lands exactly once, prove that re-running resumes instead of duplicating, and handle the cursor-less-resource refusal.
---

# Backfill a window

This guide backfills a five-day window in daily chunks, verifies each chunk landed exactly once, re-runs the command to prove it resumes instead of duplicating — and then runs into the one refusal every new user hits: a resource without an incremental cursor. The [backfill concept page](../concepts/backfill.md) covers the chunk state machine, deterministic identities, and concurrency; this page is the task.

**Prerequisites**

- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md).
- Runs fully offline, no credentials. DuckDB is full tier, so the runs ledger and the `_dlt_backfills` chunk state are available.

**Steps at a glance**

1. [Start state: a source with an incremental cursor](#1-start-state-a-source-with-an-incremental-cursor)
2. [Run the backfill](#2-run-the-backfill)
3. [Verify the window landed once](#3-verify-the-window-landed-once)
4. [Re-run: resume, not duplicate](#4-re-run-resume-not-duplicate)
5. [The refusal: a cursor-less resource](#5-the-refusal-a-cursor-less-resource)

![Terminal recording of a chunked backfill on the web_events source: five daily chunks run in window order, the WEB_EVENTS_FAIL_FROM hook kills chunk 3 mid-backfill, and re-running the same command skips the two completed chunks, retries the failed one under its original run_id, and finishes the pending tail](../assets/terminal/backfill.gif)

*A real recorded run of this page's source, with the fault-injection hook armed for chunk 3's window on the first pass (the killed-and-resumed variant [the concept page walks through](../concepts/backfill.md)) — regenerate it with `tapes/render.sh` from a repository checkout.*

## 1. Start state: a source with an incremental cursor

**Backfill works by injecting `[chunk_from, chunk_to)` bounds into each chunk's run, and only a `dlt.sources.incremental` cursor makes a resource _observe_ those bounds** — without one, every chunk would silently re-extract the whole source. The scaffolded demo's resource has no cursor (that is the refusal in step 5), so add a minimal source that does. Scaffold and add a second pipeline directory:

```bash
dlt-ops init demo --example
cd demo
mkdir -p web/source
```

`web/source/web_events.py` — six rows, one per day at noon, and a cursor on `occurred_at` (the same source the [concept page](../concepts/backfill.md) uses, fault-injection hook included):

```python
"""Backfillable source: every selected resource declares an incremental cursor."""

import os
from datetime import UTC, datetime

import dlt
import pydantic


class PageView(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    occurred_at: datetime


_ROWS = [{"id": n, "occurred_at": datetime(2026, 1, n, 12, 0, tzinfo=UTC)} for n in range(1, 7)]

# Fault-injection hook for the resume demo: set to a chunk's start timestamp
# (ISO-8601) and the "API" dies when that chunk runs.
FAIL_FROM_ENV = "WEB_EVENTS_FAIL_FROM"


@dlt.resource(name="page_views", columns=PageView, primary_key="id", write_disposition="append")
def page_views(occurred_at=dlt.sources.incremental("occurred_at", initial_value=datetime(2020, 1, 1, tzinfo=UTC))):
    if os.environ.get(FAIL_FROM_ENV) == occurred_at.start_value.isoformat():
        raise RuntimeError(f"injected API failure for window starting {occurred_at.start_value}")
    yield _ROWS


@dlt.source(name="web_events")
def web_events_source():
    return page_views
```

Declare it in `.dlt/config.toml` — `@manual` is the honest schedule for a source you only drive by hand:

```toml
[sources.web_events]

[sources.web_events.dlt_ops]
schedule = "@manual"
dataset = "web_raw"
```

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✓ All sources validated successfully
```

## 2. Run the backfill

**Five days, one chunk per day.** Bounds are `[from, to)` — start inclusive, end exclusive — and must carry an explicit timezone offset; `--chunk` takes `<N>d` / `<N>h` / `<N>m`:

```bash
dlt-ops pipeline backfill web_events --from 2026-01-01T00:00:00Z --to 2026-01-06T00:00:00Z --chunk 1d
```

```text
chunk 1/5 [2026-01-01T00:00:00+00:00 → 2026-01-02T00:00:00+00:00): running (run_id=2fe712c1ca2c99a0)
chunk 1/5 [2026-01-01T00:00:00+00:00 → 2026-01-02T00:00:00+00:00): completed (1 records)
chunk 2/5 [2026-01-02T00:00:00+00:00 → 2026-01-03T00:00:00+00:00): running (run_id=91f2aafcc3320c98)
chunk 2/5 [2026-01-02T00:00:00+00:00 → 2026-01-03T00:00:00+00:00): completed (1 records)
chunk 3/5 [2026-01-03T00:00:00+00:00 → 2026-01-04T00:00:00+00:00): running (run_id=559264ef1526bd83)
chunk 3/5 [2026-01-03T00:00:00+00:00 → 2026-01-04T00:00:00+00:00): completed (1 records)
chunk 4/5 [2026-01-04T00:00:00+00:00 → 2026-01-05T00:00:00+00:00): running (run_id=cf57c9a304c61cb8)
chunk 4/5 [2026-01-04T00:00:00+00:00 → 2026-01-05T00:00:00+00:00): completed (1 records)
chunk 5/5 [2026-01-05T00:00:00+00:00 → 2026-01-06T00:00:00+00:00): running (run_id=2398048a9866e857)
chunk 5/5 [2026-01-05T00:00:00+00:00 → 2026-01-06T00:00:00+00:00): completed (1 records)

Backfill d7a7c6e21e0d8ecb: 5 completed, 0 skipped, 0 claimed elsewhere (5 chunks)
```

Chunks run sequentially, in window order, and each one is a full pipeline run — Tier-2 preflight, assertions, and a [runs-ledger](../concepts/runs-ledger.md) row apply per chunk. Each daily chunk found exactly the one row inside its window (`1 records`). The `run_id`s are deterministic hashes of the chunk bounds and `d7a7c6e21e0d8ecb` names the whole backfill — the identities that make step 4 work.

## 3. Verify the window landed once

**Rows first** — the source emits six, ids 1–6 at noon each day:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("web_events_pipeline.duckdb", read_only=True)
con.sql("SET TimeZone = 'UTC'")
print(con.sql("SELECT id, occurred_at FROM web_raw.page_views ORDER BY id"))
print(con.sql("SELECT chunk_id, status, records_loaded, run_id FROM web_raw._dlt_backfills ORDER BY chunk_id"))
PY
```

```text
┌───────┬──────────────────────────┐
│  id   │       occurred_at        │
├───────┼──────────────────────────┤
│     1 │ 2026-01-01 12:00:00+00   │
│     2 │ 2026-01-02 12:00:00+00   │
│     3 │ 2026-01-03 12:00:00+00   │
│     4 │ 2026-01-04 12:00:00+00   │
│     5 │ 2026-01-05 12:00:00+00   │
└───────┴──────────────────────────┘

┌──────────┬───────────┬────────────────┬──────────────────┐
│ chunk_id │  status   │ records_loaded │      run_id      │
├──────────┼───────────┼────────────────┼──────────────────┤
│ 000000   │ completed │              1 │ 2fe712c1ca2c99a0 │
│ 000001   │ completed │              1 │ 91f2aafcc3320c98 │
│ 000002   │ completed │              1 │ 559264ef1526bd83 │
│ 000003   │ completed │              1 │ cf57c9a304c61cb8 │
│ 000004   │ completed │              1 │ 2398048a9866e857 │
└──────────┴───────────┴────────────────┴──────────────────┘
```

Five rows, once each — and no id 6: its row sits at `2026-01-06T12:00`, past the exclusive `--to`. That is exactly how a backfill hands off to the daily schedule without overlap or gap. The second table is `_dlt_backfills`, the per-chunk state living in the source's own dataset; `completed` there is what makes a chunk skippable. The ledger has the same five chunks as run history, `trigger_source = backfill`:

```bash
dlt-ops pipeline status --limit 5
```

```text
Source: demo_events
  no runs recorded

Source: web_events
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-16 20:02:09  2026-07-16 20:02:10  1         backfill   -               2398048a9866
  completed  2026-07-16 20:02:08  2026-07-16 20:02:09  1         backfill   -               cf57c9a304c6
  completed  2026-07-16 20:02:07  2026-07-16 20:02:08  1         backfill   -               559264ef1526
  completed  2026-07-16 20:02:06  2026-07-16 20:02:07  1         backfill   -               91f2aafcc332
  completed  2026-07-16 20:02:05  2026-07-16 20:02:06  1         backfill   -               2fe712c1ca2c
```

## 4. Re-run: resume, not duplicate

**Run the exact same command again:**

```bash
dlt-ops pipeline backfill web_events --from 2026-01-01T00:00:00Z --to 2026-01-06T00:00:00Z --chunk 1d
```

```text
chunk 1/5 [2026-01-01T00:00:00+00:00 → 2026-01-02T00:00:00+00:00): already completed, skipping
chunk 2/5 [2026-01-02T00:00:00+00:00 → 2026-01-03T00:00:00+00:00): already completed, skipping
chunk 3/5 [2026-01-03T00:00:00+00:00 → 2026-01-04T00:00:00+00:00): already completed, skipping
chunk 4/5 [2026-01-04T00:00:00+00:00 → 2026-01-05T00:00:00+00:00): already completed, skipping
chunk 5/5 [2026-01-05T00:00:00+00:00 → 2026-01-06T00:00:00+00:00): already completed, skipping

Backfill d7a7c6e21e0d8ecb: 0 completed, 5 skipped, 0 claimed elsewhere (5 chunks)
```

The same `--from --to --chunk` triple always names the same backfill (`d7a7c6e21e0d8ecb` again), so re-running is a resume by construction: completed chunks skip, failed chunks retry under their original `run_id` (reusing their [checkpoint](../concepts/checkpoints.md) namespace), pending chunks run. That makes the recovery procedure for a backfill that died at chunk 3 of 40 one step — run the same command again — for every death the process can observe: a chunk that raised, and a `Ctrl-C` (which demotes the in-flight chunk to `failed` on its way out). The exception is a kill the process cannot trap (`SIGKILL`, OOM, eviction), which strands its chunk in `running`; the [concept page](../concepts/backfill.md) covers that bound and the manual fix. The [concept page](../concepts/backfill.md) walks a killed-and-resumed backfill through the state table (this source's `WEB_EVENTS_FAIL_FROM` hook lets you reproduce it), and covers how two concurrent invocations of the same command coordinate through chunk claims.

## 5. The refusal: a cursor-less resource

**Now the case you will actually hit.** The repository's `examples/basic_project` ships a source whose `events` resource has a cursor but whose `actors` resource does not — copy it out of a [repository](https://github.com/earlybirdvc/dlt-ops) checkout (the [checkpoint-resume guide](checkpoint-resume.md) starts from the same project) and try to backfill it:

```bash
cp -R examples/basic_project /tmp/dlt-demo
cd /tmp/dlt-demo
dlt-ops pipeline backfill github_events_api --from 2026-01-01T00:00:00Z --to 2026-01-02T00:00:00Z --chunk 6h
```

```text
Error: backfill bounds were supplied but resource(s) without an incremental cursor are selected: actors. Declare a dlt.sources.incremental cursor or deselect them.
```

This is Tier-2 preflight working as intended, not a limitation to route around: for a cursor-less resource the injected bounds are silently ignored, so each of your chunks would re-extract the *entire* resource while reporting a clean windowed run — a backfill that quietly multiplies source traffic by the chunk count. The refusal names the offending resources and fires before any chunk runs or any state row is written. The scaffolded demo's `demo_events` refuses identically, naming its cursor-less `events` resource.

There is no per-resource selection flag on `backfill` in v0.1, so the unit of refusal is the source. Your two fixes: declare a `dlt.sources.incremental` cursor on every resource in the source, or split cursor-less resources (lookup tables, full-refresh catalogs — things a backfill would not mean anything for anyway) into their own source and backfill the one that has cursors.

## Troubleshooting: timezone-naive bounds

Bounds without an explicit offset are rejected at parse time, before anything else:

```text
Error: --from '2026-01-01T00:00:00' is timezone-naive; pass an explicit offset (e.g. 2026-01-01T00:00:00Z or 2026-01-01T00:00:00+00:00) — bounds are normalized to UTC
```

A naive bound would mean something different on your laptop and on the scheduler's container, silently shifting the window — and since the bounds feed the backfill's identity, the same intended window could fork into two backfills. Always write `Z` or `+00:00` (any offset works; everything normalizes to UTC).

## Where next

- [Backfill](../concepts/backfill.md) — chunk identities, the `_dlt_backfills` state machine, failure and concurrency semantics
- [Checkpoints](../concepts/checkpoints.md) — how per-chunk checkpoint namespaces compose with backfill
- [Runs ledger](../concepts/runs-ledger.md) — reading chunk history back with `pipeline status`
