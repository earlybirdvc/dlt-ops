---
description: How dlt-ops records every run and backfill in a _dlt_ops_runs table in the destination — the column-level data model, the best-effort write policy, and why pipeline status distinguishes three kinds of "no rows".
---

# Runs ledger

dlt tells you what the last run on this machine did; nothing records what ran, when, and with what outcome where the data actually lands. The runs ledger closes that gap: every `run` and `backfill` writes start and outcome rows to a `_dlt_ops_runs` table in the destination itself, and `pipeline status` reads it back. Read this to understand the data model, the write policy, and why "no rows" comes in three distinct flavors.

**At a glance**

| What it is | When it applies | Requires | On failure | Canonical detail |
|---|---|---|---|---|
| A `_dlt_ops_runs` table in the destination — one row per run and per backfill chunk, read back by `pipeline status` | Written by every `run` and `backfill`; read by `status` | Full tier (a `DestinationAdapter`); core tier has nowhere to write it | Best-effort — a failed ledger write is logged, never fails a healthy run | The column table below; the [failure-semantics contract](failure-semantics.md) |

## The data model

**Each run inserts one row with `status = "running"` after Tier-2 preflight passes and before extract, then updates that same row to a terminal status when the run ends.** There is no separate outcome row — the ledger holds one row per run, and a row still reading `running` long after its `started_at` means the process died (or lost the destination) before the terminal write.

| Column | Type | Meaning |
|---|---|---|
| `pipeline_name` | VARCHAR, not null | The dlt pipeline name (`<source>_pipeline`) |
| `source_section` | VARCHAR, not null | Config-section name of the source |
| `resource_name` | VARCHAR | Set when the run was scoped to exactly one resource; NULL = source-level run |
| `destination` | VARCHAR, not null | Resolved destination (engine) name |
| `dataset` | VARCHAR, not null | Dataset the run wrote to |
| `run_id` | VARCHAR, not null | dlt-ops run id: random (uuid4 hex) for plain runs, deterministic per chunk for backfills |
| `dlt_run_id` | VARCHAR | dlt's load id — the join key into dlt's own `_dlt_loads` |
| `backfill_id` | VARCHAR | Reference into `_dlt_backfills`; NULL for plain runs |
| `trigger_source` | VARCHAR, not null | `cli`, `airflow`, `y-scheduler`, or `backfill` |
| `started_at` | TIMESTAMPTZ, not null | Start-row insert time (UTC) |
| `completed_at` | TIMESTAMPTZ | Terminal-update time; NULL while running |
| `status` | VARCHAR, not null | `running`, `completed`, `failed`, or `skipped` |
| `records_extracted` | BIGINT | From the run trace, dlt-internal tables excluded; NULL when unavailable |
| `records_loaded` | BIGINT | Same sourcing as `records_extracted` |
| `error_summary` | VARCHAR | One line, capped at 500 characters; the full trace stays in logs |

Two vocabulary notes, stated so you don't wait for rows that never come:

- **`y-scheduler` has no emitter in v0.1** — it is a reserved `trigger_source`; the CLI writes `cli`, the Airflow DAG factory writes `airflow`, and backfill chunks write `backfill`.
- **Nothing writes `status = "skipped"` in v0.1** — a backfill re-run skips completed chunks without touching the ledger, because each chunk's original row already says `completed`.

Backfills make the id columns earn their keep: every executed chunk is its own ledger row whose `run_id` is deterministic — `sha256("<source>|<chunk_from>|<chunk_to>")[:16]` — and whose `backfill_id` groups the chunks of one `--from --to --chunk` invocation. Two chunks from one backfill, read straight from the table:

```text
┌──────────────────┬──────────────────┬────────────────┬───────────┬────────────────┐
│      run_id      │   backfill_id    │ trigger_source │  status   │ records_loaded │
├──────────────────┼──────────────────┼────────────────┼───────────┼────────────────┤
│ 1ecfd38f15c66594 │ 6f220c98f337cabb │ backfill       │ completed │              3 │
│ fc90467a121bc253 │ 6f220c98f337cabb │ backfill       │ completed │              3 │
└──────────────────┴──────────────────┴────────────────┴───────────┴────────────────┘
```

The deterministic id is what makes re-running the same window a resume rather than a duplicate — [backfill](backfill.md) covers the chunk state machine in `_dlt_backfills`.

## Why the ledger lives in the destination

**The ledger is written to the same destination and dataset the run loads data into — state co-lives with data, dlt's own pattern.** That one decision buys three properties:

- **Stateless runners keep their history.** A cron job or CI runner that checks out the project fresh each time has no local state worth trusting; the ledger survives because it lives where the data lands, not on the machine that happened to trigger the run.
- **No cross-cutting credentials.** A run loading into Postgres never needs write access to some central status store; each destination carries its own `_dlt_ops_runs`, and `status` queries the destinations the project's sources actually resolve to.
- **Status can't fail orthogonally to data.** A run and its ledger row share one destination, so there is no failure mode where the data landed but the status system was a different, separately broken service.

The ledger is also deliberately not a reuse of dlt's tables: `_dlt_loads` is load-package-level (no source-level outcome, no cross-ref to dlt-ops state), and `_dlt_trace` exists only where trace persistence is configured. An extension-owned table keeps `status` orchestrator-neutral — the same rows appear whether the trigger was your shell, Airflow, or a backfill.

Because the ledger is written through the `DestinationAdapter` boundary, it is a full-tier feature: on a core-tier destination there is no adapter to speak SQL through, so the ledger has nowhere to live — see [destinations and tiers](destinations-and-tiers.md) for how that degrades.

## Reading it back: `pipeline status`

**`pipeline status` reads the ledger back per source, newest run first, and prints each run's outcome inline.**

```bash
dlt-ops pipeline status
```

```text
Source: demo_events
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  failed     2026-07-16 15:49:27  2026-07-16 15:49:28  -         cli        -               6f51f274e7e5
    ✗ AssertionFailedError: assertion 'min_rows_per_load' failed on demo_events.events: row count 6 is below min_rows_per_load 10
  completed  2026-07-16 15:48:47  2026-07-16 15:48:48  6         cli        -               7920f7ba0838
```

Last N runs per source, newest first (`--limit`, default 10), optionally filtered to runs scoped to one resource (`--resource`). Failed runs carry their one-line `error_summary` inline — the ledger keeps outcomes, not just successes. `--json` emits one object per source with a stable shape (`source`, `ledger` state, `error`, `runs`), which is the form to parse in scripts:

```bash
dlt-ops pipeline status --limit 1 --json
```

```json
[
  {
    "source": "demo_events",
    "ledger": "ok",
    "error": null,
    "runs": [
      {
        "pipeline_name": "demo_events_pipeline",
        "source_section": "demo_events",
        "resource_name": null,
        "destination": "duckdb",
        "dataset": "demo_data",
        "run_id": "6f51f274e7e5412dbf3c42ab58d88213",
        "dlt_run_id": null,
        "backfill_id": null,
        "trigger_source": "cli",
        "started_at": "2026-07-16T15:49:27.906870+00:00",
        "completed_at": "2026-07-16T15:49:28.176234+00:00",
        "status": "failed",
        "records_extracted": null,
        "records_loaded": null,
        "error_summary": "AssertionFailedError: assertion 'min_rows_per_load' failed on demo_events.events: row count 6 is below min_rows_per_load 10"
      }
    ]
  }
]
```

`status` runs Phase-1 discovery only (it never imports your source code) and is strictly read-only against the destination. The ledger also feeds one Tier-1 rule: `stale_sources` warns about sources that have run history and then stopped — and stays quiet when the ledger is unreachable, because `validate` never requires destination credentials.

## Three absence states

**An empty listing would be ambiguous in exactly the wrong situations, so `status` refuses to collapse three different facts into one.** Each state below is real output:

**`no runs recorded`** — the ledger table does not exist (or holds no rows for this source): the source genuinely never ran against this destination.

```text
Source: demo_events
  no runs recorded
```

**`ledger unreadable`** — `status` could not read the ledger and tells you why: the destination or dataset failed to resolve from config, or the destination itself was unreachable. This is an outage or a config problem, not an empty history:

```text
Source: demo_events
  ! ledger unreadable: No dataset configured: set [dlt_ops].default_dataset or [sources.<section>.dlt_ops].dataset in .dlt/config.toml
```

**`ledger unsupported`** — the destination runs at core tier (no `DestinationAdapter`), so no ledger can exist there. A capability fact, not a fault, and rendered dim rather than as a warning:

```text
Source: demo_events
  ! ledger unsupported: destination 'filesystem' has no DestinationAdapter (core mode)
```

In `--json` these are `"ledger": "missing"`, `"unreadable"`, and `"unsupported"` (with the reason in `"error"`); `"ok"` is the fourth value. The distinction is the point: an outage never masquerades as an empty history, and a capability gap never masquerades as an outage. `status` itself never exits non-zero over a broken ledger path — it is a diagnostic, and a diagnostic that dies on the condition it should be diagnosing would be useless.

## Best-effort by policy

**Ledger writes never decide a run's fate.** The [failure-semantics contract](failure-semantics.md) is canonical here: a run that loaded data correctly must not be failed retroactively by its own bookkeeping, so both ledger writes are best-effort — a write failure on a full-tier destination is logged loudly at ERROR (`Failed to write run-start row to _dlt_ops_runs (non-fatal, run continues)`) and swallowed. On a core-tier destination the writes skip at INFO instead, because absence of a place to write is not a failure.

The honest consequence, worth internalizing before you alert on this table: **the ledger is observability, not a transaction log.** A run whose ledger writes failed is a real run with no row; a run whose terminal write failed is a real run with a row frozen at `running`. If you need a record that is guaranteed complete, dlt's `_dlt_loads` is written by the load itself; the ledger's job is the operational view across sources, machines, and triggers — and within v0.1's scope it does exactly that, nothing stronger.

## Where next

- [Failure semantics](failure-semantics.md) — the full asymmetric contract, ledger row included
- [Destinations and tiers](destinations-and-tiers.md) — why the ledger needs full tier, and what core tier does instead
- [Backfill](backfill.md) — chunk state in `_dlt_backfills` and how it cross-references the ledger
