---
description: Task guide — put a data-quality gate on a resource and fire it under all three policies (fail, warn, quarantine), read quarantined rows back from _dlt_rejected, then see the full-tier gate refuse quarantine on a core-tier destination.
---

# Gate a resource with assertions

This guide puts a data-quality gate on a resource and makes it fire under all three policies — `fail`, `warn`, and `quarantine` — then reads the quarantined rows back out of the destination. It is the hands-on companion to the [assertions concept page](../concepts/assertions.md), which covers the declaration syntax, the four built-in types, and the execution model.

**Prerequisites**

- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md).
- `fail` and `warn` work at any tier; `quarantine` needs a full-tier destination (step 7 shows the refusal without one).

**Steps at a glance**

1. [Start from the scaffolded demo](#1-start-from-the-scaffolded-demo)
2. [Declare the gates](#2-declare-the-gates)
3. [Trip the gate: `fail`](#3-trip-the-gate-fail)
4. [Downgrade to `warn`](#4-downgrade-to-warn)
5. [Quarantine the bad rows](#5-quarantine-the-bad-rows)
6. [Read the quarantine table back](#6-read-the-quarantine-table-back)
7. [Prove the tier gate](#7-prove-the-tier-gate)

## 1. Start from the scaffolded demo

**Scaffold the example and run it once for a clean baseline:**

```bash
dlt-ops init demo --example
cd demo
dlt-ops pipeline run -s demo_events -y
```

The example source loads six fixture rows (`id`, `kind`, `occurred_at` — typed by the `Event` Pydantic model in `my_pipeline/resource/events.py`) into `demo_data.events` in a local DuckDB file. That baseline is the thing the gates below protect.

## 2. Declare the gates

**Assertions live in the same `.dlt/config.toml` as everything else, one table per resource.** Append:

```toml
[sources.demo_events.dlt_ops.assertions.events]
min_rows_per_load = 1
required_columns = ["id", "kind"]
```

The two keys declared here differ in scope:

| Assertion key | Scope | What it checks |
|---|---|---|
| `min_rows_per_load` | batch | Guards against a silently empty load — an upstream outage that yields zero rows still "succeeds" in vanilla dlt. |
| `required_columns` | row | Fails any row missing one of the named keys — key presence, not non-nullness; the Pydantic model already owns type and nullability. |

`validate` checks the block statically — registered type names, column references against the model, `on_failure` values — before anything runs:

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✓ All sources validated successfully
```

Run again and the gate announces itself; six rows pass and load exactly as before:

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
2026-07-16 21:57:08|[INFO]|dlt_ops.assertions.engine|Assertion gate attached to resource 'events' (2 assertion(s), declaration order)
...
1 load package(s) were loaded to destination duckdb and into dataset demo_data
```

## 3. Trip the gate: `fail`

**Every assertion's default policy is `fail`.** Demand more rows than the fixture emits — set `min_rows_per_load = 10` in the block above — and run again:

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
2026-07-16 21:57:23|[INFO]|dlt_ops.assertions.engine|Assertion gate attached to resource 'events' (2 assertion(s), declaration order)
2026-07-16 21:57:23|[INFO]|dlt_ops.discovery.runner|Dropped pending load package(s) after assertion failure
dlt_ops.assertions.models.AssertionFailedError: assertion 'min_rows_per_load' failed on demo_events.events: row count 6 is below min_rows_per_load 10
```

The run exits 1 between extract and load: nothing reached the destination, and the `Dropped pending load package(s)` line means the extracted batch was deleted rather than left where the next run would silently auto-load it. The failure is a recorded outcome, not just a stack trace — the [runs ledger](../concepts/runs-ledger.md) keeps it with a one-line summary:

```bash
dlt-ops pipeline status --limit 3
```

```text
Source: demo_events
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  failed     2026-07-16 19:57:23  2026-07-16 19:57:23  -         cli        -               268701e72779
    ✗ AssertionFailedError: assertion 'min_rows_per_load' failed on demo_events.events: row count 6 is below min_rows_per_load 10
  completed  2026-07-16 19:57:08  2026-07-16 19:57:09  6         cli        -               756c3e3daa7b
  completed  2026-07-16 19:56:45  2026-07-16 19:56:46  6         cli        -               c98b9c58d6c4
```

## 4. Downgrade to `warn`

**`on_failure` is set per assertion with the table form** — the same key, an inline table instead of a bare value:

```toml
min_rows_per_load = { value = 10, on_failure = "warn" }
```

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
2026-07-16 21:57:56|[WARNING]|dlt_ops.assertions.engine|assertion 'min_rows_per_load' warn on demo_events.events: row count 6 is below min_rows_per_load 10
2026-07-16 21:57:56|[WARNING]|dlt_ops.assertions.engine|assertion warn summary for demo_events.events: min_rows_per_load=1
...
1 load package(s) were loaded to destination duckdb and into dataset demo_data
```

The run exits 0 and the six rows load anyway — `warn` observes and counts, nothing more. Warn counts live in logs only in v0.1; they are not written to the destination, so a warn-only gate is advisory by definition. Use it for checks you are still calibrating, then promote to `fail` or `quarantine`.

## 5. Quarantine the bad rows

**`quarantine` removes exactly the failing rows from the load and preserves them in a `_dlt_rejected` table** — the middle ground for row-scoped checks where one bad record should not block the other ten million. Restore `min_rows_per_load = 1` and add a uniqueness gate:

```toml
[sources.demo_events.dlt_ops.assertions.events]
min_rows_per_load = 1
required_columns = ["id", "kind"]
unique_columns = { value = ["id"], on_failure = "quarantine" }
```

The fixture rows all carry unique ids, so give the gate something to catch — append a duplicate to `_ROWS` in `my_pipeline/resource/events.py`:

```python
    {"id": 6, "kind": "purchase", "occurred_at": datetime(2026, 1, 4, 16, 21, tzinfo=UTC)},
```

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
2026-07-16 21:58:24|[INFO]|dlt_ops.assertions.engine|Assertion gate attached to resource 'events' (3 assertion(s), declaration order)
2026-07-16 21:58:24|[INFO]|dlt_ops.discovery.runner|Quarantined 1 row(s) to _dlt_rejected
1 load package(s) were loaded to destination duckdb and into dataset demo_data
```

The run completes: the first `id = 6` row passes (uniqueness is asserted within the load batch — the first occurrence is fine, repeats are the violations), the second is diverted, and the surviving six rows load.

## 6. Read the quarantine table back

**`_dlt_rejected` lives in the same dataset the data lands in, one row per rejected record with the full row payload as JSON:**

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("demo_events_pipeline.duckdb", read_only=True)
print(con.sql("SELECT count(*) AS loaded FROM demo_data.events"))
print(con.sql("SELECT assertion_type, violation, run_id, row_json FROM demo_data._dlt_rejected"))
PY
```

```text
┌────────┐
│ loaded │
├────────┤
│      6 │
└────────┘

┌────────────────┬────────────────────┬──────────────────────────────────┬─────────────────────────────────────────────┐
│ assertion_type │     violation      │              run_id              │                  row_json                   │
├────────────────┼────────────────────┼──────────────────────────────────┼─────────────────────────────────────────────┤
│ unique_columns │ duplicate key id=6 │ b6b3aff741874365b2f13a981a0ff2b7 │ {"id": 6, "kind": "purchase", "occurred_at" │
│                │                    │                                  │ : "2026-01-04 16:21:00+00:00"}              │
└────────────────┴────────────────────┴──────────────────────────────────┴─────────────────────────────────────────────┘
```

`run_id` joins the quarantine rows to their run in the ledger, so "which run rejected what" is one query:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("demo_events_pipeline.duckdb", read_only=True)
print(con.sql("SELECT r.status, r.records_loaded, q.assertion_type, q.violation FROM demo_data._dlt_ops_runs r JOIN demo_data._dlt_rejected q ON r.run_id = q.run_id"))
PY
```

```text
┌───────────┬────────────────┬────────────────┬────────────────────┐
│  status   │ records_loaded │ assertion_type │     violation      │
├───────────┼────────────────┼────────────────┼────────────────────┤
│ completed │              6 │ unique_columns │ duplicate key id=6 │
└───────────┴────────────────┴────────────────┴────────────────────┘
```

There is no automatic retention: quarantined rows sit in `_dlt_rejected` until you `DELETE` them. Replaying repaired rows is also on you — the table records rejections, it is not a dead-letter queue with redelivery.

## 7. Prove the tier gate

**Quarantine writes SQL to the destination, so it requires a registered `DestinationAdapter` — [full tier](../concepts/destinations-and-tiers.md).** DuckDB ships one; watch what happens on a destination that does not. Point the source at a local filesystem bucket by adding one line under `[sources.demo_events.dlt_ops]`:

```toml
destination = "filesystem"
```

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
Pipeline Configuration
----------------------------------------
  Source: demo_events
  ...
  Destination: filesystem
  Capabilities: core (no adapter: runs ledger and status, checkpoints, backfill, clean (remote), reconcile, assertion quarantine unavailable)

Starting pipeline...
dlt_ops.preflight.DestinationCapabilityError: destination 'filesystem' has no registered DestinationAdapter, but this run engages adapter-gated feature(s): assertion quarantine (on_failure = "quarantine" on resource(s): events). Features gated on an adapter: runs ledger and status, checkpoints, backfill, clean (remote), reconcile, assertion quarantine. Registered adapters: 'bigquery', 'duckdb', 'postgres'. Install a DestinationAdapter under the 'dlt_ops.destination' entry-point group, switch to a destination that has one, or remove the feature from the run; see docs/reference/destinations.md.
```

The refusal happens at Tier-2 preflight, before extract: a gate your config demands must not silently downgrade to "load the bad rows anyway". `fail` and `warn` touch no destination SQL and work at every tier — drop the `quarantine` policy (or the `destination` override) and the run proceeds. Remove the override before continuing.

## Troubleshooting: quarantine on a batch-scoped assertion

**`quarantine` only makes sense where there are specific rows to divert.** Putting it on a batch-scoped type — `min_rows_per_load = { value = 1, on_failure = "quarantine" }` — is invalid config, caught statically:

```text
✗ 1 error(s):
  [demo_events] assertions.events.min_rows_per_load: assertion type 'min_rows_per_load' is batch-scoped; on_failure = "quarantine" is invalid — there are no specific rows to quarantine when a batch verdict fails
```

When a batch verdict like "too few rows" fails, no individual row is at fault, so there is nothing to quarantine — pick `fail` or `warn` for batch-scoped types. The same check re-runs at Tier-2 preflight, so a scheduler that never invokes `validate` still refuses the config instead of guessing.

## Where next

- [Assertions](../concepts/assertions.md) — the four built-in types, custom predicates, and how gates execute inside a run
- [Failure semantics](../concepts/failure-semantics.md) — the canonical contract for what each policy does to a run
- [Configuration reference](../configuration/reference.md) — the full assertion declaration syntax
