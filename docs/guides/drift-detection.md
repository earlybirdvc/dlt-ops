---
description: Task guide — inject additive and removal schema drift into a live DuckDB destination and catch both with pipeline reconcile, wire alert sinks, and pin down the exit codes a scheduler sees.
---

# Catch schema drift

This guide injects both kinds of schema drift into a live DuckDB destination and catches them with `pipeline reconcile`: a column added behind your model's back (additive drift), and a model column whose data quietly stops arriving (removal drift). Along the way it wires alert sinks and pins down the exit codes a scheduler will see. The [reconciler concept page](../concepts/reconciler.md) owns the theory — detection windows, thresholds, what deliberately does not count as drift; this page is the task.

**Prerequisites**

- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md).
- A checkout of the [dlt-ops repository](https://github.com/earlybirdvc/dlt-ops) — the guide runs its `examples/basic_project`, which sets the `load_timestamp_column` removal detection needs.

**Steps at a glance**

1. [Start state: a loaded destination](#1-start-state-a-loaded-destination)
2. [Baseline: no drift](#2-baseline-no-drift)
3. [Inject a column behind the model's back](#3-inject-a-column-behind-the-models-back)
4. [Emit for real: the logging sink](#4-emit-for-real-the-logging-sink)
5. [Wire a sink](#5-wire-a-sink)
6. [Catch a column going dark: `--include-removal`](#6-catch-a-column-going-dark-include-removal)
7. [Exit codes: what your scheduler sees](#7-exit-codes-what-your-scheduler-sees)

## 1. Start state: a loaded destination

**Copy `examples/basic_project` and run its incremental source once.** The project matters here for one config key: it sets `[dlt_ops] load_timestamp_column = "loaded_at"`, the time axis removal detection needs (the troubleshooting note shows what happens without it):

```bash
cp -R examples/basic_project /tmp/dlt-demo
cd /tmp/dlt-demo
dlt-ops pipeline run -s github_events_api -y
```

```text
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

## 2. Baseline: no drift

**`--dry-run` prints findings without emitting alerts** — the right mode while you are iterating by hand:

```bash
dlt-ops pipeline reconcile -s github_events_api --dry-run
```

```text
Dry-run: alert emission suppressed

Source: github_events_api  |  Findings: 0  |  Duration: 0.94s
  ✓ No drift
```

The live destination schema and the resources' Pydantic models agree. The reconciler is strictly read-only, so you can run this against production as often as you like — it never mutates pipeline state and never blocks runs.

## 3. Inject a column behind the model's back

**Commit the classic crime: `ALTER TABLE` directly in the destination** — the move an engineer makes to unblock an ingest at 2 a.m., with the model PR deferred and then forgotten:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events_api_pipeline.duckdb")
con.execute("ALTER TABLE github_events_raw.events ADD COLUMN ip_address VARCHAR")
con.execute("UPDATE github_events_raw.events SET ip_address = '203.0.113.' || (id % 40)")
con.close()
PY
dlt-ops pipeline reconcile -s github_events_api --dry-run
```

```text
Dry-run: alert emission suppressed

Source: github_events_api  |  Findings: 1  |  Duration: 0.86s
  • events: additive drift (1 column(s))
      Columns: ip_address
      First seen: 2026-07-17T08:34:31.080660+00:00
```

One finding for the drifted resource, carrying the full column set — a twenty-column upstream change is still one finding, not twenty alerts. The finding also carries the destination-reported type per column, up to five recent sample values, and a copy-pasteable `reproduce` query; the concept page's [additive-detection section](../concepts/reconciler.md#additive-detection) covers what is deliberately skipped (dlt system columns, configured injected columns, resources without a model).

## 4. Emit for real: the logging sink

**Drop `--dry-run` and the same findings also go through the configured alert sinks.** With no sink configuration at all, the default is the core `logging` sink — one structured WARNING per finding, surfacing in whatever collects your logs:

```bash
dlt-ops pipeline reconcile -s github_events_api
```

```text
2026-07-17 10:34:42|[WARNING]|dlt_ops.reconciler._emission|schema drift (additive): github_events.github_events_api.events — 1 column(s): ip_address | reproduce: SELECT "ip_address" FROM "github_events_raw"."events" WHERE "loaded_at" >= TIMESTAMP '2026-07-17T08:34:42.850569+00:00' LIMIT 5
Source: github_events_api  |  Findings: 1  |  Duration: 0.82s
  • events: additive drift (1 column(s))
      Columns: ip_address
      First seen: 2026-07-17T08:34:42.850569+00:00
```

The WARNING line is the sink emitting; the block below is the CLI's own rendering, printed in both modes. Emission is the difference between the two commands — `--dry-run` suppresses all of it, drift and error events alike.

## 5. Wire a sink

**Sinks are configured per project in `[dlt_ops]`:** `alert_sinks` lists sink plugin names (every configured sink receives every event), and each sink's non-secret options live in a table named after it — `[dlt_ops.alert_sink.<name>]`, passed to the sink's constructor as keyword arguments. The shape, with the `sentry` sink that ships as the `[sentry]` extra:

```toml
[dlt_ops]
alert_sinks = ["logging", "sentry"]

[dlt_ops.alert_sink.sentry]
environment = "prod"
```

The `sentry` extra is not installed in the environment this guide was verified in, so no Sentry emission is shown here; its DSN comes from dlt secrets, and its fingerprinting (one issue per drifted resource) is on the [concept page](../concepts/reconciler.md#findings-and-alert-sink-routing).

The `logging` sink takes no options. An explicitly empty `alert_sinks = []` disables emission on purpose; setting it back to `["logging"]` is the same as the default. Third-party sinks plug into the same `alert_sink` axis — see [write plugins](write-plugins.md).

Sink names are validated statically, so a typo dies in CI rather than silently dropping your alerts at 3 a.m. — with `alert_sinks = ["loging"]`:

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✗ 1 error(s):
  [dlt_ops.alert_sinks] alert_sinks: alert_sinks references 'loging' but no such plugin is registered under the 'dlt_ops.alert_sink' entry-point group; inspect with `dlt-ops plugins doctor`
```

`validate` exits 1 on it (the `alert_sink_registered` rule), and the Tier-2 preflight re-checks the same thing at run time. Fix the typo before continuing.

## 6. Catch a column going dark: `--include-removal`

**Additive drift is half the story.** A provider can stop sending a field with no schema change at all: the column still exists, every contract mode accepts `null`, nothing fails — the data just goes dark. Simulate it: push the existing rows (where `actor_login` has 80% non-null coverage) back into the baseline window, then append fresh loads where it is only `NULL`:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events_api_pipeline.duckdb")
con.execute("UPDATE github_events_raw.events SET loaded_at = loaded_at - INTERVAL 3 DAY")
for i in range(5):
    con.execute(
        "INSERT INTO github_events_raw.events (id, event_type, actor_login, occurred_at, loaded_at, _dlt_load_id, _dlt_id) "
        f"VALUES ({100 + i}, 'push', NULL, now(), now(), 'manual', 'manual-{i}')"
    )
con.close()
PY
dlt-ops pipeline reconcile -s github_events_api --include-removal --dry-run
```

```text
Dry-run: alert emission suppressed

Source: github_events_api  |  Findings: 1  |  Duration: 0.99s
  • events: additive drift (1 column(s))
      Columns: ip_address
      First seen: 2026-07-17T08:35:40.885579+00:00

Source: github_events_api (removal)  |  Findings: 1  |  Duration: 0.62s
  • events: removal drift (1 column(s))
      Columns: actor_login
      First seen: 2026-07-17T08:35:41.480853+00:00
```

The removal scan compares each model column's non-null coverage in the recent window against a baseline window, timestamped on `load_timestamp_column` — a column that had coverage and lost it went dark. The windows and thresholds (and why an idle resource is deliberately never flagged) are on the [concept page](../concepts/reconciler.md#removal-detection-include-removal). Without `--dry-run`, each finding emits through the sinks — here two WARNING lines, one per finding:

```bash
dlt-ops pipeline reconcile -s github_events_api --include-removal
```

```text
2026-07-17 10:36:03|[WARNING]|dlt_ops.reconciler._emission|schema drift (additive): github_events.github_events_api.events — 1 column(s): ip_address | reproduce: SELECT "ip_address" FROM "github_events_raw"."events" WHERE "loaded_at" >= TIMESTAMP '2026-07-17T08:36:03.601681+00:00' LIMIT 5
2026-07-17 10:36:04|[WARNING]|dlt_ops.reconciler._emission|schema drift (removal): github_events.github_events_api.events — 1 column(s): actor_login | reproduce: SELECT "actor_login" FROM "github_events_raw"."events" WHERE "loaded_at" >= TIMESTAMP '2026-07-17T08:36:04.343170+00:00' LIMIT 5
Source: github_events_api  |  Findings: 1  |  Duration: 1.51s
  • events: additive drift (1 column(s))
      Columns: ip_address
      First seen: 2026-07-17T08:36:03.601681+00:00

Source: github_events_api (removal)  |  Findings: 1  |  Duration: 0.74s
  • events: removal drift (1 column(s))
      Columns: actor_login
      First seen: 2026-07-17T08:36:04.343170+00:00
```

## 7. Exit codes: what your scheduler sees

**The exit code reports on the *reconciler*, not on your schema.** Findings alone exit 0 — drift is an alert to route through sinks, not a command failure for cron to retry; the run above, two findings and all, exits 0:

```bash
dlt-ops pipeline reconcile -s github_events_api --include-removal; echo "exit: $?"
```

```text
...
exit: 0
```

Only a reconciler error — unreachable destination, unknown source, a [core-tier destination](../concepts/destinations-and-tiers.md) it refuses to half-work against — exits 1:

```bash
dlt-ops pipeline reconcile -s no_such_source --dry-run; echo "exit: $?"
```

```text
Dry-run: alert emission suppressed

Source: no_such_source  |  Findings: 0  |  Duration: 0.71s
  ✗ Reconciler error: source 'no_such_source' not found in discovered sources
exit: 1
```

So the scheduling recipe is: run `reconcile --all` (every discovered source, each against its own destination) on whatever cadence your cron or orchestrator gives it, alert on exit 1 as a broken sweep, and let the sinks carry the drift findings. For post-processing findings as data instead of parsing CLI output, the same machinery is importable — `reconcile_source`, `reconcile_all`, `detect_removal` are public exports of `dlt_ops`.

## Troubleshooting: no `load_timestamp_column`

**Removal detection needs a time axis.** If `[dlt_ops] load_timestamp_column` is not set, `--include-removal` degrades honestly — and the stamped column itself stops being auto-ignored, so it surfaces as additive drift on every table. The same project with the key commented out:

```text
2026-07-17 10:39:51|[WARNING]|dlt_ops.reconciler.removal|removal detection skipped: [dlt_ops] load_timestamp_column is not set — windowed coverage needs a time axis. Set it to enable removal-drift detection.
Source: github_events_api  |  Findings: 2  |  Duration: 0.88s
  • events: additive drift (2 column(s))
      Columns: ip_address, loaded_at
      First seen: 2026-07-17T08:39:51.365245+00:00
  • actors: additive drift (1 column(s))
      Columns: loaded_at
      First seen: 2026-07-17T08:39:51.366585+00:00

Source: github_events_api (removal)  |  Findings: 0  |  Duration: 0.54s
  ✓ No drift
  ! removal detection skipped: [dlt_ops] load_timestamp_column is not set — windowed coverage needs a time axis. Set it to enable removal-drift detection.
```

Both symptoms have the same one-line fix: set `load_timestamp_column` in `[dlt_ops]` ([config reference](../configuration/reference.md)). Other columns your infrastructure stamps on every row get the same treatment via `injected_columns` — project-wide or per source — so a new stamped key is a TOML edit, never a page of false-positive drift.

## Where next

- [Reconciler](../concepts/reconciler.md) — detection mechanics, thresholds, sink isolation, and the read-only guarantee
- [Write plugins](write-plugins.md) — ship your own alert sink on the `alert_sink` axis
- [Config reference](../configuration/reference.md) — `alert_sinks`, `load_timestamp_column`, `injected_columns`
