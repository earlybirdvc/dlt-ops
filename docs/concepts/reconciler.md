---
description: How dlt-ops pipeline reconcile diffs the live destination schema against your Pydantic models in both directions — additive drift (new columns behind your model's back) and removal drift (columns that went dark) — and routes findings to pluggable alert sinks, read-only.
---

# Schema-drift reconciler

`pipeline reconcile` diffs the live destination schema against the Pydantic models your resources declare via `columns=`, and routes findings to pluggable alert sinks. It is strictly read-only — it never mutates pipeline state and never blocks runs — and it exists because schema drift is silent at ingest in both directions: columns can appear in the destination behind your model's back, and columns your model declares can quietly stop carrying data. Read this for both detection mechanisms, what deliberately does not count as drift, and how findings reach you.

**At a glance**

| What it is | When it applies | Requires | On failure | Canonical detail |
|---|---|---|---|---|
| `pipeline reconcile` — a read-only diff of the live destination schema against your Pydantic `columns=` models, routed to alert sinks | On demand or your own cron; against every discovered source regardless of contract mode | Full tier (a `DestinationAdapter`); removal detection also needs `load_timestamp_column` | Findings exit 0 (an alert to route, not a failure); only a reconciler error exits 1; sinks are best-effort | [Failure semantics](failure-semantics.md) for where read-only diagnostics sit |

Schema contracts and the reconciler split the work: the canonical freeze contract rejects unknown columns *at ingest*, while the reconciler catches what no ingest-time contract can see — after-the-fact schema surgery and data that goes dark inside columns that still exist. That is why it runs against every discovered source regardless of contract mode:

- **Evolve sources** — the primary drift signal: an evolving contract admits the new column at ingest by design, so the reconciler is the only thing that tells you upstream shipped a field your model doesn't know.
- **Freeze sources** — the patched-schema failure mode: an engineer hand-patches the destination schema to unblock ingest and forgets (or defers) the model PR. Freeze is satisfied at ingest, but the live table now has more columns than the model — the reconciler fires.

## Additive detection

**Additive detection is the default scan: for each resource it fetches the live column list from the destination, computes the column set the resource's Pydantic model produces *at the destination*, and reports live columns the model doesn't know.** The examples below run in a copy of the repository's `examples/basic_project` (the same fuller demo the [checkpoints](checkpoints.md) page uses) after one `run` of its `github_events_api` source; a fresh project reconciles clean:

```bash
dlt-ops pipeline reconcile -s github_events_api --dry-run
```

```text
Dry-run: alert emission suppressed

Source: github_events_api  |  Findings: 0  |  Duration: 0.90s
  ✓ No drift
```

Now commit the classic crime — add a column directly in the destination, behind the model's back:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events_api_pipeline.duckdb")
con.execute("ALTER TABLE github_events_raw.events ADD COLUMN payload_size BIGINT")
con.execute("UPDATE github_events_raw.events SET payload_size = 512 + id * 7")
con.close()
PY
dlt-ops pipeline reconcile -s github_events_api
```

```text
2026-07-16 18:25:09|[WARNING]|dlt_ops.reconciler._emission|schema drift (additive): github_events.github_events_api.events — 1 column(s): payload_size | reproduce: SELECT "payload_size" FROM "github_events_raw"."events" WHERE "loaded_at" >= TIMESTAMP '2026-07-16T16:25:09.758696+00:00' LIMIT 5
Source: github_events_api  |  Findings: 1  |  Duration: 0.88s
  • events: additive drift (1 column(s))
      Columns: payload_size
      First seen: 2026-07-16T16:25:09.758696+00:00
```

The first line is the default `logging` alert sink emitting the finding; the block under it is the CLI's own rendering, printed with or without emission.

Detection emits **one finding per drifted resource** — carrying the full drifted column set, the destination-reported type per column, up to five recent sample values per column (one windowed query per resource; a failed sample query never blocks the alert), and a copy-pasteable `reproduce` SELECT — rather than one alert per column, so a single upstream change cannot fan out into twenty pages.

Three column families are deliberately not drift:

- **dlt's own system columns** (anything prefixed `_dlt_`) are skipped by prefix.
- **Columns your infrastructure stamps on every row** are subtracted via config — project-wide `[dlt_ops] injected_columns`, per-source `[sources.<X>.dlt_ops] injected_columns`, and the configured `load_timestamp_column`, which is auto-ignored because the runner stamps it (it is never part of a Pydantic model by design). Adding a new stamped key is a one-line TOML edit, not a reconciler change.
- **A resource with no Pydantic `columns=` model** has nothing to diff against, so it is skipped — as is a resource whose table doesn't exist at the destination yet.

One subtlety does the correctness heavy lifting: both sides of the diff speak **destination-side names**. The model's attribute names and aliases are run through the same dlt naming convention the write path uses — the source's own schema convention, not a hardcoded default — so a Pydantic field `startTime` matches the persisted `start_time` column instead of surfacing as false-positive drift.

## Removal detection: `--include-removal`

**Additive drift is half the story.** A provider can stop sending a field without any schema change: the column still exists, ingestion accepts `null` under every contract mode, and nothing fails — the data just goes dark.

`--include-removal` adds a windowed non-null-coverage scan over the model's columns: for each column, one query computes the fraction of non-null values in the **recent** window (last 6 hours of loads) and in the **baseline** window (the 7-day window ending where the recent one begins), timestamped on the configured `[dlt_ops] load_timestamp_column`. A column whose baseline coverage exceeded 20% and whose recent coverage fell below 1% went dark.

Simulating exactly that — backdating the existing rows into the baseline window, then appending fresh loads where `actor_login` (a nullable model column with 80% historical coverage) is only `NULL`:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events_api_pipeline.duckdb")
con.execute("UPDATE github_events_raw.events SET loaded_at = loaded_at - INTERVAL 3 DAY")
for i in range(5):
    con.execute(
        "INSERT INTO github_events_raw.events "
        "(id, event_type, actor_login, occurred_at, loaded_at, _dlt_load_id, _dlt_id) "
        f"VALUES ({100 + i}, 'push', NULL, now(), now(), 'manual', 'manual-{i}')"
    )
con.close()
PY
dlt-ops pipeline reconcile -s github_events_api --include-removal --dry-run
```

```text
Dry-run: alert emission suppressed

Source: github_events_api  |  Findings: 1  |  Duration: 0.82s
  • events: additive drift (1 column(s))
      Columns: payload_size
      First seen: 2026-07-16T16:25:24.842742+00:00

Source: github_events_api (removal)  |  Findings: 1  |  Duration: 0.66s
  • events: removal drift (1 column(s))
      Columns: actor_login
      First seen: 2026-07-16T16:25:25.474803+00:00
```

The thresholds are deliberately conservative in both directions. A window with no rows at all yields no coverage ratio, and the detector refuses to flag on it — an idle resource is indistinguishable from a dropped field, and guessing would alert on every quiet weekend.

Removal findings carry empty samples (there are no recent non-null values to sample, by definition) and empty inferred types; the `reproduce` SELECT still ships. The windows and thresholds are keyword parameters of the Python API (`detect_removal`, defaults exported as `DEFAULT_*`); the CLI runs the canonical defaults.

Removal detection needs a time axis, so it requires `load_timestamp_column` — the example project sets it (`loaded_at`); the scaffolded demo project does not, and asking anyway degrades honestly instead of guessing:

```text
Source: demo_events (removal)  |  Findings: 0  |  Duration: 0.26s
  ✓ No drift
  ! removal detection skipped: [dlt_ops] load_timestamp_column is not set — windowed coverage needs a time axis. Set it to enable removal-drift detection.
```

## Findings and alert-sink routing

**Every finding and every reconciler-internal error goes through the `AlertSink` protocol — the contract of the `alert_sink` plugin axis (`emit_drift`, `emit_error`, `flush`), so no alerting SDK ever loads unless you configured its sink.** Sinks are selected per project, every configured sink receives every event, and per-sink non-secret options live in a table named after the sink:

```toml
[dlt_ops]
alert_sinks = ["logging", "sentry"]

[dlt_ops.alert_sink.sentry]
environment = "prod"
```

The default is `["logging"]` — the core sink that needs zero configuration and writes one structured log line per event (shown in the additive demo above), so a zero-config project still surfaces drift in whatever collects its logs.

The `sentry` sink ships with the `[sentry]` extra and reads its DSN from dlt secrets (`[alert_sinks.sentry] dsn` in `.dlt/secrets.toml`; the bare `SENTRY_DSN` environment variable is deliberately not read — one source of truth). It fingerprints findings as `["schema-drift", pipeline, source, resource]`, so additive and removal drift on the same resource collapse into **one** Sentry issue by design — one model PR closes both — with a `drift_type` tag to disambiguate, and reconciler-internal errors land under a separate fingerprint so a drift triage never wades through reconciler bugs.

Third-party sinks register on the same axis — see [plugins](plugins.md) and the [plugin-writing guide](../guides/write-plugins.md).

Sink plumbing follows the observability side of the [failure-semantics contract](failure-semantics.md):

- **One failure never starves the rest** — one sink raising never crashes the sweep or the other sinks.
- **A sink that fails to load is dropped for the invocation** — `validate`'s `alert_sink_registered` rule and the Tier-2 preflight are the enforcement points for misconfiguration.
- **Emission is never silently lost** — if every configured sink drops, emission falls back to the core logging sink; an explicitly empty `alert_sinks = []` disables emission on purpose.
- **Sinks flush on exit** — every public entry point flushes on the way out, bounding background transports (Sentry's queue) before a short-lived CLI or orchestrator task exits.
- **`--dry-run` outranks everything** — it suppresses all emission (drift and error events alike) while still printing and returning the findings.

## Read-only, and what failure means

**The reconciler only ever reads: live column metadata and windowed SELECTs, both through the [`DestinationAdapter` boundary](destinations-and-tiers.md) in canonical SQL** — which also makes `reconcile` a full-tier verb that refuses on a core-tier destination rather than half-working (the refusal is shown in [failure semantics](failure-semantics.md)).

Because it can only observe, its exit code reports on the *reconciler*, not on your schema: findings alone exit 0 — drift is an alert to route, not a command failure to retry — and only a reconciler error (unreachable destination, unknown source) exits 1.

Failures are isolated at every level so one broken resource cannot hide drift elsewhere:

- a per-resource failure is reported through the sink's error path and the sweep continues;
- a source-level failure lands in that source's `error` field;
- `reconcile --all` gives every discovered source its own result block, each resolving — and reconciling against — its own destination and dataset from the config chain, so multi-destination projects sweep without cross-destination credentials.

A `reconcile --all` sweep, one source drifted and one clean:

```bash
dlt-ops pipeline reconcile --all --dry-run
```

```text
Source: github_events_api  |  Findings: 1  |  Duration: 0.21s
  • events: additive drift (1 column(s))
      Columns: payload_size
      First seen: 2026-07-16T16:25:42.081234+00:00

Source: github_events_full  |  Findings: 0  |  Duration: 0.02s
  ✓ No drift
```

The same machinery is importable for orchestrated sweeps — `reconcile_source`, `reconcile_all`, `detect_removal`, plus the `DriftFinding` / `ReconcileResult` models and the `AlertSink` protocol, all public exports of `dlt_ops` — returning findings as data so a scheduled task can post-process instead of parsing CLI output. There is no built-in scheduler for reconciliation, deliberately: it is one read-only verb, and it runs on whatever cadence your cron or orchestrator gives it.

## Where next

- [Drift-detection guide](../guides/drift-detection.md) — inject drift, wire a sink, and tune removal detection, step by step
- [Destinations and tiers](destinations-and-tiers.md) — why `reconcile` needs a `DestinationAdapter`
- [Failure semantics](failure-semantics.md) — where read-only diagnostics sit in the run contract
