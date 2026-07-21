# Compatibility

## dlt version policy

The dlt dependency is a floor, never a cap: **`dlt>=1.27`**. You own your project's dlt version — this package's metadata will not force a resolver downgrade or block a dlt upgrade.

The floor is the oldest minor in the verified matrix below. Single source of truth: [`ci/dlt-versions.txt`](ci/dlt-versions.txt) — the CI test matrix, the cleanup guard (`dlt_ops/_compat.py`), the `pyproject.toml` floor, and the matrix below all derive from it; `tests/test_packaging.py` and `tests/test_cleanup.py` fail if any of them drift.

What "verified" gates at runtime: almost nothing. Every feature except one runs on any dlt at or above the floor. The exception is remote `clean` — it rewrites dlt-internal state tables (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`) whose layout is reverse-engineered per minor, so on a dlt minor outside the verified set it refuses with a `CleanupUnsupportedError` instead of guessing against an unknown layout (dlt's own `pipeline.drop()` remains the escape hatch). A newer, not-yet-verified dlt minor costs you exactly that one feature until the matrix catches up.

## Verified matrix

One row per supported dlt minor (mirrors `ci/dlt-versions.txt`); cells say how
each dlt-minor × destination combination was verified.

| dlt minor | DuckDB | Postgres | BigQuery | Databricks |
|---|---|---|---|---|
| 1.27 | ci-required | spike | spike | unverified (pending) |
| 1.28 | ci-required | bracketed | bracketed | unverified (pending) |
| 1.29 | ci-required | spike + ci-integration | spike + ci-integration | unverified (pending) |

### Verified-by legend

- **ci-required** — full test suite in the required CI lane
  (`test` matrix in [`.github/workflows/ci.yml`](.github/workflows/ci.yml)):
  Python 3.11/3.12/3.13 × this dlt minor, zero cloud credentials.
- **ci-integration** — credentialed, non-blocking CI lane
  (`integration` / `integration-bigquery`); runs the integration
  suite on the locked dlt version (currently a 1.29 patch).
- **spike** — state-schema portability diff
  ([`ci/dump_state_schema.py`](ci/dump_state_schema.py), run 2026-07-14 on
  dlt 1.27.2 / 1.29.0): column-name sets, normalized logical types, and row
  semantics of the three state tables verified identical; cleanup's canonical
  SQL transpiled with sqlglot and executed against live Postgres (rolled
  back) / validated against live BigQuery (dry-run).
- **bracketed** — not probed directly. The spike found the 1.27 and 1.29
  dumps byte-identical per destination, bracketing 1.28; the full test suite
  still runs on 1.28 in the required DuckDB lane.
- **unverified (pending)** — the Databricks cells await credentials to run the
  suite against a live Databricks; until they arrive, the matrix cannot vouch
  for them. That is a verification gap, not an adapter statement: as with Snowflake,
  no `DestinationAdapter` is registered for Databricks, so the `[databricks]`
  extra installs dlt's own destination support only and runs at core tier.

## Python

3.11 / 3.12 / 3.13 (`requires-python = ">=3.11"`; all three in the required
CI matrix).

## Extras without a first-party adapter

`[snowflake]` and `[databricks]` install `dlt`'s destination support only — no
`dlt_ops.destination` entry point is registered for them, so they run at
**core tier** (see the
[destinations reference](docs/reference/destinations.md)). `run` and scheduling
work on any destination dlt can resolve; only the adapter-routed features are
gated on a registered `DestinationAdapter`. On a core-tier destination they
degrade loudly rather than silently:

- **Refuse before doing anything** (a typed error): assertion `quarantine`,
  `@with_checkpoints`, and `backfill` fail the Tier-2 preflight and the
  `destination_capability` validate rule; with
  `[dlt_ops] require_destination_adapter = true`, every `run`/`backfill`
  fails preflight. Remote `clean` and `reconcile` refuse with a
  capability-specific message; `clean --local-only` is unaffected.
- **Degrade to a loud no-op**: the runs ledger skips with one INFO line per
  write, and `status` reports the source's ledger as `ledger unsupported`.

Installing a first-party adapter (`[duckdb]`, `[postgres]`, `[bigquery]`) or
registering your own switches any of these on — the tier is per destination,
not per install.

The object-store extras — `[filesystem]`, `[s3]`, `[gs]`, `[az]` — are the same
story from the other direction: they install dlt's filesystem destination
support for remote buckets (a local `file://` bucket needs no extra), and
because an object store has no SQL engine, no adapter can back the gated
features — those destinations stay core tier by construction, not just "until an
adapter ships".

## Extending the verified matrix

When a new dlt minor is released:

1. Re-run [`ci/dump_state_schema.py`](ci/dump_state_schema.py) per destination on the new minor and diff against the committed dumps (`ci/state-schema-dumps/`).
2. On an empty diff, extend — together, in one change — `ci/dlt-versions.txt`, `SUPPORTED_DLT_MINORS` in `dlt_ops/_compat.py`, and the matrix above; raise the `pyproject.toml` floor only when an old minor is dropped. The test suite enforces the sync, and the non-blocking `test-dlt-latest` CI lane gives early warning when a brand-new dlt minor breaks the suite.
