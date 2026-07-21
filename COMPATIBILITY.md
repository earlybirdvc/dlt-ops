# Compatibility

## dlt version policy

The dlt dependency is a floor, never a cap: **`dlt>=1.27`**. You own your project's dlt version — this package's metadata will not force a resolver downgrade or block a dlt upgrade.

The floor is the oldest minor in the verified matrix below. [`ci/dlt-versions.txt`](ci/dlt-versions.txt) is the single source of truth for **which minors CI tests**: the test matrix, the `pyproject.toml` floor, and the matrix below all derive from it, and `tests/test_packaging.py` fails if any of them drift.

**What "verified" gates at runtime: nothing.** The matrix is a statement about what CI exercised, never a runtime ceiling — every feature runs on any dlt at or above the floor, including a minor released after this table was last updated. No package module may gate a feature on a list of dlt minors, and a test enforces that: a fresh install must not lose a feature the day dlt ships a minor this repo has not seen.

That holds because nothing in the package needs to know a dlt version. The destination-side work in remote `clean` is dlt's own `pipeline_drop`, and the rows dlt-ops deletes afterwards from the shared bookkeeping tables (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`) are addressed by names read off the live schema rather than hardcoded — dlt normalizes its own table and column identifiers through the schema's naming convention before writing them, so the names follow the destination's convention, which is a per-destination choice rather than a dlt release. The one file cleanup still edits by hand, the local `state.json`, checks the state engine version dlt stamped into it and refuses an unfamiliar layout, which keys on what dlt records rather than on a list of releases.

## Verified matrix

One row per supported dlt minor (mirrors `ci/dlt-versions.txt`); cells say how each dlt-minor × destination combination was verified.

| dlt minor | DuckDB | Postgres | BigQuery | Databricks |
|---|---|---|---|---|
| 1.27 | ci-required | spike | spike | unverified (pending) |
| 1.28 | ci-required | bracketed | bracketed | unverified (pending) |
| 1.29 | ci-required | spike + ci-integration | spike + ci-integration | unverified (pending) |

### Verified-by legend

- **ci-required** — full test suite in the required CI lane (`test` matrix in [`.github/workflows/ci.yml`](.github/workflows/ci.yml)): Python 3.11/3.12/3.13 × this dlt minor, zero cloud credentials.
- **ci-integration** — credentialed, non-blocking CI lane (`integration` / `integration-bigquery`); runs the integration suite on the locked dlt version (currently a 1.29 patch).
- **spike** — a one-off state-schema portability diff, run 2026-07-14 on dlt 1.27.2 / 1.29.0: column-name sets, normalized logical types, and row semantics of dlt's three state tables were verified identical across those minors, and cleanup's canonical SQL was transpiled with sqlglot and executed against live Postgres (rolled back) / validated against live BigQuery (dry-run). A historical record of how these cells were established.
- **bracketed** — not probed directly. The spike found the 1.27 and 1.29 dumps byte-identical per destination, bracketing 1.28; the full test suite still runs on 1.28 in the required DuckDB lane.
- **unverified (pending)** — the Databricks cells await credentials to run the suite against a live Databricks; until they arrive, the matrix cannot vouch for them. That is a verification gap, not an adapter statement: as with Snowflake, no `DestinationAdapter` is registered for Databricks, so the `[databricks]` extra installs dlt's own destination support only and runs at core tier.

## Python

3.11 / 3.12 / 3.13 (`requires-python = ">=3.11"`; all three in the required CI matrix).

## Extras without a first-party adapter

`[snowflake]` and `[databricks]` install `dlt`'s destination support only — no `dlt_ops.destination` entry point is registered for them, so they run at **core tier** (see the [destinations reference](docs/reference/destinations.md)). `run` and scheduling work on any destination dlt can resolve; only the adapter-routed features are gated on a registered `DestinationAdapter`. On a core-tier destination they degrade loudly rather than silently:

- **Refuse before doing anything** (a typed error): assertion `quarantine`,
  `@with_checkpoints`, and `backfill` fail the Tier-2 preflight and the
  `destination_capability` validate rule; with
  `[dlt_ops] require_destination_adapter = true`, every `run`/`backfill`
  fails preflight. Remote `clean` and `reconcile` refuse with a
  capability-specific message; `clean --local-only` is unaffected.
- **Degrade to a loud no-op**: the runs ledger skips with one INFO line per
  write, and `status` reports the source's ledger as `ledger unsupported`.

Installing a first-party adapter (`[duckdb]`, `[postgres]`, `[bigquery]`), registering your own, or opting into a capability-derived one switches any of these on — the tier is per destination, not per install. A derived adapter is not the same claim as a tested one; see [derived is not the same as tested](docs/reference/destinations.md#derived-is-not-the-same-as-tested) before relying on one.

The object-store extras — `[filesystem]`, `[s3]`, `[gs]`, `[az]` — are the same story from the other direction: they install dlt's filesystem destination support for remote buckets (a local `file://` bucket needs no extra), and an object store has no durable SQL engine of its own for the gated features to write to — those destinations stay core tier by construction, not just "until an adapter ships".

## Extending the verified matrix

When a new dlt minor is released:

1. Wait for the non-blocking `test-dlt-latest` CI lane to run the full suite against it and go green.
2. Extend `ci/dlt-versions.txt` and the matrix above together, in one change; raise the `pyproject.toml` floor only when an old minor is dropped. `tests/test_packaging.py` enforces the sync.

Adding a minor widens what CI covers. It unlocks nothing at runtime, because nothing is gated on it — a dlt release this table has never seen already runs every feature.
