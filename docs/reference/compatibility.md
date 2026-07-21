---
description: The verified dlt-minor × destination matrix and dlt version policy — dlt-ops sets a floor (dlt>=1.27), never a cap. What "verified" gates at runtime (only remote clean), the per-cell verification legend, and how to extend the matrix.
---

# Compatibility

This page records the verified dlt-minor × destination matrix and the dlt version policy: `dlt-ops` sets a dlt floor and never a cap, so you own your project's dlt version. Canonical source: `COMPATIBILITY.md` in the repo root — the test suite parses it, and the matrix, the `pyproject.toml` floor, and the cleanup guard all derive from one file.

## dlt version policy

**The dlt dependency is a floor, never a cap: `dlt>=1.27`.** You own your project's dlt version — this package's metadata will not force a resolver downgrade or block a dlt upgrade.

The floor is the oldest minor in the verified matrix below. Single source of truth: [`ci/dlt-versions.txt`](https://github.com/earlybirdvc/dlt-ops/blob/main/ci/dlt-versions.txt) — the CI test matrix, the cleanup guard (`dlt_ops/_compat.py`), the `pyproject.toml` floor, and the matrix below all derive from it; `tests/test_packaging.py` and `tests/test_cleanup.py` fail if any of them drift.

What "verified" gates at runtime is almost nothing: every feature except one runs on any dlt at or above the floor. The exception is remote `clean` — it rewrites dlt-internal state tables (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`) whose layout is reverse-engineered per minor, so on a dlt minor outside the verified set it refuses with a `CleanupUnsupportedError` instead of guessing against an unknown layout (dlt's own `pipeline.drop()` remains the escape hatch). A newer, not-yet-verified dlt minor costs you exactly that one feature until the matrix catches up.

## Verified matrix

**One row per supported dlt minor (mirrors `ci/dlt-versions.txt`); cells say how each dlt-minor × destination combination was verified.**

| dlt minor | DuckDB | Postgres | BigQuery | Databricks |
|---|---|---|---|---|
| 1.27 | ci-required | spike | spike | unverified (pending) |
| 1.28 | ci-required | bracketed | bracketed | unverified (pending) |
| 1.29 | ci-required | spike + ci-integration | spike + ci-integration | unverified (pending) |

### Verified-by legend

**Each matrix cell is one of five verification states:**

- **ci-required** — full test suite in the required CI lane (`test` matrix in [`.github/workflows/ci.yml`](https://github.com/earlybirdvc/dlt-ops/blob/main/.github/workflows/ci.yml)): Python 3.11/3.12/3.13 × this dlt minor, zero cloud credentials.
- **ci-integration** — credentialed, non-blocking CI lane (`integration` / `integration-bigquery`); runs the integration suite on the locked dlt version (currently a 1.29 patch).
- **spike** — state-schema portability diff ([`ci/dump_state_schema.py`](https://github.com/earlybirdvc/dlt-ops/blob/main/ci/dump_state_schema.py), run 2026-07-14 on dlt 1.27.2 / 1.29.0): column-name sets, normalized logical types, and row semantics of the three state tables verified identical; cleanup's canonical SQL transpiled with sqlglot and executed against live Postgres (rolled back) / validated against live BigQuery (dry-run).
- **bracketed** — not probed directly. The spike found the 1.27 and 1.29 dumps byte-identical per destination, bracketing 1.28; the full test suite still runs on 1.28 in the required DuckDB lane.
- **unverified (pending)** — the Databricks cells await credentials to run the suite against a live Databricks; until they arrive, the matrix cannot vouch for them. That is a verification gap, not an adapter statement: as with Snowflake, no `DestinationAdapter` is registered for Databricks, so the `[databricks]` extra installs dlt's own destination support only and runs at core tier.

## Python

**3.11 / 3.12 / 3.13** (`requires-python = ">=3.11"`; all three in the required CI matrix).

## Extras without a first-party adapter

**`[snowflake]` and `[databricks]` install dlt's destination support only — no `dlt_ops.destination` entry point is registered for them, so they run at core tier.** `run` and scheduling work on any destination dlt can resolve; the adapter-routed features degrade loudly — the gates the config demands (assertion `quarantine`, `@with_checkpoints`, `backfill`, `require_destination_adapter = true`) refuse at preflight, while observability (the runs ledger, `status`) skips with a logged no-op. The [destinations reference](destinations.md#core-tier-verb-by-verb) has the behavior and exact messages verb by verb.

Installing a first-party adapter (`[duckdb]`, `[postgres]`, `[bigquery]`) or registering your own switches these on — the tier is per destination, not per install.

The object-store extras — `[filesystem]`, `[s3]`, `[gs]`, `[az]` — are core tier by construction: they add dlt's filesystem destination support for remote buckets (a local `file://` bucket needs no extra), and an object store has no SQL engine to back the gated features. See [object-store destinations](destinations.md#object-store-destinations).

## Extending the verified matrix

**When a new dlt minor is released, verify it against the committed state-schema dumps before adding it:**

1. Re-run [`ci/dump_state_schema.py`](https://github.com/earlybirdvc/dlt-ops/blob/main/ci/dump_state_schema.py) per destination on the new minor and diff against the committed dumps (`ci/state-schema-dumps/`).
2. On an empty diff, extend — together, in one change — `ci/dlt-versions.txt`, `SUPPORTED_DLT_MINORS` in `dlt_ops/_compat.py`, and the matrix above; raise the `pyproject.toml` floor only when an old minor is dropped. The test suite enforces the sync, and the non-blocking `test-dlt-latest` CI lane gives early warning when a brand-new dlt minor breaks the suite.
