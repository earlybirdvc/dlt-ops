---
description: How dlt-ops is tested — the credential-free default lane every PR must keep green, the opt-in integration lane and its cross-system triangle, the CI job map and which jobs gate a merge, and the dlt verified-matrix machinery.
---

# Testing

`dlt-ops` has one test suite with two lanes: a credential-free default that every PR must keep green, and an opt-in `integration` selection that drives real source and destination systems. This page maps the lanes to what they prove, shows how to run the credentialed ones locally, walks the CI jobs and which of them gate a merge, and covers the dlt-minor verified-matrix machinery.

**Test lanes at a glance**

| Lane | Command | Covers | In CI |
|---|---|---|---|
| Default (credential-free) | `uv run --no-sync pytest` | the whole suite against local DuckDB — no cloud credentials, no network | required — every `test` matrix cell |
| Integration | `uv run --no-sync pytest -m integration` | the cross-system triangle plus the live-Postgres adapter suite | required — the `integration` job |
| Other CI lanes | see [the CI job map](#the-ci-job-map) | BigQuery, Airflow, Windows, and newest-dlt | non-blocking |

## The default lane is credential-free

**The default lane runs the entire suite against local DuckDB — no cloud credentials, no network — and is the lane every PR must keep green.**

```bash
uv run --no-sync pytest
```

The end-to-end example-project suite (`tests/test_e2e_example.py`) actively fails on any socket connect, so an accidental network dependency is caught rather than tolerated. This is the bulk of the suite — 951 of 998 tests at the time of writing.

The other 47 carry the `integration` marker and self-skip here: each integration test is gated on its backing being present (psycopg2 for Postgres, credentials for BigQuery, the `[airflow]` extra for the Airflow adapter) and skips cleanly when it is absent, so the default lane stays credential-free even though the marked tests are collected.

## The integration marker and the cross-system triangle

**The `integration` marker (declared in `pyproject.toml` as "exercises a real project tree or destination") selects the tests that touch a live system.** `tests/test_integration_flows.py` is the core of it: three lanes forming a triangle across the three destination shapes the package supports, each driving the installed `dlt-ops` console script as a subprocess against a scaffolded project — the same path a user runs.

| Lane | Flow | Tier | Proves |
|---|---|---|---|
| `TestFilesystemToPostgres` | local JSONL → dlt filesystem source → live Postgres | full | full-tier features against Postgres: `run` lands the seeded rows, the runs ledger writes, `status` reads it back |
| `TestPostgresToDuckDB` | live Postgres table → dlt `sql_database` source → local DuckDB file | full | a SQL-database source feeding a full-tier DuckDB destination end to end, ledger included |
| `TestDuckDBToFilesystem` | local DuckDB → plain connection → local `file://` bucket | core | the loud core-tier degradation: `run` succeeds with a `core (no adapter...)` notice, `status` reports `ledger unsupported` |

Together they cover both directions across filesystem / Postgres / DuckDB and both capability tiers. The two full-tier lanes prove the adapter-routed features (the runs ledger, `status`) actually work against live SQL destinations; the core-tier lane proves the no-adapter path degrades loudly instead of crashing or silently doing nothing — the [destinations concept](../concepts/destinations-and-tiers.md) is the design behind that split.

Each class runs its methods in definition order against one shared project (a staged suite; running a single method with `-k` is unsupported), and seed locations reach the source modules through `DLTX_IT_*` env vars read at call time, so the sandboxed `validate` import never touches the source system.

## Running integration locally

**Postgres is the integration lane most worth running locally.** Install the extra and select the marker:

```bash
uv sync --locked --extra postgres
uv run --no-sync pytest -m integration
```

The `postgres_url` fixture (`tests/conftest.py`) decides where Postgres comes from, in this order:

1. `POSTGRES_URL` — an env var pointing at any reachable instance (CI sets it to a service container; locally it is your override).
2. otherwise a throwaway `postgres:16` docker container the fixture starts on port 55433 and tears down at session end.
3. otherwise the Postgres lanes skip.

psycopg2 (from the `[postgres]` extra) gates the whole fixture, so the credential-free lane — which never installs it — never reaches the docker branch. To point at your own instance instead of docker, set the env var:

```bash
POSTGRES_URL=postgresql://user:pass@localhost:5432/db uv run --no-sync pytest -m integration
```

!!! note
    `-m integration` runs the entire marked selection, not just the Postgres legs. The SQL-source leg of `TestPostgresToDuckDB` needs SQLAlchemy, already present via the dev group's `dlt[sql_database]`; BigQuery-marked tests skip without credentials; Airflow-marked ones skip without the `[airflow]` extra. Nothing in the marked selection needs cloud credentials to pass — the credentialed tests self-skip.

## The CI job map

**`.github/workflows/ci.yml` runs on every PR and every push to `main`.** Its jobs, and which gate a merge (the required/non-blocking split is declared in the file header and enforced by `continue-on-error: true` on the non-blocking ones):

| Job | What it runs | Gates merge? |
|---|---|---|
| `lint` | `ruff check`, `ruff format --check`, and zizmor on the workflows | required |
| `typecheck` | `pyrefly check` | required |
| `guards` | `ci/sql_boundary_guard.sh` | required |
| `pr-title` | conventional-commit check on the PR title (PRs only) | required |
| `test` | the DuckDB lane: `pytest` across 3 Pythons × the dlt minors in `ci/dlt-versions.txt`, zero cloud credentials | required — every matrix cell |
| `integration` | `pytest -m integration` with a `postgres:18-alpine` service container and `POSTGRES_URL` set | required |
| `test-dlt-latest` | the suite against the newest dlt on PyPI | non-blocking |
| `test-airflow` | the Airflow-adapter tests with the `[airflow]` extra | non-blocking |
| `test-windows` | `pytest -m "not integration"` on Windows | non-blocking |
| `integration-bigquery` | `pytest -m integration` with BigQuery credentials | non-blocking |

Notes worth knowing:

- Each `test` matrix cell syncs the locked env, then `uv pip install "dlt~=X.Y.0"` for its matrix dlt minor, then `uv run --no-sync pytest` — the `--no-sync` is what keeps that hand-installed dlt from being reverted. In this lane the two Postgres legs of the triangle self-skip (no psycopg2), but the core-tier `TestDuckDBToFilesystem` leg and the E2E example suite run in every one of the 9 cells; the `integration` job is where the Postgres legs actually execute against live Postgres.
- `test-dlt-latest` is an early-warning lane: the dlt dependency is floor-only, so PyPI can run ahead of the verified matrix. A red run here means the next dlt minor needs the state-schema diff and matrix extension (below) before it is trusted — it does not block your PR.
- `test-windows` is non-blocking until it has a green history on `main`, then flips to required; the package does filesystem discovery, so path-semantics is its risk profile.
- `integration-bigquery` skips cleanly when the `BQ_SERVICE_ACCOUNT_JSON` secret is absent (forks have none), so it never fails a fork PR.
- Python 3.14 is deliberately out of the matrix: the floor dlt minor (1.27) stops declaring support at 3.13 while newer minors declare 3.14, so 3.14 joins when the floor moves.

## The dlt verified matrix

**The package supports a set of dlt minors, verified in CI, with one single source of truth: `ci/dlt-versions.txt` (one `X.Y` per line — currently 1.27, 1.28, 1.29).** Everything derives from that file:

- the `test` matrix (a `dlt-versions` job reads the file into the matrix's dlt axis),
- the `pyproject.toml` dlt floor (the oldest listed minor),
- `SUPPORTED_DLT_MINORS` in `dlt_ops/_compat.py` (the guard that makes remote `clean` refuse on an unverified minor),
- the matrix table in `COMPATIBILITY.md`.

`tests/test_packaging.py` and `tests/test_cleanup.py` fail if any of these drift out of sync.

### Adding a dlt minor

**Add a dlt minor once it releases and `test-dlt-latest` is green on it:**

1. Run `ci/dump_state_schema.py` per destination on the new minor and diff against the committed dumps in `ci/state-schema-dumps/`. The script runs a test pipeline twice, then dumps the `information_schema` columns, row semantics, and state/schema codec of dlt's three internal state tables (`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`) as stable-ordered JSON that diffs mechanically. It runs on a bare `dlt[...]` venv — it deliberately copies dlt-ops's state codec rather than importing package internals — the way its module docstring shows.
2. On an empty diff, extend `ci/dlt-versions.txt`, `SUPPORTED_DLT_MINORS`, and the `COMPATIBILITY.md` matrix together in one change; raise the `pyproject.toml` floor only when you drop an old minor.

**Why the state-schema diff gates it:** remote `clean` rewrites dlt's internal state tables, whose layout dlt-ops reverse-engineers per minor. An empty diff is the evidence that the reverse-engineering still holds on the new minor. The full policy and the verified-by legend are in [compatibility](../reference/compatibility.md).

## Where next

- [Contributing](contributing.md) — dev setup and the conventions CI enforces
- [Compatibility](../reference/compatibility.md) — the verified matrix as published
- [Releases](releases.md) — how a green `main` gates a release
