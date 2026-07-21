# Contributing

The full development documentation lives in the docs site — [contributing](docs/development/contributing.md), [testing](docs/development/testing.md), and [releases](docs/development/releases.md). This file is the short version.

## Dev setup

The project uses [uv](https://docs.astral.sh/uv/). Python 3.11+.

```bash
uv sync --locked                    # package + dev group (pytest, ruff, pyrefly, the DuckDB lane)
uv run pytest                       # full credential-free suite
uv run ruff check                   # lint (line-length 120, target py311)
uv run ruff format --check          # formatting
uv run pyrefly check                # type check
bash ci/sql_boundary_guard.sh       # SQL-boundary guard — must stay green
uv run --no-sync mkdocs build --strict   # docs site (needs `uv sync --locked --group docs` once)
```

## Test lanes

The **default lane is credential-free**: `uv run pytest` runs everything — including the end-to-end example-project suite — against local DuckDB, with no cloud credentials and no network (the E2E suite actively fails on any socket connect). This is the lane every PR must keep green.

The other lanes are opt-in:

- **Integration** (`-m integration`) — the cross-system triangle (filesystem → Postgres, Postgres → DuckDB, DuckDB → filesystem) plus the live-Postgres adapter suite. Set `POSTGRES_URL` to a reachable instance, or have a docker CLI available (the fixture spins up a throwaway container and tears it down):

  ```bash
  uv sync --locked --extra postgres
  uv run --no-sync pytest -m integration
  ```

- **BigQuery** — needs Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account key, as CI does): `uv sync --locked --extra bigquery && uv run --no-sync pytest -m integration`
- **Airflow** — Airflow is deliberately **not** in the dev group; the adapter tests skip their Airflow-marked halves in the default lane: `uv sync --locked --extra airflow && uv run --no-sync pytest tests/test_airflow_runtime.py tests/test_orchestration.py`
- **Sentry sink** — the Sentry-specific tests skip without the SDK: `uv run --with sentry-sdk pytest tests/test_alert_sinks.py`

CI mirrors this ([job map](docs/development/testing.md)): `lint`, `typecheck`, `guards`, `pr-title`, the DuckDB `test` matrix (3 Pythons × the dlt minors in `ci/dlt-versions.txt`), and the credential-free `integration` lane are required; the Airflow, Windows, dlt-latest, and BigQuery lanes are non-blocking.

## Type checking

`pyrefly` is configured in `pyproject.toml` (`[tool.pyrefly]`). Modules importing extra-gated dependencies (`airflow`, `sentry_sdk`, `google.cloud`) are handled via `replace-imports-with-any`, so `uv run pyrefly check` passes in the plain dev env — if you add a new extra-gated import, extend that list rather than installing the extra into the dev group.

## PR expectations

- PRs are **squash-merged** and the **PR title must be a conventional commit** (`feat:`, `fix:`, `docs:`, ...) — the title becomes the commit on main and drives [release automation](docs/development/releases.md). CI lints it.
- Keep the credential-free lane, ruff (check + format), pyrefly, the guard scripts, and the strict docs build green.
- Behavior changes come with tests; changes to the config or rule surface also update [docs/configuration/](docs/configuration/reference.md).
- **Do not hand-edit `CHANGELOG.md`** — it is machine-owned by python-semantic-release and regenerates from commit history at release time.
- Public API = what `dlt_ops/__init__.py` (and explicitly public subpackages) export. Everything else is internal with no stability promise; don't grow the public surface casually.
- Dependencies declare floors, never upper bounds.

## Plugin authoring

Everything extensible in `dlt-ops` extends through **one mechanism**: Python entry points. Six axes, each with an entry-point group named `dlt_ops.<axis>`:

| Axis | Entry-point group | Contract |
|---|---|---|
| `destination` | `dlt_ops.destination` | `dlt_ops.DestinationAdapter` Protocol |
| `alert_sink` | `dlt_ops.alert_sink` | `AlertSink` Protocol (`emit_drift` / `emit_error` / `flush`) |
| `validators` | `dlt_ops.validators` | zero-arg callable returning an iterable of `RuleSpec` |
| `secret_backend` | `dlt_ops.secret_backend` | `dlt_ops.SecretBackend` Protocol (+ optional `secret_requests` hook) |
| `orchestrator` | `dlt_ops.orchestrator` | reserved; first-party Airflow adapter ships via the `[airflow]` extra |
| `assertion` | `dlt_ops.assertion` | `dlt_ops.AssertionType` Protocol (`check_config` static half + `start`/`observe`/`finalize` runtime half) |

The group names are frozen public API. For quick experiments and tests there is a runtime twin that feeds the same registry:

```python
import dlt_ops

@dlt_ops.register("alert_sink", "my_sink")
class MySink: ...
```

Full worked examples — a complete destination adapter built, installed, and run at both tiers, plus one example per remaining axis — live in the [adapter-authoring guide](docs/guides/write-a-destination-adapter.md) and the [plugin-authoring guide](docs/guides/write-plugins.md).
