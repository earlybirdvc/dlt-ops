---
description: Task guide — take a destination from core tier to full tier by implementing the DestinationAdapter Protocol and shipping it as a dlt_ops.destination entry point; a QuackDB demo (DuckDB registered under a different engine name) runs the whole build offline — write the adapter, register it, watch the project gain a queryable _dlt_ops_runs table, then test it like the first-party adapters.
---

# Write a destination adapter

**Try `register_derived_adapter` first.** dlt publishes, per destination, most of what an adapter needs, so for any destination declaring a `sqlglot_dialect` a whole working adapter can be built from that alone — one call, no code. Hand-writing one is what you do when a **driver** fact differs from what its dialect implies: a paramstyle the dialect does not write, a `NULL` the driver refuses to bind, an `information_schema` scoped per dataset instead of globally, or a schema that infrastructure rather than the adapter owns. Start at [reaching full tier](../reference/destinations.md#reaching-full-tier) — if a derived adapter covers your destination, you are done, and you should read what derivation does and does not prove before relying on it.

This guide is the other path: you implement the `DestinationAdapter` Protocol, ship it as an entry point in your own distribution, and watch the same project go from "runs ledger skipped" to a queryable `_dlt_ops_runs` table — without touching `dlt-ops` itself. Read [destinations and capability tiers](../concepts/destinations-and-tiers.md) first for why the boundary is shaped this way.

**Prerequisites**

- `dlt-ops` installed, plus `dlt[duckdb]` and `sqlglot` — both pulled by the scratch distribution you build in step 1; the demo runs locally with zero credentials.
- The demo engine is **QuackDB**: dlt's DuckDB destination registered under a different engine name, so every step exercises exactly the code paths a real adapter exercises. For a real engine you replace the dialect facts (transpile target, placeholder style, capability flags, fragments) — the wiring, registration, and verification loop stay identical.

**Sections at a glance**

The [contract](#the-contract) is the `DestinationAdapter` Protocol surface; the build then goes:

1. [Start state: an engine with no adapter](#1-start-state-an-engine-with-no-adapter)
2. [Write the adapter](#2-write-the-adapter)
3. [Register it](#3-register-it)
4. [Run at full tier](#4-run-at-full-tier)
5. [Runtime registration, where an install isn't feasible](#5-runtime-registration-where-an-install-isnt-feasible)
6. [Test it like the first-party adapters](#6-test-it-like-the-first-party-adapters)

## The contract

**An adapter is one class satisfying the `dlt_ops.DestinationAdapter` Protocol** (`dlt_ops/destinations/protocol.py` — its docstrings are the spec). The boundary rules, fixed by the [canonical-SQL design](../concepts/destinations-and-tiers.md#one-canonical-sql-dialect-one-boundary):

- **Callers hand you canonical SQL** in the DuckDB dialect with positional `?` placeholders, plus the parameters. Your adapter owns the whole translation: transpile to the native dialect, convert placeholders to the native style, execute through the live dlt `sql_client` the caller passes in. Parameter values never enter the SQL text — swap placeholders as sqlglot AST nodes, bind values natively. (The one sanctioned exception is typed, not textual: a driver that cannot bind some value — BigQuery rejects a bound `None` — may inline it as an AST literal.)
- **Adapters never construct credentials or clients.** Callers own pipeline attachment and hand a live client in; your class is stateless dialect knowledge.
- **Fragments are canonical too.** `timestamp_now_sql` and `timestamp_sub_days_sql(days)` are written in the canonical dialect, not the native one, because sqlglot transpiles syntax rather than every function idiom. They are shared defaults — `CURRENT_TIMESTAMP` and `<now> - INTERVAL 'N days'` — that every first-party adapter inherits unchanged and that are snapshot-locked per adapter in `tests/test_destinations.py`. Declare your own only if transpilation does not produce a spelling your destination accepts.

The full member surface — the Tier-2 preflight probes every one of these, attributes included, so a missing member fails runs targeting your destination:

| Member | Contract |
|---|---|
| `name: str` | Registry key, and nothing more. Must equal the engine name dlt reports (`Destination.to_name(destination_type)`) and your entry-point name. It is deliberately **not** the transpile target: which dialect you write is your adapter's own business and stays out of the port. |
| `placeholder_style: str` | The native positional placeholder token of your destination's dlt `sql_client`, as a plain string. The contract is only that the token is what your client binds against — the Protocol enumerates no closed set, so a driver wanting `:1` or `%(name)s` conforms like any other. Informational for diagnostics; conversion happens inside `execute_sql` / `execute_query`, never in caller code. |
| `supports_if_exists: bool` | `CREATE TABLE IF NOT EXISTS` / `DROP TABLE IF EXISTS` are valid DDL; `drop_table_if_exists` falls back to probe-then-drop when False. |
| `supports_create_schema_if_not_exists: bool` | The adapter may create the schema/dataset via `ensure_schema`; when False, `ensure_schema` must be a no-op (BigQuery's choice: dataset creation is owned by dlt/infra). |
| `timestamp_now_sql: str` | Canonical-dialect fragment for "now" that survives your transpile. |
| `timestamp_sub_days_sql(days) -> str` | Canonical-dialect fragment for "now minus N days" — interval arithmetic is the idiom sqlglot most often mistranslates. |
| `render_identifier(ident) -> str` | Validate against your identifier grammar, quote canonically (DuckDB-style); raise `ValueError` outside the grammar. |
| `render_table_ref(dataset, table) -> str` | `dataset.table` in canonical form, both parts validated. |
| `execute_sql(client, sql, *params) -> None` | Transpile, bind, execute. |
| `execute_query(client, sql, *params) -> Cursor` | Same, returning a cursor with `fetchone()` / `fetchall()`. |
| `table_exists(client, dataset, table) -> bool` | Existence probe. |
| `drop_table_if_exists(client, dataset, table) -> None` | Idempotent drop. |
| `ensure_schema(client, dataset) -> None` | Create the schema when supported and needed; callers invoke it unconditionally. |
| `fetch_columns(client, dataset, table) -> list[ColumnInfo] \| None` | Columns from one `information_schema.columns` SELECT; `None` when the table (or its dataset) is absent — never an empty list. |

`ColumnInfo` and the `Cursor` protocol are public imports from `dlt_ops.destinations`. The DuckDB adapter (`dlt_ops/destinations/duckdb.py`) is the smallest real implementation to crib from — note it builds on an internal base class (`_base.py`) that third-party adapters should not import; implement the Protocol directly, as this guide does.

## 1. Start state: an engine with no adapter

**Create the scratch distribution that plays your destination's vendor package.** At this step it ships only the demo engine — a dlt destination factory, no adapter yet:

```text
dlt-ops-quackdb/
├── pyproject.toml
└── src/dlt_ops_quackdb/
    ├── __init__.py     # the QuackDB engine (demo scaffolding — your engine already exists)
    └── adapter.py      # the DestinationAdapter (step 2)
```

```toml
[project]
name = "dlt-ops-quackdb"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["dlt-ops", "dlt[duckdb]", "sqlglot"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/dlt_ops_quackdb"]
```

`src/dlt_ops_quackdb/__init__.py` — the stand-in engine. dlt derives the engine name from the factory class, so a pipeline built on this reports `quackdb`, not `duckdb`:

```python
"""QuackDB: a DuckDB-compatible engine, standing in for your real destination.

The factory subclasses dlt's DuckDB destination so the demo runs locally with
zero credentials; dlt derives the engine name from the class name, so a
pipeline built on it reports `quackdb`, not `duckdb`. A real adapter project
skips this file entirely — your engine already exists.
"""

import dlt


class quackdb(dlt.destinations.duckdb):
    pass
```

Install it next to `dlt-ops`, scaffold a project, and point the project at the new engine (a fully qualified factory path is a destination reference dlt resolves like any other):

```bash
pip install -e dlt-ops-quackdb
dlt-ops init quackdemo --example
cd quackdemo
```

In `.dlt/config.toml`, change the scaffolded destination:

```toml
[dlt_ops]
default_destination = "dlt_ops_quackdb.quackdb"
```

Run it. The engine resolves, the data loads — and everything adapter-gated announces its absence, because tier resolution found no adapter registered for the engine name `quackdb`:

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
Pipeline Configuration
----------------------------------------
  Source: demo_events
  Function: demo_events_source
  Resources: all (1 total)
  Destination: dlt_ops_quackdb.quackdb
  Dataset: demo_data (from .dlt/config.toml)
  Capabilities: core (no adapter: runs ledger and status, checkpoints, backfill, clean (remote), reconcile, assertion quarantine unavailable)

Starting pipeline...

2026-07-17 11:01:45|[WARNING]|dlt_ops.discovery.runner|destination 'dlt_ops_quackdb.quackdb' has no registered DestinationAdapter — running in core mode; adapter-gated features unavailable: runs ledger and status, checkpoints, backfill, clean (remote), reconcile, assertion quarantine; extract/load, fail/warn assertions, and trace persistence run normally
2026-07-17 11:01:45|[INFO]|dlt_ops.runs.writer|runs ledger skipped: destination 'dlt_ops_quackdb.quackdb' has no DestinationAdapter (core mode)
...
1 load package(s) were loaded to destination duckdb and into dataset demo_data
The duckdb destination used duckdb:////tmp/quackdemo/demo_events_pipeline.duckdb location to store data
Load package 1784278905.737501 is LOADED and contains no failed jobs
```

(dlt's own summary still says `destination duckdb` — the stand-in engine runs dlt's DuckDB implementation underneath. Every `dlt-ops` decision keyed on the engine name `quackdb`.) `status` reports the capability gap as its own state, distinct from an outage:

```bash
dlt-ops pipeline status
```

```text
Source: demo_events
  ! ledger unsupported: destination 'dlt_ops_quackdb.quackdb' has no DestinationAdapter (core mode)
```

## 2. Write the adapter

**`src/dlt_ops_quackdb/adapter.py` — the whole contract in one file.** For a real engine, the parts you change are marked: the transpile target, the placeholder conversion, the capability flags, and the fragments.

```python
"""DestinationAdapter for QuackDB — the whole full-tier contract in one file.

Package code hands every adapter canonical SQL in the DuckDB dialect with
positional ``?`` placeholders; this class owns the translation to QuackDB's
native dialect and the execution through the live dlt sql_client the caller
passes in. Nothing else in dlt-ops knows QuackDB exists.
"""

import re
from typing import Any

import sqlglot
from sqlglot import exp

from dlt_ops.destinations import ColumnInfo


class _Rows:
    """Rows drained eagerly from a dlt cursor.

    dlt's ``execute_query`` cursor only lives inside its context manager, so
    the adapter materializes before the context closes. Result sets at this
    boundary (checkpoint lookups, column listings) are small by design.
    """

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def fetchone(self) -> Any:
        return self._rows.pop(0) if self._rows else None

    def fetchall(self) -> list[Any]:
        rows, self._rows = self._rows, []
        return rows


class QuackDBAdapter:
    name = "quackdb"
    """The registry key: must equal the engine name dlt reports for your
    destination (``Destination.to_name(destination_type)``) AND the
    entry-point name. Tier resolution is a lookup of this string."""

    placeholder_style = "?"
    """QuackDB's DB-API placeholder. Informational for diagnostics — the
    conversion from canonical ``?`` happens inside ``execute_*`` below."""

    # Capability flags: which DDL shapes are safe to emit against this engine.
    # Callers branch on these instead of guessing (checkpoint DDL, schema
    # creation).
    supports_if_exists = True
    supports_create_schema_if_not_exists = True

    # Fragments, written in the CANONICAL dialect: sqlglot transpiles syntax,
    # not every function idiom, so each adapter owns the idioms it guarantees
    # survive its own transpile step. Interval arithmetic is the classic
    # casualty — hence a method, not a constant.
    timestamp_now_sql = "CURRENT_TIMESTAMP"

    # QuackDB speaks DuckDB's SQL dialect, so transpile targets "duckdb".
    # A real engine puts its own sqlglot dialect name here ("snowflake", ...).
    _write_dialect = "duckdb"

    # dlt datasets and tables are snake_case; anything wider would not be
    # portable across destinations anyway. Reject early, before quoting.
    _identifier_re = re.compile(r"[A-Za-z0-9_]+")

    def timestamp_sub_days_sql(self, days: int) -> str:
        return f"CURRENT_TIMESTAMP - INTERVAL '{int(days)} days'"

    def render_identifier(self, ident: str) -> str:
        if not isinstance(ident, str) or not self._identifier_re.fullmatch(ident):
            raise ValueError(f"invalid quackdb identifier {ident!r}: must match {self._identifier_re.pattern}")
        # Canonical (DuckDB) quoting; _transpile converts it to native quoting.
        return f'"{ident}"'

    def render_table_ref(self, dataset: str, table: str) -> str:
        return f"{self.render_identifier(dataset)}.{self.render_identifier(table)}"

    def _transpile(self, canonical_sql: str, param_count: int) -> str:
        """One canonical statement -> one native statement, placeholders intact.

        Params never enter the SQL text: placeholders are AST nodes, and the
        count is asserted against what the caller actually bound.
        """
        statements = sqlglot.parse(canonical_sql, read="duckdb")
        if len(statements) != 1 or statements[0] is None:
            raise ValueError(f"expected exactly one canonical SQL statement, got {len(statements)}")
        statement = statements[0]
        placeholders = list(statement.find_all(exp.Placeholder))
        if any(p.this for p in placeholders):
            raise ValueError("canonical SQL must use positional '?' placeholders, not named ones")
        if len(placeholders) != param_count:
            raise ValueError(f"placeholder/param mismatch: {len(placeholders)} '?' placeholders, {param_count} params")
        # QuackDB's native placeholder is already '?'. An adapter whose driver
        # wants %s or $1 replaces each node here — as AST nodes, never string
        # substitution: p.replace(exp.Var(this="%s")).
        return statement.sql(dialect=self._write_dialect)

    def execute_sql(self, client: Any, canonical_sql: str, *params: Any) -> None:
        client.execute_sql(self._transpile(canonical_sql, len(params)), *params)

    def execute_query(self, client: Any, canonical_sql: str, *params: Any) -> _Rows:
        with client.execute_query(self._transpile(canonical_sql, len(params)), *params) as cursor:
            return _Rows(list(cursor.fetchall()))

    def table_exists(self, client: Any, dataset: str, table: str) -> bool:
        return self.fetch_columns(client, dataset, table) is not None

    def drop_table_if_exists(self, client: Any, dataset: str, table: str) -> None:
        self.execute_sql(client, f"DROP TABLE IF EXISTS {self.render_table_ref(dataset, table)}")

    def ensure_schema(self, client: Any, dataset: str) -> None:
        # No-op when supports_create_schema_if_not_exists is False (BigQuery's
        # choice: dataset creation is owned by infra) — callers call this
        # unconditionally either way.
        self.execute_sql(client, f"CREATE SCHEMA IF NOT EXISTS {self.render_identifier(dataset)}")

    def fetch_columns(self, client: Any, dataset: str, table: str) -> list[ColumnInfo] | None:
        # dataset/table are DATA here — bound params, not identifiers.
        rows = self.execute_query(
            client,
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            dataset,
            table,
        ).fetchall()
        if not rows:
            return None  # absent table (or dataset) — never an empty list
        return [ColumnInfo(name=str(row[0]), data_type=str(row[1])) for row in rows]
```

Two design points worth internalizing before you adapt this to a real engine. First, `_transpile` is where the injection guarantee lives: parsing to an AST and asserting the placeholder count means a hostile parameter value physically cannot become SQL text, and `tests/test_destinations.py` probes exactly that with a `"x'); DROP TABLE t;--"` round trip. Second, `fetch_columns` binds `dataset` and `table` as parameters while `render_table_ref` validates them as identifiers — the same string is data in one statement and an identifier in another, and the adapter is the one place that distinction is enforced.

## 3. Register it

**The entry point is the whole integration — append to the distribution's `pyproject.toml`:**

```toml
[project.entry-points."dlt_ops.destination"]
quackdb = "dlt_ops_quackdb.adapter:QuackDBAdapter"
```

Entry points conventionally register the class, instantiated with no arguments (a ready instance also works). Reinstall — entry-point metadata only refreshes on install, even for editable installs, so a pyproject edit without a reinstall silently changes nothing:

```bash
pip install -e dlt-ops-quackdb
dlt-ops plugins doctor
```

```text
destination:
  bigquery  [dlt-ops]  dlt_ops.destinations.bigquery:BigQueryAdapter
  duckdb  [dlt-ops]  dlt_ops.destinations.duckdb:DuckDBAdapter
  postgres  [dlt-ops]  dlt_ops.destinations.postgres:PostgresAdapter
  quackdb  [dlt-ops-quackdb]  dlt_ops_quackdb.adapter:QuackDBAdapter
...
plugins doctor: OK
```

Your adapter sits in the same registry as the first-party ones, with its distribution and object path as provenance — there is no privileged path ([plugins](../concepts/plugins.md)).

## 4. Run at full tier

**Same project, same command.** The tier line flips, the core-mode warning and both "ledger skipped" lines disappear, and the ledger writes go through your adapter:

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
Pipeline Configuration
----------------------------------------
  Source: demo_events
  Function: demo_events_source
  Resources: all (1 total)
  Destination: dlt_ops_quackdb.quackdb
  Dataset: demo_data (from .dlt/config.toml)
  Capabilities: full

Starting pipeline...
...
1 load package(s) were loaded to destination duckdb and into dataset demo_data
The duckdb destination used duckdb:////tmp/quackdemo/demo_events_pipeline.duckdb location to store data
Load package 1784278982.378659 is LOADED and contains no failed jobs
```

`status` now reads runs back instead of reporting `ledger unsupported`:

```bash
dlt-ops pipeline status
```

```text
Source: demo_events
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-17 09:03:02  2026-07-17 09:03:02  6         cli        -               bf5eed7fee49
```

And the row physically lives in the destination — inserted, via your `execute_sql`, into the `_dlt_ops_runs` table your adapter's DDL created ([runs ledger](../concepts/runs-ledger.md)):

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("demo_events_pipeline.duckdb", read_only=True)
con.sql("SET TimeZone = 'UTC'")
print(con.sql("SELECT status, destination, records_loaded, trigger_source FROM demo_data._dlt_ops_runs ORDER BY started_at"))
PY
```

```text
┌───────────┬─────────────────────────┬────────────────┬────────────────┐
│  status   │       destination       │ records_loaded │ trigger_source │
│  varchar  │         varchar         │     int64      │    varchar     │
├───────────┼─────────────────────────┼────────────────┼────────────────┤
│ completed │ dlt_ops_quackdb.quackdb │              6 │ cli            │
└───────────┴─────────────────────────┴────────────────┴────────────────┘
```

One adapter unlocks all six gated features at once, because they all emit the same canonical SQL — the reconciler, for instance, now answers through your `fetch_columns`:

```bash
dlt-ops pipeline reconcile -s demo_events --dry-run
```

```text
Dry-run: alert emission suppressed

Source: demo_events  |  Findings: 0  |  Duration: 0.33s
  ✓ No drift
```

## 5. Runtime registration, where an install isn't feasible

**`dlt_ops.register` is the entry point's runtime twin — it feeds the same process-wide registry, so `get`/lookup behavior is identical.** It only exists in the process that executed it: the CLI is a separate process, so shipping an adapter to the toolchain always means the entry point. The runtime form is for pytest fixtures, notebooks, and scripts that drive the Python API in-process. `register_derived_adapter("motherduck")` is the shortest version of this — same registry, same tier, nothing to subclass — and the explicit form below is what you write when your adapter must override a driver fact the derivation cannot see. MotherDuck is DuckDB-hosted, so upgrading it in a session is one subclass either way:

```python
import dlt_ops
from dlt_ops.destinations import has_adapter

from dlt_ops_quackdb.adapter import QuackDBAdapter

print("motherduck before:", has_adapter("motherduck"))

@dlt_ops.register("destination", "motherduck")
class MotherDuckAdapter(QuackDBAdapter):
    """MotherDuck is DuckDB-hosted: the same dialect facts, a different engine name."""
    name = "motherduck"

print("motherduck after: ", has_adapter("motherduck"))
```

```text
motherduck before: False
motherduck after:  True
```

## 6. Test it like the first-party adapters

**`tests/test_destinations.py` in the `dlt-ops` repository is the behavioral spec worth mirroring in your own suite:** snapshot-lock the native SQL your transpile produces for the checkpoint/ledger statement shapes, assert the placeholder/param-count mismatch raises before execution, run the injection probe (the hostile value must round-trip as data while a decoy table survives), and check `fetch_columns` returns `None` — not `[]` — for absent tables and datasets. The live half of that file runs the same shapes through a real dlt `sql_client`; `tests/test_destinations_postgres.py` shows the same pattern against a disposable live instance.

## Troubleshooting: a present-but-broken adapter fails loudly

**Registration alone does not earn trust.** Delete one member — say `supports_if_exists` — reinstall, and `validate` fails on every source resolving to your destination (Tier-2 preflight runs the same probe on every `run` and `backfill`, so a scheduler-triggered run that never saw `validate` is equally protected):

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✗ 1 error(s):
  [demo_events] destination: destination adapter 'quackdb' is missing required capability member(s): supports_if_exists. The DestinationAdapter Protocol is the contract; the plugin is incomplete or outdated.
```

`plugins doctor` stays green through this — doctor proves registration and importability, not Protocol completeness. The capability probe belongs to `validate` and the preflight, because "installed features silently lost" is the failure mode the [failure-semantics contract](../concepts/failure-semantics.md) exists to forbid.

## Where next

- [Destinations and capability tiers](../concepts/destinations-and-tiers.md) — the tier model and the canonical-SQL boundary your adapter implements
- [Destinations reference](../reference/destinations.md) — the feature × tier matrix
- [Write plugins](write-plugins.md) — the other plugin axes: alert sinks, assertion types, validator providers, secret backends
