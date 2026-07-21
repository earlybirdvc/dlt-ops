---
description: Task guide — ship one plugin on each of the four remaining dlt-ops extension axes (alert sink, assertion type, validator provider, secret backend) from one scratch distribution and prove each live in plugins doctor and against a running project; the destination axis has its own guide and the orchestrator axis is reserved. Ends with name-collision resolution via [dlt_ops.plugins].
---

# Write plugins

This guide ships one plugin on each of the four remaining extension axes — an alert sink, an assertion type, a validator provider, and a secret backend — inside a single scratch distribution, then proves each one live: registered in `plugins doctor`, and doing its job against a running project. The `destination` axis has [its own guide](write-a-destination-adapter.md); the `orchestrator` axis is reserved in v0.1.

**Prerequisites**

- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md).
- Read [plugins](../concepts/plugins.md) first for the loader policy these examples rely on — its [six axes](../concepts/plugins.md#the-six-axes) table is the canonical axis reference (entry-point group, contract, first-party names), which this guide links to rather than repeats.

**Steps at a glance**

1. [One distribution, four entry points](#1-one-distribution-four-entry-points)
2. [An alert sink](#2-an-alert-sink)
3. [An assertion type](#3-an-assertion-type)
4. [A validator provider](#4-a-validator-provider)
5. [A secret backend](#5-a-secret-backend)
6. [Name collisions and `[dlt_ops.plugins]`](#6-name-collisions-and-dlt_opsplugins)

## 1. One distribution, four entry points

**Every axis extends the same way, so one distribution can serve several axes** — the entry-point groups `dlt_ops.<axis>` are the whole integration, no config needed to enable anything. The scratch distribution:

```text
acme-dlt-ops/
├── pyproject.toml
└── src/acme_dlt_ops/
    ├── __init__.py
    ├── sink.py         # alert sink
    ├── assertion.py    # assertion type
    ├── rules.py        # validator provider
    └── secrets.py      # secret backend
```

```toml
[project]
name = "acme-dlt-ops"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["dlt-ops"]

[project.entry-points."dlt_ops.alert_sink"]
filedump = "acme_dlt_ops.sink:FileDumpAlertSink"

[project.entry-points."dlt_ops.assertion"]
acme_suffix_check = "acme_dlt_ops.assertion:SuffixCheck"

[project.entry-points."dlt_ops.validators"]
acme = "acme_dlt_ops.rules:acme_rules"

[project.entry-points."dlt_ops.secret_backend"]
acme_env = "acme_dlt_ops.secrets:AcmeEnvBackend"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/acme_dlt_ops"]
```

Write the four modules (each section below), install the distribution next to `dlt-ops`, scaffold a demo project, and check the registry:

```bash
pip install -e acme-dlt-ops
dlt-ops init demo --example
cd demo
dlt-ops plugins doctor
```

```text
...
assertion:
  acme_suffix_check  [acme-dlt-ops]  acme_dlt_ops.assertion:SuffixCheck
  max_rows_per_load  [dlt-ops]  dlt_ops.assertions.builtin:MaxRowsPerLoad
...
validators:
  acme  [acme-dlt-ops]  acme_dlt_ops.rules:acme_rules
  airflow  [dlt-ops]  dlt_ops.airflow.validators:airflow_rules
...
secret_backend:
  acme_env  [acme-dlt-ops]  acme_dlt_ops.secrets:AcmeEnvBackend
  airflow  [dlt-ops]  dlt_ops.airflow.secrets:AirflowVariableBackend
  secrets_toml  [dlt-ops]  dlt_ops.secrets.default:SecretsTomlBackend
alert_sink:
  filedump  [acme-dlt-ops]  acme_dlt_ops.sink:FileDumpAlertSink
  logging  [dlt-ops]  dlt_ops.reconciler._emission:LoggingAlertSink
  sentry  [dlt-ops]  dlt_ops.sentry:SentryAlertSink
plugins doctor: OK
```

All four plugins sit next to the first-party ones with their distribution as provenance. If you edit entry points later, reinstall — entry-point metadata only refreshes on install, even for editable installs.

## 2. An alert sink

**Alert sinks receive schema-drift findings and reconciler-internal errors.** The contract (`dlt_ops.AlertSink`, defined in `dlt_ops.reconciler.protocols`) is three methods: `emit_drift(finding)`, `emit_error(exc, *, source_name, resource_name, context)`, and `flush(timeout)`. Register a class and it is constructed with the project's `[dlt_ops.alert_sink.<name>]` table as keyword arguments (a ready instance also works; its options are then ignored with a warning). Every public reconcile entry point calls `flush` on the way out, so a sink with a background transport must drain it there — a short-lived CLI or orchestrator task may exit immediately after. All configured sinks receive every event, and one raising sink is logged and never blocks the others.

`src/acme_dlt_ops/sink.py`:

```python
"""Toy alert sink: appends one JSON line per reconciler event to a file."""

import json
from pathlib import Path


class FileDumpAlertSink:
    def __init__(self, *, path: str = "alerts.jsonl") -> None:
        self._path = Path(path)

    def emit_drift(self, finding) -> None:
        record = {
            "event": "drift",
            "kind": str(finding.kind),
            "source": finding.source_name,
            "resource": finding.resource_name,
            "columns": list(finding.columns),
        }
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def emit_error(self, exc, *, source_name, resource_name=None, context) -> None:
        record = {
            "event": "error",
            "source": source_name,
            "resource": resource_name,
            "context": context,
            "error": f"{type(exc).__name__}: {exc}",
        }
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def flush(self, timeout: float = 2.0) -> None:
        return None
```

Enable it in the demo project's `.dlt/config.toml` — the list picks sinks by plugin name, the table carries this sink's constructor options:

```toml
[dlt_ops]
alert_sinks = ["logging", "filedump"]

[dlt_ops.alert_sink.filedump]
path = "drift-alerts.jsonl"
```

Prove it end to end: load once, commit the classic crime (`ALTER TABLE` behind the model's back), reconcile, and read the sink's output file. Both configured sinks fire — the `logging` WARNING line and one JSON line from yours:

```bash
dlt-ops pipeline run -s demo_events -y
python - <<'PY'
import duckdb
con = duckdb.connect("demo_events_pipeline.duckdb")
con.execute("ALTER TABLE demo_data.events ADD COLUMN ip_address VARCHAR")
con.execute("UPDATE demo_data.events SET ip_address = '203.0.113.' || (id % 40)")
con.close()
PY
dlt-ops pipeline reconcile -s demo_events
```

```text
2026-07-17 11:07:54|[WARNING]|dlt_ops.reconciler._emission|schema drift (additive): my_pipeline.demo_events.events — 1 column(s): ip_address | reproduce: SELECT "ip_address" FROM "demo_data"."events" LIMIT 5
Source: demo_events  |  Findings: 1  |  Duration: 0.41s
  • events: additive drift (1 column(s))
      Columns: ip_address
      First seen: 2026-07-17T09:07:54.953153+00:00
```

```bash
cat drift-alerts.jsonl
```

```text
{"event": "drift", "kind": "additive", "source": "demo_events", "resource": "events", "columns": ["ip_address"]}
```

A typo'd sink name never silently drops alerts: every name in `alert_sinks` is checked by the `alert_sink_registered` rule at `validate` and again by the Tier-2 preflight at run time — the [drift-detection guide](drift-detection.md) shows that refusal.

## 3. An assertion type

**An assertion type is a pre-load data-quality check users declare per resource — your entry-point name IS the TOML key.** The contract (`dlt_ops.AssertionType`) has a static half and a runtime half. `check_config(params, ctx)` must catch everything checkable without data: param types and domains, and column references against `ctx.declared_columns` — skipping the column check when that is `None` (no resolvable model is another rule's problem). `start(params)` returns a fresh accumulator per (resource, run); `observe(state, row, params)` returns `None` or a message meaning *this row* fails; `finalize(state, params)` returns the batch verdict. `row_scoped` is the single scope discriminator: `False` means only `finalize` can fail, and `on_failure = "quarantine"` becomes a config error. The engine owns policy — your type never sees `on_failure`, never writes quarantine rows, never touches destination clients — which keeps it trivially testable: construct, feed rows, assert messages. Params arrive normalized: a shorthand declaration (`acme_suffix_check = ".html"`) reaches you as `{"value": ".html"}`. Do not register the reserved names `on_failure` or `custom` — `validate` rejects a plugin that squats them.

`src/acme_dlt_ops/assertion.py`:

```python
"""Toy assertion type: a string column's values must end with a configured suffix."""


class SuffixCheck:
    name = "acme_suffix_check"  # registry name == entry-point name == the TOML key
    row_scoped = True  # observe() emits per-row verdicts (quarantine-compatible)

    def check_config(self, params, ctx):
        """The static half: everything checkable without data, checked here."""
        errors = []
        value = params.get("value")
        if not isinstance(value, str) or not value:
            errors.append(f"acme_suffix_check requires a non-empty string value, got {value!r}")
        column = params.get("column")
        if not isinstance(column, str) or not column:
            errors.append(f"acme_suffix_check requires a 'column' param naming the column to check")
        elif ctx.declared_columns is not None and column not in ctx.declared_columns:
            # Skip the existence check when declared_columns is None — the
            # resource has no resolvable model, and that is another rule's job.
            errors.append(
                f"acme_suffix_check references column {column!r} not on the declared model "
                f"(declared: {', '.join(ctx.declared_columns)})"
            )
        return errors

    def start(self, params):
        return None  # a pure row-scoped check accumulates nothing

    def observe(self, state, row, params):
        value = str(row.get(params["column"], ""))
        if value.endswith(params["value"]):
            return None
        return f"{params['column']} value {value!r} does not end with {params['value']!r}"

    def finalize(self, state, params):
        return None  # no batch verdict; row verdicts did the work
```

Declare it on the demo resource like any built-in — and typo the column first, to watch your static half work through `validate` (the always-on `assertion_config_valid` / `assertion_columns_exist` rules run `check_config`):

```toml
[sources.demo_events.dlt_ops.assertions.events]
acme_suffix_check = { column = "knd", value = "up" }
```

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✗ 1 error(s):
  [demo_events] assertions.events.acme_suffix_check: acme_suffix_check references column 'knd' not on the declared model (declared: id, kind, occurred_at)
```

Fix the column to `"kind"` and run. The runtime half now gates extract; the example's fixture rows include `kind = "login"`, so the default `on_failure = "fail"` aborts before anything loads and drops the extracted package:

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
2026-07-17 11:08:20|[INFO]|dlt_ops.assertions.engine|Assertion gate attached to resource 'events' (1 assertion(s), declaration order)
2026-07-17 11:08:20|[INFO]|dlt_ops.discovery.runner|Dropped pending load package(s) after assertion failure
...
dlt_ops.assertions.models.AssertionFailedError: assertion 'acme_suffix_check' failed on demo_events.events: kind value 'login' does not end with 'up'
```

`on_failure = "warn"` and (because `row_scoped = True`) `"quarantine"` come for free — policy is declared next to the params and executed by the engine, exactly as for the built-ins ([assertions](../concepts/assertions.md)). Drop the block, or downgrade it to `warn`, before moving on — the fixture data keeps failing it by design.

## 4. A validator provider

**A validator provider is a zero-argument callable returning an iterable of `dlt_ops.RuleSpec`** — each spec a stable `rule_id`, a validator function (`ValidationContext -> list[ValidationError]`), and your plugin name as provenance. Installed rules auto-activate (`default_on=False` ships a rule opt-in) and get the `[dlt_ops.rules]` knob and per-source `rule_exemptions` for free. Prefix rule IDs with your plugin name: they are globally unique, and a duplicate ID is skipped and reported at assembly time. If your rules only apply when some dependency is importable, return `()` in its absence — the first-party Airflow provider (`dlt_ops/airflow/validators.py`) is that pattern, which is why a bare install's doctor stays green.

`src/acme_dlt_ops/rules.py`:

```python
"""Validator provider: acme's project conventions as validate rules."""

from dlt_ops import RuleSpec, ValidationContext, ValidationError


def no_shouting_resources(ctx: ValidationContext) -> list[ValidationError]:
    """Resource names must not be ALL CAPS."""
    errors: list[ValidationError] = []
    for name, source in ctx.sources.items():
        for resource in source.resources:
            if resource.isupper():
                errors.append(
                    ValidationError(
                        source_name=name,
                        field="resources",
                        message=f"resource '{resource}' is ALL CAPS; acme convention is snake_case",
                    )
                )
    return errors


def acme_rules() -> tuple[RuleSpec, ...]:
    """Entry-point target: zero args in, RuleSpecs out."""
    return (RuleSpec(rule_id="acme_no_shouting_resources", validator=no_shouting_resources, plugin="acme"),)
```

The rule joins the resolved set of every project in the environment, listed with your plugin name as its origin:

```bash
dlt-ops pipeline validate --show-resolved-rules
```

```text
Resolved rules (24):
  acme_no_shouting_resources           on   acme
  bigquery_partitioning                on   bigquery
  bigquery_partition_hints             on   bigquery
  import_safety                        on   core
  ...
```

And it is switchable through the same knob as every core rule — with `acme_no_shouting_resources = false` under `[dlt_ops.rules]`, the same listing shows:

```text
  acme_no_shouting_resources           off  acme
```

## 5. A secret backend

**A secret backend decides where secrets come from; the runtime calls `get(ref)` and writes the results into `dlt.secrets`.** The required surface is `name` plus `get(key) -> str`, raising `SecretNotFoundError` for a missing secret — never returning a placeholder, so the runtime's `fail_on_missing` policy stays enforceable. A backend *claims* a source by implementing the optional `secret_requests(ext)` hook: it receives the source's raw `[sources.<X>.dlt_ops]` table and returns the fetch plan (`SecretRequest(ref=..., key=...)` entries) when its own trigger keys are present. Trigger keys stay plugin-owned — core never hardcodes them. Exactly one backend may claim a source; no claim falls back to the `secrets_toml` default, which serves without fetching because dlt reads that file natively.

`src/acme_dlt_ops/secrets.py`:

```python
"""Secret backend serving secrets from process environment variables.

A source claims this backend with the plugin-owned trigger key `acme_env`:

    [sources.my_api.dlt_ops.acme_env]
    api_token = "MY_API_TOKEN"    # dlt.secrets leaf = env var to read
"""

import os
from collections.abc import Mapping, Sequence
from typing import Any

from dlt_ops.secrets import SecretNotFoundError, SecretRequest


class AcmeEnvBackend:
    name = "acme_env"

    def secret_requests(self, ext: Mapping[str, Any]) -> Sequence[SecretRequest]:
        """Claim a source iff its [sources.<X>.dlt_ops] table carries our trigger key.

        The returned entries double as the fetch plan: get(ref) will be
        written to dlt.secrets under sources.<section>.<key>.
        """
        table = ext.get("acme_env")
        if not isinstance(table, Mapping):
            return ()
        return tuple(SecretRequest(ref=str(env_var), key=str(leaf)) for leaf, env_var in table.items())

    def get(self, key: str) -> str:
        """Fetch one env var; a missing secret raises — never a placeholder."""
        value = os.environ.get(key)
        if value is None:
            raise SecretNotFoundError(f"environment variable {key!r} is not set")
        return value
```

Claim the demo source in `.dlt/config.toml`:

```toml
[sources.demo_events.dlt_ops.acme_env]
api_token = "DEMO_API_TOKEN"
```

`setup_secrets` is the runtime that consumes the claim — orchestrator adapters call it inside `run_source` before every run (that is how the Airflow backend feeds `dlt.secrets` from Variables), while the CLI dev loop doesn't fetch at all because dlt reads `.dlt/secrets.toml` natively. Drive it directly to watch the resolution:

```bash
DEMO_API_TOKEN=tok-super-secret-123 python - <<'PY'
from pathlib import Path

import dlt
from dlt_ops.secrets import setup_secrets

setup_secrets(project_root=Path("."))
print("resolved:", dlt.secrets["sources.demo_events.api_token"])
PY
```

```text
resolved: tok-super-secret-123
```

The failure contract, demonstrated by unsetting the variable — `get` raises, and `setup_secrets` propagates it because `fail_on_missing` defaults to True:

```bash
python - <<'PY'
from pathlib import Path

from dlt_ops.secrets import setup_secrets

setup_secrets(project_root=Path("."))
PY
```

```text
Traceback (most recent call last):
  ...
dlt_ops.secrets.protocol.SecretNotFoundError: environment variable 'DEMO_API_TOKEN' is not set
```

The chain is enforced at both tiers like everything else: `secret_backend_registered` fails `validate` when a source's engaged backend is unregistered or broken, and the Tier-2 preflight re-resolves the backend per source on every run. Two backends claiming one source is a hard error naming both claimants — a source resolves to exactly one backend.

## 6. Name collisions and `[dlt_ops.plugins]`

**Plugin names are first-come within an axis, so pick names that identify your implementation and never squat the first-party names** (`duckdb`, `bigquery`, `postgres`, `logging`, `sentry`, `secrets_toml`, `airflow`, `core`). When two installed distributions genuinely claim the same `<axis>/<name>`, nothing silently wins. Install a fork of the sink distribution that also registers `alert_sink/filedump` and doctor flags the contest, exit code 1:

```bash
dlt-ops plugins doctor
```

```text
alert_sink:
  filedump  COLLISION: 'acme-alerts-fork' (acme_alerts_fork.sink:FileDumpAlertSink), 'acme-dlt-ops' (acme_dlt_ops.sink:FileDumpAlertSink)
  disambiguate in .dlt/config.toml:
[dlt_ops.plugins.alert_sink]
filedump = "acme-alerts-fork"
  logging  [dlt-ops]  dlt_ops.reconciler._emission:LoggingAlertSink
...
plugins doctor: 0 failure(s), 1 collision(s)
```

Every lookup of the contested name fails the same way — `validate` on the project now errors with the identical disambiguation block in its message:

```text
✗ 1 error(s):
  [dlt_ops.alert_sinks] alert_sinks: alert sink 'filedump' failed to load: PluginCollisionError: multiple plugins register alert_sink/filedump: 'acme-alerts-fork' (acme_alerts_fork.sink:FileDumpAlertSink), 'acme-dlt-ops' (acme_dlt_ops.sink:FileDumpAlertSink). Pick a winner (distribution name or qualified object path) in .dlt/config.toml:

[dlt_ops.plugins.alert_sink]
filedump = "acme-alerts-fork"
```

The suggested block shows the syntax with one claimant filled in — put the winner *you* mean in `.dlt/config.toml` (a distribution name or a qualified object path both match):

```toml
[dlt_ops.plugins.alert_sink]
filedump = "acme-dlt-ops"
```

Loading the project config installs the pick into the registry, so `validate` passes again and `reconcile` emits through the winning sink. One caveat observed live: `plugins doctor` keeps reporting the collision even after the config entry, because doctor inspects the raw registry without loading any project's config — treat doctor as the collision *detector* and a green `validate` as proof the pick works. Uninstalling the fork removes the collision at the root, and doctor returns to `OK`.

## Testing your plugins

**For unit tests inside your plugin repository, do what the `dlt-ops` test suite does instead of installing fixture distributions:** overlay fake entry points on top of the installed metadata and reset the process registry between cases. `tests/test_alert_sinks.py` (the `extra_entry_points` fixture, plus `RecordingAlertSink` as a reference third-party sink) is the pattern to copy for sinks; `tests/test_assertions.py` uses the same overlay for assertion types; `tests/test_secrets.py` covers the claim-resolution semantics a backend must fit.

## Where next

- [Plugins](../concepts/plugins.md) — the six axes, loader policy, and enforcement tiers behind everything this guide did
- [Write a destination adapter](write-a-destination-adapter.md) — the remaining axis, end to end
- [Drift detection](drift-detection.md) — the reconciler workflow your alert sink plugs into
