---
description: Install dlt-ops and choose an extra — the base install runs core tier on any dlt destination; [duckdb]/[postgres]/[bigquery] register the first-party adapters that unlock full tier; other destinations and object stores stay core; [airflow] and [sentry] add integrations. Extras pull dependencies only.
---

# Installation

How to install `dlt-ops`, which extra to pick, and what each choice does to the feature set. Extras only pull in dependencies — destination drivers, the Airflow adapter, the Sentry SDK; all first-party code ships in the one distribution.

**At a glance**

| Install | Adds | Tier |
|---|---|---|
| `pip install dlt-ops` (base, no extra) | CLI + plugin registry: discovery, `validate`, `run`, and scheduling metadata on any destination dlt can resolve | core |
| `[duckdb]` / `[postgres]` / `[bigquery]` | the matching first-party `DestinationAdapter` | **full** |
| `[snowflake]` / `[databricks]` | the dlt destination driver — no adapter registered | core |
| `[filesystem]` / `[s3]` / `[gs]` / `[az]` | object-store drivers (a local `file://` bucket needs none) | core |
| `[airflow]` | the Airflow adapter: DAG factory, Airflow Variable secret backend, Airflow-specific `validate` rules | — |
| `[sentry]` | the Sentry alert sink for the reconciler | — |

## Base install

**The base install ships the CLI and the plugin registry — no destination extra required.** Discovery, `pipeline validate`, `pipeline run`, and scheduling metadata work on any destination dlt can resolve; a local `filesystem` bucket, for example, needs no extra at all.

=== "pip"

    ```bash
    pip install dlt-ops
    ```

=== "uv"

    ```bash
    uv add dlt-ops
    ```

## Destination extras and capability tiers

**Which destination you point a source at decides its capability tier, and the tier decides which features work there.** Full tier means a `DestinationAdapter` is registered for the destination; it unlocks the [six adapter-gated features](../reference/destinations.md) that speak SQL to the destination directly. Core tier runs the pipeline and everything else; the gated features refuse loudly instead of degrading silently.

The tier is per destination, not per install: one project can load into full-tier DuckDB and core-tier Snowflake side by side.

Object stores are core tier by construction, not "until an adapter ships": they have no SQL engine, so nothing can back the adapter-routed features. Snowflake and Databricks are core tier until a `DestinationAdapter` exists for them — first-party or third-party via entry points.

[Destinations and tiers](../concepts/destinations-and-tiers.md) explains the model; the [destinations reference](../reference/destinations.md) has the full feature × tier matrix.

For the quickstart and any local development loop, DuckDB is the full-tier destination with zero credentials:

=== "pip"

    ```bash
    pip install "dlt-ops[duckdb]"
    ```

=== "uv"

    ```bash
    uv add "dlt-ops[duckdb]"
    ```

## Python and dlt versions

**Python 3.11, 3.12, and 3.13 are supported and CI-tested** (`requires-python = ">=3.11"`).

**The dlt dependency is a floor, never a cap: `dlt>=1.27`.** You own your project's dlt version — the package metadata will not force a resolver downgrade or block a dlt upgrade.

Verification is a separate question from allowance: [the compatibility matrix](../reference/compatibility.md) records which dlt minor × destination combinations CI proves, and every feature except one runs on any dlt at or above the floor regardless.

The exception is remote `clean` — it rewrites dlt-internal state tables whose layout is reverse-engineered per minor, so on an unverified dlt minor it refuses with a `CleanupUnsupportedError` instead of guessing. A newer, not-yet-verified dlt minor costs you exactly that one feature until the matrix catches up.

## Check the install

**Confirm the CLI resolves and lists its command groups.**

```bash
dlt-ops --help
```

```text
Usage: dlt-ops [OPTIONS] COMMAND [ARGS]...

  dlt-ops — opinionated project layout and toolchain for dlt pipelines.

Options:
  -r, --root DIRECTORY  Project root (holds .dlt/config.toml). Default: walk
                        up from cwd.
  --help                Show this message and exit.

Commands:
  checkpoints  Manage checkpoints for dlt pipelines.
  init         Scaffold a dlt-ops project at ROOT (default: current...
  pipeline     Manage dlt pipelines - discover, run, validate.
  plugins      Inspect the plugin registry.
```

## Where next

- [Quickstart](quickstart.md) — scaffold a project and tour every operational feature offline
- [Project layout](project-layout.md) — the conventions the toolchain enforces
