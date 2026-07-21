---
description: The canonical destination capability-tier reference — the feature × tier matrix (core vs full), core-tier behavior verb by verb, object-store notes, and how to reach full tier by registering a DestinationAdapter.
---

# Destinations reference

`dlt-ops` runs on **any destination dlt itself can resolve**. The core run loop — extract, pre-load assertions (`fail`/`warn`), schema contracts, load-timestamp stamping, run-window bounds injection, normalize, load — is destination-agnostic and never depends on a `dlt-ops` plugin. A subset of features speaks SQL to the destination directly — the adapter-gated features enumerated in the matrix below — and those require a `DestinationAdapter` registered for the destination. That split is the **capability-tier** model. For the reasoning behind it — how tiers resolve and the canonical-SQL boundary that keeps one adapter cheap — see [destinations and capability tiers](../concepts/destinations-and-tiers.md); this page is the reference matrix and per-destination notes.

## The two tiers

**Two tiers, resolved per destination: core (any destination dlt can resolve) and full (a registered `DestinationAdapter`).**

- **core** — every destination dlt can resolve (`Destination.from_reference` succeeds: shorthand like `filesystem`, `snowflake`, `duckdb`, or a full `dlt.destinations.<name>` module path). Guaranteed surface: discovery, `validate`, `run` (extract, `fail`/`warn` assertions, schema contracts, load-timestamp stamping, run-window bounds injection, normalize, load), run-trace persistence, scheduling metadata, the Airflow DAG factory, `list`/`resources`, and `clean --local-only`.
- **full** — a `DestinationAdapter` is registered for the destination's engine name. Adds the SQL-observed features in the matrix below.

### How the tier resolves

**Tier is resolved per destination name — at run time and at `validate` time — by a registry-membership check on the destination's engine name** (`Destination.to_name(...)`, the same normalization every adapter lookup uses, so `duckdb` and `dlt.destinations.duckdb` resolve to one entry). The adapter is never loaded to answer the question; only its registration is consulted.

First-party adapters ship for **`duckdb`**, **`postgres`**, and **`bigquery`** as entry points, so those engine names always resolve to full tier — even when the destination's own SDK extra is not installed. A missing SDK surfaces later, at client construction, with dlt's own error; it does not change the tier.

## Feature × tier matrix

**The feature × tier matrix: every guaranteed surface, and which tier provides it (✓).**

| Feature | core | full |
|---|---|---|
| discovery, `validate` | ✓ | ✓ |
| `run` — extract → assertions (`fail`/`warn`) → normalize → load | ✓ | ✓ |
| schema contracts, load-timestamp stamping, bounds injection | ✓ | ✓ |
| run-trace persistence | ✓ | ✓ |
| scheduling metadata, Airflow DAG factory | ✓ | ✓ |
| `list`, `resources` | ✓ | ✓ |
| `clean --local-only` | ✓ | ✓ |
| runs ledger and status | — | ✓ |
| checkpoints (`@with_checkpoints`) | — | ✓ |
| backfill (chunk state in `_dlt_backfills`) | — | ✓ |
| clean (remote) | — | ✓ |
| reconcile | — | ✓ |
| assertion quarantine | — | ✓ |

The six full-tier rows are the canonical adapter-gated feature list; every message that names them renders from one constant, so the docs and the runtime can't drift.

## Core tier, verb by verb

**On a core-tier destination the run loop is unchanged; the adapter-gated features degrade loudly — the package never silently drops a feature.** The [failure-semantics contract](../concepts/failure-semantics.md) governs the split: observability goes quiet, a gate the config demands fails hard.

| Surface | Core-tier behavior | Message / state |
|---|---|---|
| `run` | Proceeds normally; the run loop is unchanged. | One `WARNING` at run start names the destination, "core mode", and the darkened features, then adds `extract/load, fail/warn assertions, and trace persistence run normally`. The configuration block prints `Capabilities: core (no adapter: … unavailable)`. |
| Runs ledger | `write_start` and `write_end` each skip — the ledger has nowhere to live, and nothing is broken. Not an `ERROR`; that is reserved for a real write failure at full tier. | One `INFO` line per write: `runs ledger skipped: destination 'X' has no DestinationAdapter (core mode)`. |
| `status` | Reports the source's ledger as a fourth state — `unsupported`, distinct from `ok`, `missing` (never ran), and `unreadable` (an outage). A capability fact, not a fault. | Text: `! ledger unsupported: destination 'X' has no DestinationAdapter (core mode)`; JSON `"ledger": "unsupported"`. |
| `@with_checkpoints`, assertion `quarantine`, `backfill`, `require_destination_adapter = true` | Refused at preflight (and flagged by `validate`) before any extract — a gate the config explicitly demands cannot silently downgrade. | `DestinationCapabilityError` naming the engaged feature. |
| Remote `clean`, `reconcile` | Refused; `clean --local-only` (which never resolves the destination) keeps working. | A capability-specific message. |

A typo'd or otherwise unresolvable destination is a different failure: dlt cannot resolve it at all, so it fails the typo guard (`UnknownDestinationError`) before the tier is ever considered — core tier is only for destinations dlt *can* reach.

## Object-store destinations

**`filesystem`, S3, GCS, and Azure Blob are core-tier destinations by construction: `run` and scheduling work, the adapter-routed features do not — an object store has no SQL engine for a ledger to live in or a reconcile query to run against.** That is a permanent property of the destination, not "until an adapter ships".

Point `default_destination` (or a per-source `destination`) at `filesystem` and give dlt a `bucket_url` its own way (`[destination.filesystem]` in `.dlt/config.toml`, or `DESTINATION__FILESYSTEM__BUCKET_URL`). A local `file://` bucket needs no extra beyond base `dlt-ops`; a remote bucket needs the matching install extra so dlt can reach the backend — `dlt-ops[s3]`, `[gs]`, `[az]`, or `[filesystem]` for the generic filesystem destination support.

## Reaching full tier

**Two ways to full tier — install a first-party adapter, or author one for your engine.**

1. **Use a first-party adapter.** Install one of `dlt-ops[duckdb]`, `[postgres]`, or `[bigquery]` and point `default_destination` (or a per-source `destination`) at `duckdb` / `postgres` / `bigquery`. DuckDB is the credential-free dev-loop destination.
2. **Author an adapter.** Any destination reaches full tier once a `DestinationAdapter` is registered for its engine name under the `dlt_ops.destination` entry-point group. The Protocol (canonical DuckDB-dialect SQL in; transpile-and-bind at the boundary) and a worked example are in the [destination-adapter guide](../guides/write-a-destination-adapter.md).

## Demanding full tier: `require_destination_adapter`

**Degrade-by-default is deliberate — a scheduled run should not die because an observability table has nowhere to live.** Teams that operate *on* the adapter-backed surfaces (the ledger, checkpoints) can invert that default:

```toml
[dlt_ops]
require_destination_adapter = true
```

With the knob set, a resolved destination with no registered adapter is a hard **preflight failure** on every `run`/`backfill` and an **error** from the `destination_capability` validate rule — absence becomes fatal instead of core-mode degradation. Default is `false`; only the literal `true` engages it. See the [config reference](../configuration/reference.md#project-level-dlt_ops) and the [`destination_capability` rule](../configuration/rules.md#destination_capability).

## Future work

**dlt can expose read-only SQL over a filesystem via DuckDB views, which could someday back a read-only adapter split** that gives `status` and `reconcile` on the object-store destinations above; no protocol change ships now.
