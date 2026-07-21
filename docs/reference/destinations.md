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

Registration is what the check consults, and it does not care where the registration came from: an entry point in an installed distribution, a runtime `dlt_ops.register`, and `register_derived_adapter` all land in the same registry and produce the same full tier. What full tier *means* for each is not identical — see [derived is not the same as tested](#derived-is-not-the-same-as-tested).

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

**Three ways to full tier — install a first-party adapter, opt into a capability-derived one, or author one for your engine.**

1. **Use a first-party adapter.** Install one of `dlt-ops[duckdb]`, `[postgres]`, or `[bigquery]` and point `default_destination` (or a per-source `destination`) at `duckdb` / `postgres` / `bigquery`. DuckDB is the credential-free dev-loop destination.
2. **Register a capability-derived adapter.** dlt publishes, per destination, most of what an adapter needs (see [capabilities come from dlt](../concepts/destinations-and-tiers.md#capabilities-come-from-dlt)), so for any destination that declares a `sqlglot_dialect` a whole adapter can be built from that alone — no code to write. It is opt-in, at runtime, one call: `register_derived_adapter("snowflake")`. Read the next section before you rely on one.
3. **Author an adapter.** Any destination reaches full tier once a `DestinationAdapter` is registered for its engine name under the `dlt_ops.destination` entry-point group. The Protocol (canonical DuckDB-dialect SQL in; transpile-and-bind at the boundary) and a worked example are in the [destination-adapter guide](../guides/write-a-destination-adapter.md). Hand-writing is what you do when a *driver* fact differs from its dialect's convention — a paramstyle, NULL binding, an `information_schema` that is scoped differently.

### Derived is not the same as tested

**Derivation proves the SQL will be *shaped* for the right dialect. It proves nothing about whether anyone has run it there.** Those are two separate claims, and this package keeps them separate on purpose.

**What CI actually verifies.** `CI_VERIFIED_DESTINATIONS` is `("duckdb", "postgres")` — the two destinations whose adapter runs against a live instance on every commit. BigQuery has a live lane too, but it is credential-gated and therefore non-blocking. That is the honest floor under any "supported" claim here.

**What derivation cannot see.** Whether the driver binds parameters the way its dialect writes them, whether it can bind a typed `NULL`, whether the destination's `information_schema` has the standard shape and scope, who owns schema creation — and whether the destination's SQL client writes anywhere durable. Registering a derived adapter logs a `WARNING` naming the destination as derived and unverified, for exactly that reason. Verify the adapter-gated features against your destination before relying on them.

**`filesystem` is derivable and must not be registered.** dlt gives the filesystem destination a SQL client, so a dialect derives cleanly (`duckdb`) — but that client is an ephemeral in-memory DuckDB that creates views over the bucket's files. SQL through it reads the bucket; it does not write to it. A ledger row, checkpoint, or backfill claim inserted that way lands in a database that disappears when the process exits. The [object-store note above](#object-store-destinations) stands unchanged: object stores are core tier by construction, and derivability does not alter that.

**Which destinations are derivable.** `derivable_destinations()` enumerates them for the dlt version you have installed — a diagnostic surface, showing what you *could* opt into, not what is supported. Against the dlt in this project's lock file it returns fourteen: `athena`, `bigquery`, `databricks`, `dremio`, `duckdb`, `ducklake`, `fabric`, `filesystem`, `motherduck`, `mssql`, `postgres`, `redshift`, `snowflake`, `synapse`. Three of those are the first-party adapters; one is `filesystem`, above. The remaining ten are derivable and unverified — usable, at your own verification.

**Two destinations dlt ships are not derivable**, and `register_derived_adapter` raises `UnderivableDestinationError` rather than guessing a dialect that would produce SQL which parses and silently means something else:

| Destination | Why it cannot be derived |
|---|---|
| `clickhouse` | Its dialect is declared, but sqlglot's ClickHouse writer renders a positional placeholder as `{?: }` — a form that carries structure inline and is not usable as a substituted token. |
| `sqlalchemy` | Its dlt capabilities declare no `sqlglot_dialect`, so there is no transpile target to read. It fronts many engines, and which one is a runtime fact rather than a published capability. |

Both reach full tier the normal way, by hand-writing an adapter that declares the missing fact.

**Nothing derives automatically.** Registration is always a call you make, so "dlt publishes enough to derive this" is never silently read as "this package supports it". `is_capability_derived(adapter)` answers, for a resolved adapter, which kind you are running.

## Demanding full tier: `require_destination_adapter`

**Degrade-by-default is deliberate — a scheduled run should not die because an observability table has nowhere to live.** Teams that operate *on* the adapter-backed surfaces (the ledger, checkpoints) can invert that default:

```toml
[dlt_ops]
require_destination_adapter = true
```

With the knob set, a resolved destination with no registered adapter is a hard **preflight failure** on every `run`/`backfill` and an **error** from the `destination_capability` validate rule — absence becomes fatal instead of core-mode degradation. Default is `false`; only the literal `true` engages it. See the [config reference](../configuration/reference.md#project-level-dlt_ops) and the [`destination_capability` rule](../configuration/rules.md#destination_capability).

## Future work

**dlt exposes read-only SQL over a filesystem via DuckDB views, which could someday back a read-only adapter split** that gives `status` and `reconcile` on the object-store destinations above. That is exactly the capability described in [derived is not the same as tested](#derived-is-not-the-same-as-tested): the SQL client reads the bucket well and writes nowhere durable, so the useful split is read-only verbs, not the full six. No protocol change ships now.
