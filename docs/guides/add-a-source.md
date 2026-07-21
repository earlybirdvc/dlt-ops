---
description: Task guide — build a dlt source by hand from an empty dlt-ops project to a green validate, a real run, and a runs-ledger entry; scaffold, write the source module and Pydantic model, declare the config section and schedule, then read the naming-chain refusal.
---

# Add a source

This guide takes you from an empty project to a green `validate`, a real run, and a ledger entry — writing a source by hand instead of using the scaffolded example. Read the [quickstart](../getting-started/quickstart.md) first if you have never run `dlt-ops` at all; read [project layout](../getting-started/project-layout.md) for why each convention below exists.

**Prerequisites**

- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md).
- Runs fully offline, no credentials. DuckDB is full tier, so the run records to the `_dlt_ops_runs` ledger that `status` reads back in step 7.

**Steps at a glance**

1. [Scaffold an empty project](#1-scaffold-an-empty-project)
2. [Write the source module](#2-write-the-source-module)
3. [Validate — and read the refusal](#3-validate-and-read-the-refusal)
4. [Declare the source in config](#4-declare-the-source-in-config)
5. [Confirm discovery sees it](#5-confirm-discovery-sees-it)
6. [Run it](#6-run-it)
7. [Verify the outcome](#7-verify-the-outcome)

## 1. Scaffold an empty project

**`init` without `--example` creates the project marker and one empty pipeline directory:**

```bash
dlt-ops init shop
cd shop
```

```text
✓ Initialized dlt-ops project at /tmp/shop

  .dlt/config.toml
  .dlt/secrets.toml
  my_pipeline/source/
  my_pipeline/resource/
  my_pipeline/source/.gitkeep
  my_pipeline/resource/.gitkeep

Next steps:
  1. Write a source: my_pipeline/source/<section>.py (module stem = [sources.<section>] in .dlt/config.toml)
  2. Validate the project: dlt-ops pipeline validate
```

The scaffolded `.dlt/config.toml` already sets `default_destination = "duckdb"`, so the project runs locally with no credentials. `my_pipeline` is the starter pipeline directory (`--pipeline <name>` picks another name); `source/` and `resource/` are the two subdirectories discovery scans — nothing else in a pipeline directory is touched.

## 2. Write the source module

**One module carries the whole naming chain** — the module stem `orders` is the config section, the source function is `orders_source`, and the decorator names the section explicitly. Create `my_pipeline/source/orders.py`; that chain is how [discovery](../concepts/discovery.md) maps files to config without importing anything:

```python
"""Orders source: pages order records out of the shop backend.

Naming chain (enforced by `validate`): module stem `orders` = config section
[sources.orders] = @dlt.source(name="orders") = function name orders_source.
"""

from datetime import UTC, datetime

import dlt
import pydantic


class Order(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    customer_email: str | None  # nullable: guest checkouts carry no account
    total_cents: int
    placed_at: datetime


# Stand-in for a paginated API client. A real source would page through HTTP
# responses here; keep module level free of network I/O — `validate` fails
# imports that touch the network (the orchestrator-parse foot-gun).
_ROWS = [
    {"id": 101, "customer_email": "ada@example.com", "total_cents": 4200, "placed_at": datetime(2026, 2, 1, 9, 30, tzinfo=UTC)},
    {"id": 102, "customer_email": None, "total_cents": 1350, "placed_at": datetime(2026, 2, 1, 11, 5, tzinfo=UTC)},
    {"id": 103, "customer_email": "sam@example.com", "total_cents": 899, "placed_at": datetime(2026, 2, 2, 8, 15, tzinfo=UTC)},
    {"id": 104, "customer_email": "ada@example.com", "total_cents": 15600, "placed_at": datetime(2026, 2, 3, 17, 40, tzinfo=UTC)},
]


def _pages(page_size: int = 2):
    for start in range(0, len(_ROWS), page_size):
        yield _ROWS[start : start + page_size]


@dlt.resource(name="orders", columns=Order, primary_key="id", write_disposition="replace")
def orders():
    for page in _pages():
        yield page


@dlt.source(name="orders")
def orders_source():
    return orders
```

**Three declarations do the structural work:**

- **The Pydantic model is the schema.** `columns=Order` gives dlt typed destination columns at load time instead of type inference, and the [reconciler](../concepts/reconciler.md) later diffs the live schema against it — every resource must declare one.
- **`extra="forbid"` is what makes the model a contract.** dlt reads it as `schema_contract` `columns: "freeze"`, so a field the API starts sending that `Order` does not declare fails the run instead of being dropped in silence. Leave it off and Pydantic's default applies — dlt derives `columns: "discard_value"` and you lose the column without being told. The [`pydantic_model_forbids_extra`](../configuration/rules.md#pydantic_model_forbids_extra) rule fails `validate` on models that omit it.
- **The resource carries no `schema_contract` kwarg**, and needs none: the contract dlt derives from the model above already is the canonical freeze contract (`{"tables": "evolve", "columns": "freeze", "data_type": "freeze"}`). A resource whose `columns=` is a plain dict, or that declares no `columns=` at all, gets that literal applied by the runtime instead.
- **The resource lives in the source's own module** because nothing else shares it — a resource used by several sources moves to `my_pipeline/resource/` instead.

## 3. Validate — and read the refusal

**The module alone is not a source yet.** Run validate:

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✗ 2 error(s):
  [orders] config_section: Missing config section [sources.orders]
  [orders] schedule: Missing 'schedule' field in [sources.orders.dlt_ops]
```

Discovery found the module (the naming chain parsed), but two of the nine conventions are config-side: every source needs a `[sources.<X>]` section (dlt's own requirement — secrets and source config resolve per section) and a `schedule` under `[sources.<X>.dlt_ops]` (the `config_section_required` and `schedule_required` rules). The command exits 1 — this is Tier 1 of the [enforcement model](../concepts/validation.md), the same check CI runs.

## 4. Declare the source in config

**Append the source's section and its `dlt_ops` block to `.dlt/config.toml`:**

```toml
[sources.orders]
# dlt-native source config (API base URL, auth key names) would go here.

[sources.orders.dlt_ops]
schedule = "@daily"
dataset = "shop_raw"
```

`schedule` takes one of `@hourly`, `@2hourly`, `@daily`, `@weekly`, `@monthly`, `@manual` — it is metadata the orchestrator adapters compile into DAGs, not a scheduler inside `dlt-ops` ([scheduling](../concepts/scheduling-and-orchestration.md)). `dataset` scopes this source's tables; without it the source needs a project-wide `[dlt_ops].default_dataset`. Validate again:

```text
Validating sources

✓ All sources validated successfully
```

## 5. Confirm discovery sees it

**Both orientation commands run the pure AST scan** — they never import your code, which is what makes them safe on any scheduler heartbeat:

```bash
dlt-ops pipeline list
dlt-ops pipeline resources -s orders
```

```text
Found 1 source(s)

Name                           Pipeline        Schedule   Resources
----------------------------------------------------------------------
orders                         my_pipeline     @daily     1
```

```text
Source: orders
Pipeline: my_pipeline
Function: orders_source
Config: [sources.orders]
Schedule: @daily

Resources (1):
  • orders
```

## 6. Run it

**Run the source you just declared; the resolved configuration prints before anything executes** — check the destination, dataset, and capability tier match what you configured:

```bash
dlt-ops pipeline run -s orders -y
```

```text
Pipeline Configuration
----------------------------------------
  Source: orders
  Function: orders_source
  Resources: all (1 total)
  Destination: duckdb
  Dataset: shop_raw (from .dlt/config.toml)
  Capabilities: full

Starting pipeline...
...
1 load package(s) were loaded to destination duckdb and into dataset shop_raw
The duckdb destination used duckdb:////tmp/shop/orders_pipeline.duckdb location to store data
Load package 1784231741.896383 is LOADED and contains no failed jobs
```

The underlying dlt pipeline is named `<source>_pipeline`, and dlt's DuckDB destination writes `orders_pipeline.duckdb` into the directory you run from — the project root, if you follow this guide. `Capabilities: full` means DuckDB has a registered `DestinationAdapter`, so the ledger read in the next step works; a core-tier destination would run the same pipeline but report no ledger ([destinations and tiers](../concepts/destinations-and-tiers.md)).

## 7. Verify the outcome

**The run wrote its outcome to the `_dlt_ops_runs` ledger in the destination itself; `status` reads it back:**

```bash
dlt-ops pipeline status
```

```text
Source: orders
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-16 19:55:41  2026-07-16 19:55:42  4         cli        -               c5a28ec49782
```

And the four typed rows are in the destination, with columns derived from the Pydantic model:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("orders_pipeline.duckdb", read_only=True)
con.sql("SET TimeZone = 'UTC'")
print(con.sql("SELECT id, customer_email, total_cents, placed_at FROM shop_raw.orders ORDER BY id"))
PY
```

```text
┌───────┬─────────────────┬─────────────┬──────────────────────────┐
│  id   │ customer_email  │ total_cents │        placed_at         │
│ int64 │     varchar     │    int64    │ timestamp with time zone │
├───────┼─────────────────┼─────────────┼──────────────────────────┤
│   101 │ ada@example.com │        4200 │ 2026-02-01 09:30:00+00   │
│   102 │ NULL            │        1350 │ 2026-02-01 11:05:00+00   │
│   103 │ sam@example.com │         899 │ 2026-02-02 08:15:00+00   │
│   104 │ ada@example.com │       15600 │ 2026-02-03 17:40:00+00   │
└───────┴─────────────────┴─────────────┴──────────────────────────┘
```

From here the source grows in place: swap `_ROWS` and `_pages` for a real client, add an incremental cursor when the API supports one, and put a data-quality gate on the resource ([assertions guide](assertions.md)).

## Troubleshooting: the naming chain breaks as a set

**The decorator's `name=` is the source's identity** — change one link of the chain and every config keyed on the old name goes dark at once. Renaming just the decorator to `@dlt.source(name="shop_orders")` produces three errors, not one:

```text
✗ 3 error(s):
  [shop_orders] config_section: Missing config section [sources.shop_orders]
  [shop_orders] schedule: Missing 'schedule' field in [sources.shop_orders.dlt_ops]
  [shop_orders] module_stem: Module filename mismatch: 'orders.py' but config section is 'shop_orders'. Rename module to 'shop_orders.py' so dlt.secrets.value resolves correctly in resources.
```

Read the bracket: `validate` now reports the source under its new identity `shop_orders` and measures everything else against it — the existing `[sources.orders]` section, the module stem, the schedule all appear missing or mismatched. To rename a source, move all four links together: module file, function name, decorator `name=`, and config section.

## Where next

- [Gate a resource with assertions](assertions.md) — put data-quality checks on the source you just added
- [Project layout](../getting-started/project-layout.md) — all nine conventions, each with its reason
- [Configuration reference](../configuration/reference.md) — every `[dlt_ops]` and `[sources.<X>.dlt_ops]` key
