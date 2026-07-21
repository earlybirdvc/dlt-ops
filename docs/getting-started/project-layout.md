---
description: The mandatory dlt-ops project layout — how filesystem discovery maps directories and file names to sources and config, the nine conventions validate enforces, and the project marker. Deviate and discovery refuses to find your source.
---

# Project layout

The layout is the contract: sources are discovered by scanning the filesystem, so where a file lives and what it is named is load-bearing. Read this page before writing your first source — it lists every convention the toolchain enforces and why each one exists.

**At a glance**

The mandatory shape — every command discovers sources by walking it; the numbered conventions are detailed below:

```text
<project root>/            # the marker: .dlt/config.toml with a [dlt_ops] table
├── .dlt/
│   ├── config.toml        # project marker + all dlt-ops config
│   └── secrets.toml       # dlt-native secrets, shared project-wide
└── <pipeline>/            # one directory per pipeline
    ├── source/<X>.py      # a @dlt.source(name="<X>") ↔ [sources.<X>]
    └── resource/<Y>.py    # shared @dlt.resource definitions (columns=<Model>)
```

## Why the layout is mandatory

**`dlt-ops` is strict and layout-mandatory by design: deviate from the layout and discovery refuses to find you — there is no flexible mode, no fallback to environment variables, no `--config` flag pointing somewhere else.** That rigidity is the value proposition, not a limitation to work around. Discovery by scanning means:

- **No registration code.** There is no `register_source(...)` call, no plugin file, no import-side-effect registry. A source exists because a correctly named module sits in a correctly named directory.
- **Enumerating sources never runs your code.** `pipeline list`, `pipeline resources`, and the orchestrator adapters use a pure AST scan (Phase 1) — they parse your files without importing them, so a scheduler can enumerate a project on every heartbeat without executing anything. [Discovery](../concepts/discovery.md) covers the two-phase model.
- **Tooling and review can rely on structure.** Every project laid out this way looks the same; nothing about "where do sources live here" has to be rediscovered per repository.

The naming conventions double as the config link: because the module stem, the source-function name, the decorator's `name=`, and the `[sources.<X>]` section all carry the same string, discovery can map a file to its configuration without importing anything.

## Terminology

**`dlt-ops` uses four layout nouns with fixed meanings — project, pipeline, source, and resource.**

- **project** — the directory tree rooted at the marker (`.dlt/config.toml` with a `[dlt_ops]` table). All CLI commands operate on one project.
- **pipeline** — a directory directly under the project root. The directory name is the pipeline name (`github_events/`). One pipeline directory can hold several sources.
- **source** — a `@dlt.source(name="<X>")`-decorated function in `<pipeline>/source/<X>.py`. The `name=` value is the source's identity, and `[sources.<X>]` is its config section.
- **resource** — a `@dlt.resource`-decorated function, either declared in the source's own module or shared via `<pipeline>/resource/`. Resource names must be unique within a pipeline directory.

## The annotated tree

**The numbers on each line map to the nine conventions in the table below.**

```text
<project root>/                    ← identified by the marker; every command walks up to it (or takes --root)
├── .dlt/                          ← dlt-native config dir, once per project — not per pipeline
│   ├── config.toml                ← the project marker: contains [dlt_ops] plus all dlt-ops config
│   └── secrets.toml               ← dlt-native secrets, shared by every pipeline in the project
├── github_events/                 ← ① one directory per pipeline, directly under the root (no leading . or _)
│   ├── source/                    ← ② exact singular name; discovery scans the modules in here
│   │   ├── github_events_api.py   ← ③ module stem = config section [sources.github_events_api]
│   │   │                            ④ defines github_events_api_source()
│   │   │                            ⑤ decorated @dlt.source(name="github_events_api")
│   │   └── github_events_full.py  ← a second source sharing the pipeline directory
│   ├── resource/                  ← ② exact singular name; shared @dlt.resource definitions + Pydantic models
│   │   └── events.py              ← ⑨ resources declare columns=<Model>; ⑧ names unique within the pipeline
│   └── data/                      ← anything else in a pipeline directory is yours; the scanner ignores it
└── web_analytics/                 ← the next pipeline
    └── ...
```

And the config side of the same chain, in `.dlt/config.toml`:

```toml
[dlt_ops]                    # the marker table; project-wide defaults live here
default_destination = "duckdb"

[sources.github_events_api]        # ⑥ one section per source; dlt-native source config goes here

[sources.github_events_api.dlt_ops]
schedule = "@hourly"               # ⑦ required on every source
```

The scanner reads `source/` for source functions and `resource/` for shared resource declarations; both scans are AST-only. A file that fails to parse is skipped with a warning and its siblings are unaffected.

## The nine conventions

**Roughly nine conventions per source, against vanilla dlt's three (`@dlt.source`, a function returning resources, `pipeline.run()`) — that is the adoption math you are signing up for.** In exchange you get discovery, validation, scheduling, checkpoints, backfill, cleanup, a runs ledger, drift detection, and assertions. The README's ["opinions you're signing up for"](https://github.com/earlybirdvc/dlt-ops#the-opinions-youre-signing-up-for) table is the canonical short form; here is each rule, what it enforces, and why:

| # | Convention | What it enforces | Why |
|---|---|---|---|
| 1 | One directory per pipeline directly under the project root, no leading `.` or `_` | The scan boundary — discovery walks exactly one level of directories | Dot/underscore prefixes are the standard "not mine" escape hatch (`.venv`, `_scratch`) |
| 2 | Source modules in `<pipeline>/source/`, shared resources in `<pipeline>/resource/` (exact singular names) | Fixed scan targets for the discovery scanner | The scanner never has to guess which subdirectories contain pipeline code |
| 3 | Module stem equals the config section: `source/<X>.py` ↔ `[sources.<X>]` | A file maps to its config without any import | dlt's own `dlt.secrets.value` injection also resolves per config section, so the stem keeps secrets resolution aligned |
| 4 | The source function is named `<X>_source` | The scan's heuristic for which top-level function is *the* source | No registration call is needed |
| 5 | The decorator names the section explicitly: `@dlt.source(name="<X>")` | The `name=` value is the source identity dlt uses | Making it explicit keeps the identity greppable and independent of function-name derivation |
| 6 | Every source has a `[sources.<X>]` section in `.dlt/config.toml` | The config section dlt itself requires | This is dlt-native territory `dlt-ops` builds on |
| 7 | Every source declares a `schedule` under `[sources.<X>.dlt_ops]` | A schedule the runtime and orchestrator adapters key on | A source without a schedule cannot be turned into a DAG |
| 8 | Resource names are unique within a pipeline directory | Unambiguous table addressing for `clean`, `status`, checkpoints, and backfill | Those verbs address destination tables by resource name; a collision would make them ambiguous |
| 9 | Every `@dlt.resource` declares `columns=` as a Pydantic model | The model is the schema — typed destination columns at load time instead of inference | The reconciler diffs the live schema against it |

Two more opinions are enforced without you writing anything:

- **Schema contracts.** A resource that declares no `schema_contract` gets the canonical freeze contract (`{"tables": "evolve", "columns": "freeze", "data_type": "freeze"}`) auto-applied at runtime. Evolving contracts require an explicit, justified opt-in in config (`schema_contract_evolve_reason`).
- **Import safety.** `validate` imports each source module in a sandbox and fails on network I/O or disk writes at import time (disk *reads* are permitted — loading a schema file at module level is fine). This catches the orchestrator-parse foot-gun — a module-level `requests.get(...)` that fires on every scheduler heartbeat — before deploy.

## Overrides — and their limit

**Every `validate` rule can be switched off per project (`[dlt_ops.rules]`) or exempted per source with a mandatory written reason (`[sources.<X>.dlt_ops.rule_exemptions]`) — the [rules reference](../configuration/rules.md) covers the knobs.** Know the limit: the switches silence findings, they do not teach discovery a different layout. A source module outside `<pipeline>/source/`, or without a `@dlt.source`-decorated function, is not found no matter which rules you disable — conventions 1, 2, and the decorator itself are structural, not advisory.

## The project marker

**A directory is a `dlt-ops` project iff both hold:**

1. `.dlt/config.toml` exists, and
2. it contains a top-level `[dlt_ops]` table.

Every command walks up from the current directory until a directory qualifies, or takes `--root` explicitly (an explicit root is only verified, never widened). If no directory qualifies, commands fail fast with a `dlt-ops init` hint. A `config.toml` that exists but is broken TOML is a loud error, not a "keep walking up" — a broken marker must never silently widen the root search.

Two deliberate non-features:

- **No separate marker file.** There is no `dlt-ops.toml`: dlt already requires `.dlt/config.toml`, and all dlt-ops config lives namespaced inside it (`[dlt_ops]`, `[sources.<X>.dlt_ops]`) — one config file, no duplicated surface. [Configuration](../configuration/index.md) covers the namespace model.
- **No per-pipeline `.dlt/`.** The `.dlt/` directory sits once at the project root, because dlt itself wants a single project dir and shared secrets (warehouse credentials, project IDs) should live in one place instead of being duplicated per pipeline.

## Where next

- [Quickstart](quickstart.md) — see the conventions in a running project
- [Discovery](../concepts/discovery.md) — what the two scan phases do and never do
- [Configuration](../configuration/index.md) — the `[dlt_ops]` namespace and its precedence ladder
