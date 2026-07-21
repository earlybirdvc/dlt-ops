---
description: How to change the dlt-ops package itself — dev setup on uv, the conventions that gate a merge, the CI guard scripts, and previewing the docs. To extend dlt-ops from your own distribution, see the plugin guides instead.
---

# Contributing

This page is for changing the `dlt-ops` package itself: the dev setup, the conventions that gate a merge, the guard scripts that run in CI, and how to preview the docs. If you are not changing the package but extending it from your own distribution — a destination adapter, alert sink, assertion type, validator, or secret backend — you want the [plugin guides](#extending-dlt-ops-from-your-own-package) instead.

## Dev setup

**`dlt-ops` develops on [uv](https://docs.astral.sh/uv/) and targets Python 3.11+.** One command installs the package plus the dev dependency-group (pytest, ruff, pyrefly, and the DuckDB test lane) at the exact versions in the lockfile:

```bash
uv sync --locked
```

After that initial sync, run every tool through `uv run --no-sync`. The `--no-sync` flag runs against the environment you already have instead of first reconciling it against the lockfile — which matters because the CI test matrix (and the integration harness) hand-install a specific dlt version with `uv pip install`, and a plain `uv run` would silently revert it. Running with `--no-sync` locally means you run exactly what CI runs:

```bash
uv run --no-sync pytest              # the credential-free suite (default lane)
uv run --no-sync ruff check          # lint (line-length 120, target py311)
uv run --no-sync ruff format --check # formatting
uv run --no-sync pyrefly check       # type check
```

The docs toolchain (mkdocs-material, mkdocstrings, mkdocs-click) lives in a separate `docs` dependency-group, kept out of the default dev environment. Sync it when you work on documentation:

```bash
uv sync --locked --group docs
```

## Conventions that gate merges

**A merge must satisfy the conventions below; CI enforces the ones that can be checked mechanically.**

### Squash-merge and conventional PR titles

**Every PR is squash-merged, so the PR title — not the individual commits on your branch — becomes the single commit message on `main`.** That title must parse as a [Conventional Commit](https://www.conventionalcommits.org) (`feat:`, `fix:`, `docs:`, and so on). Two things enforce and consume it:

- The `pr-title` CI check rejects a title that does not parse.
- [python-semantic-release](releases.md) reads those squash-commit messages on `main` to compute the next version and write the changelog. `feat:` bumps the minor, `fix:`/`perf:` the patch; a `docs:`/`refactor:`/`chore:` title ships no release. The type is the release lever, not decoration — get it right.

Body bullets and branch commits do not count: the release config parses the squash title alone (`parse_squash_commits = false`), so nothing in a PR body or a mid-branch commit can add a bump or a changelog line. The full mapping is in [releases](releases.md).

### One SQL boundary

**All destination SQL is written once, in the canonical DuckDB dialect with positional `?` placeholders, and routed through a `DestinationAdapter`.** Package code never speaks raw dialect SQL and never builds identifiers by string concatenation. sqlglot transpiles the canonical form to each destination at the adapter boundary — that is what lets N destinations share M features without N×M hand-written SQL. The [destinations concept](../concepts/destinations-and-tiers.md) covers the design; the `sql_boundary_guard.sh` script below enforces it mechanically.

### Floors-only dependencies

**Every dependency declares a floor and never an upper bound** — users own their dlt (and extras) resolution, and this package's metadata will not force a resolver downgrade or block an upgrade. The dlt floor is the oldest CI-verified minor; see [compatibility](../reference/compatibility.md).

### Long-line Markdown

**Documentation and Markdown are written with one paragraph per source line — no hard-wrapping prose at a fixed column.** Wrap only inside code blocks and tables.

### Public API discipline

**The public surface is what `dlt_ops` exports (listed in its `__all__`) plus explicitly-public subpackages like `dlt_ops.airflow`; everything else is internal, importable but with no stability promise.** New internal modules are underscore-prefixed. Do not grow the public surface casually — `tests/test_api.py` locks it, and [versioning](../reference/versioning.md) defines what that promise means.

## Guard script

**The SQL-boundary guard runs in the `guards` CI job and must stay green — run it locally before pushing.**

```bash
bash ci/sql_boundary_guard.sh
```

```text
SQL-boundary guard: OK (0 allowlisted files still pending port)
```

`sql_boundary_guard.sh` fails when package code outside `dlt_ops/destinations/` acquires a raw dlt `sql_client` or calls `.execute_sql`/`.execute_query` on anything that is not an adapter — the mechanical half of the one-SQL-boundary rule above.

It uses a shrinking per-file allowlist (`ci/sql-boundary-allow.txt`): an entry tolerates existing hits in one file until a named ticket ports them, and an allowlisted file with no hits left fails the guard as a stale entry — so the list only ever shrinks. It is empty today, which is why the guard reports `OK (0 ...)`.

## Preview the docs

**Serve the docs locally with live reload to preview changes as you write.**

```bash
uv sync --locked --group docs
uv run --no-sync mkdocs serve
```

That builds the site and serves it at `http://127.0.0.1:8000`. To reproduce the CI gate instead — a strict build that fails on a broken cross-link or a missing nav entry — run `uv run --no-sync mkdocs build --strict`. mkdocstrings imports the package during the build, so a code change can break the docs build too; that is why the Docs CI job has no path filter and runs on every PR.

## PR expectations

**Every PR keeps CI green and ships the tests and doc updates its changes require.**

- Keep the credential-free lane, ruff (check + format), pyrefly, the guard scripts, and the strict docs build green — CI runs all of them on every PR.
- Behavior changes come with tests. A change to the config or rule surface also updates the [configuration reference](../configuration/reference.md) or [rules reference](../configuration/rules.md).
- Do not hand-edit `CHANGELOG.md` — it is machine-owned. python-semantic-release writes it from your PR title at release time.
- Write the PR title as the conventional-commit summary you want to land on `main`.

## Extending dlt-ops from your own package

**If you are building against `dlt-ops` rather than changing it, the extension points are entry-point plugins with their own guides.** [Write a destination adapter](../guides/write-a-destination-adapter.md) for the destination axis, and [write plugins](../guides/write-plugins.md) for alert sinks, assertion types, validators, and secret backends. The [plugins concept](../concepts/plugins.md) explains the loader and collision model behind them.

## Where next

- [Testing](testing.md) — the test architecture and how to run the integration lanes locally
- [Releases](releases.md) — how a merged PR title becomes a published version
