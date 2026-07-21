# dlt-ops — dev conventions

Conventions for contributors, human or agent. Setup, test-lane details, and the plugin-authoring guide live in [CONTRIBUTING.md](CONTRIBUTING.md); this file is the short list a contributor most often needs mid-change.

## The rule

This package is generic across every source — per-source facts live in the user's `.dlt/config.toml`, never in package code. A hardcoded set literal or per-provider constant is a smell.

## Workflow (uv)

- `uv sync --locked` — package + dev group (pytest, ruff, pyrefly, the DuckDB lane)
- `uv run pytest` — full suite, credential-free by default (cloud-gated tests self-skip; docker enables the Postgres lanes)
- `uv run pytest -m integration` — the cross-system lanes only (filesystem / DuckDB / Postgres)
- `uv run ruff check` + `uv run ruff format --check` — lint (line-length 120, target py311)
- `uv run pyrefly check` — types (`[tool.pyrefly]` in pyproject)
- `bash ci/sql_boundary_guard.sh` — the SQL-boundary guard, stays green
- `uv run --group docs mkdocs build --strict` — the docs site must build clean

## Conventions that gate merges

- Squash-merge only; the PR title must parse as a conventional commit — it becomes the commit on main and drives release automation.
- All destination SQL is canonical DuckDB-dialect with `?` placeholders, routed through a `DestinationAdapter`; never raw dialect SQL or string-built identifiers (the SQL-boundary guard enforces this).
- Dependencies declare floors, never upper bounds — users own their dlt/extras resolution.
- Markdown is written with long lines; never hard-wrap prose.

## Public API convention

- Public = exported from the `dlt_ops` top level (listed in `__all__`) or from an explicitly-public subpackage (`dlt_ops.airflow`). Everything else is internal — importable, but with no stability promise.
- New internal modules are underscore-prefixed (`_sandbox_child.py` is the pattern); existing non-underscore internals are grandfathered but never exported.
- Every `__init__.py` that exports anything declares `__all__`.
- Heavy or optional dependencies never load at `import dlt_ops` time — re-export lazily via PEP 562 module `__getattr__` (the reconciler names are the pattern). `tests/test_api.py` locks the surface and import hygiene.
- Tests and examples import exported names from the top level, not deep module paths; internal names are imported from their defining module.
