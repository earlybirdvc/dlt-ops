# Versioning and stability

`dlt-ops` follows [Semantic Versioning](https://semver.org).
Current major: **0**.

## What counts as public API

- Names exported from `dlt_ops` and listed in its `__all__`.
- CLI commands and options of the `dlt-ops` console script.
- Entry-point group names (`dlt_ops.<axis>`) and the contracts plugins
  implement against them (`DestinationAdapter`, `Validator`, `SecretBackend`,
  `AlertSink`).
- `.dlt/config.toml` keys under `[dlt_ops]` and
  `[sources.<name>.dlt_ops]`.

Everything else is internal: importable, but with no stability promise
(see the convention note in `dlt_ops/__init__.py`).

## 0.x — unstable, breaking changes possible

Until the API and plugin surface settle:

- Breaking changes to any public surface may land in **any 0.x minor**.
  They are called out in the changelog; upgrade notes accompany them.
- Patch releases (0.x.y → 0.x.y+1) are backwards-compatible fixes only.
- Pin accordingly: `dlt-ops>=0.1,<0.2` is the safe range for 0.1
  consumers.

## 1.0 criteria

1.0 is cut when the surface has proven itself, concretely:

- **2-3 third-party plugins** exercising the entry-points surface
  (any axis) in the wild.
- **3+ destinations** tested in CI (DuckDB, Postgres, BigQuery today;
  see [COMPATIBILITY.md](COMPATIBILITY.md)).
- Validator and assertion APIs mature — no known must-break redesigns
  outstanding.

## Deprecation policy (1.x line)

- A deprecated public name keeps working for a **minimum of 2 minor
  versions** after the release that deprecates it, emitting a
  `DeprecationWarning` that names the replacement.
- Removal happens no earlier than the second minor after deprecation and is
  listed in the changelog's breaking section.
- The dlt floor is not covered by this policy — it may rise in a minor release when old dlt minors leave the verified matrix in [COMPATIBILITY.md](COMPATIBILITY.md); that is not treated as a breaking change of this package's API.
