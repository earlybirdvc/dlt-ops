---
description: What counts as dlt-ops public API, the 0.x stability contract, the 1.0 criteria, and the 1.x deprecation policy. dlt-ops follows SemVer at major version 0, so breaking changes are possible between 0.x minors.
---

# Versioning and stability

`dlt-ops` follows [Semantic Versioning](https://semver.org) and is on major version 0: the public API and plugin surface can still break between minor releases. This page states what counts as public API, the 0.x contract, the 1.0 criteria, and the deprecation policy. Canonical source: `VERSIONING.md` in the repo root.

## What counts as public API

**Four surfaces carry the stability promise; everything else is internal — importable, but with no stability promise (see the convention note in `dlt_ops/__init__.py`).**

| Public surface | Where documented |
|---|---|
| Names exported from `dlt_ops` and listed in its `__all__` | [API reference](api.md) |
| CLI commands and options of the `dlt-ops` console script | [CLI reference](cli.md) |
| Entry-point group names (`dlt_ops.<axis>`) and the contracts plugins implement against them (`DestinationAdapter`, `Validator`, `SecretBackend`, `AlertSink`) | [Write plugins](../guides/write-plugins.md) |
| `.dlt/config.toml` keys under `[dlt_ops]` and `[sources.<name>.dlt_ops]` | [Config reference](../configuration/reference.md) |

## 0.x — unstable, breaking changes possible

**The 0.x line makes no cross-minor compatibility promise — pin to a minor range.**

- Breaking changes to any public surface may land in **any 0.x minor**. They are called out in the changelog; upgrade notes accompany them.
- Patch releases (0.x.y → 0.x.y+1) are backwards-compatible fixes only.
- Pin accordingly: `dlt-ops>=0.1,<0.2` is the safe range for 0.1 consumers.

## 1.0 criteria

**1.0 waits on three proofs that the surface has settled:**

- **2-3 third-party plugins** exercising the entry-points surface (any axis) in the wild.
- **3+ destinations** tested in CI (DuckDB, Postgres, BigQuery today; see [compatibility](compatibility.md)).
- Validator and assertion APIs mature — no known must-break redesigns outstanding.

## Deprecation policy (1.x line)

**On the 1.x line, a deprecated public name keeps working for at least two minor versions with a `DeprecationWarning`; the dlt floor sits outside this policy.**

- A deprecated public name keeps working for a **minimum of 2 minor versions** after the release that deprecates it, emitting a `DeprecationWarning` that names the replacement.
- Removal happens no earlier than the second minor after deprecation and is listed in the changelog's breaking section.
- The dlt floor is not covered by this policy — it may rise in a minor release when old dlt minors leave the verified matrix in [compatibility](compatibility.md); that is not treated as a breaking change of this package's API.
