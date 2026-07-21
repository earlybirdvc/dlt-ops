---
description: Two things to set deliberately before a dlt-ops project runs in production — how dlt resolves secrets (provider order, env vars, Docker/Kubernetes file secrets, Google and AWS secret managers, Airflow Variables, custom providers and their precedence) and how dlt's telemetry setting is configured.
---

# Production readiness

Two knobs an adopter should set on purpose rather than inherit: where secrets come from, and dlt's telemetry setting. Both belong to dlt rather than to `dlt-ops` — this page states what dlt does, factually, and where the decision points are, because the [deployment guide](deployment.md) hands a scheduled run one command and everything below is what that command resolves around it.

Everything here was read off the installed dlt source. Version-specific details move; the resolution *order* is the part worth designing around.

## Secrets

### The provider chain, in order

**dlt resolves a value by asking each config provider in turn and taking the first non-`None` answer.** Position in that list is precedence, and the list is fixed:

| # | Provider | Reads | Serves secrets | Enabled |
|---|---|---|---|---|
| 1 | `EnvironProvider` | environment variables, and `/run/secrets` files for secret-typed values | yes | always |
| 2 | `SecretsTomlProvider` | `.dlt/secrets.toml` (project, then the global dir) | yes | always |
| 3 | `ConfigTomlProvider` | `.dlt/config.toml` (project, then the global dir) | **no** | always |
| 4 | `AirflowSecretsTomlProvider` | an Airflow Variable | yes | `enable_airflow_secrets`, default **true** — active when `airflow.models` imports |
| 5 | `GoogleSecretsProvider` | Google Secret Manager | yes | `enable_google_secrets`, default **false** |
| 6 | `AwsSecretsManagerProvider` | AWS Secrets Manager | yes | `enable_aws_secrets`, default **false** |
| 7 | anything you register yourself | your code | as declared | on registration |

Row 3 is why credentials belong in `secrets.toml`: `config.toml`'s provider declares that it does not serve secrets, so a secret-typed lookup skips straight past it. Putting a `credentials` key there does not make it resolve.

The operational consequence people are surprised by is the ordering itself: **a `.dlt/secrets.toml` sitting in the working directory outranks every managed secret store below it.** A developer's leftover file, or one baked into an image, wins over Google Secret Manager, AWS Secrets Manager, and any provider you wrote. Design for that — keep `secrets.toml` out of production images, or accept that it is the highest-precedence store you operate.

### Environment variables

**Sections join with `__` and the whole key is uppercased.** `sources.orders.api_secret_key` is `SOURCES__ORDERS__API_SECRET_KEY`; a destination's connection string is `DESTINATION__POSTGRES__CREDENTIALS`. This is the route for CI runners and containers: scheduler secret store → environment → dlt, with nothing on disk.

### File-mounted secrets: Docker and Kubernetes

**For values dlt knows are secrets, the environment provider checks a mounted file *before* it checks the environment.** The path is `/run/secrets/<name>`, where `<name>` is the env-var key lowercased with every underscore turned into a hyphen — so `SOURCES__ORDERS__API_SECRET_KEY` is looked for at `/run/secrets/sources--orders--api-secret-key`. Docker Compose writes a secret as that file directly; Kubernetes mounts a directory, so if the path is a directory dlt appends the same name again and reads `/run/secrets/sources--orders--api-secret-key/sources--orders--api-secret-key`. Any read error, missing file included, falls through silently to the ordinary environment lookup.

Two qualifications worth knowing. The file path is only consulted for **secret-typed** lookups — `dlt.secrets[...]`, and arguments annotated `dlt.secrets.value` — never for plain config. And on a hit, dlt returns the file's contents unstripped but also writes a stripped copy into the process environment so forked child processes inherit it.

### Managed secret stores that ship with dlt

**Google Secret Manager and AWS Secrets Manager providers ship in OSS dlt and are off by default.** Both are enabled under the `[providers]` section, so `enable_google_secrets` is `[providers] enable_google_secrets = true` in `config.toml` or `PROVIDERS__ENABLE_GOOGLE_SECRETS=true` in the environment; `enable_aws_secrets` follows the same shape. Each provider's own credentials nest one level deeper — `providers.google_secrets.credentials.*`, or `PROVIDERS__GOOGLE_SECRETS__CREDENTIALS__*`. Enabling one without giving it credentials fails loudly at startup, naming every key it tried.

The AWS provider prefixes every secret name with `dlt/` by default, so the key `sources.orders.api_secret_key` becomes the secret `dlt/sources/orders/api_secret_key`. The prefix is configurable via `secret_name_prefix`, and an empty string disables it.

Both providers sit *below* the TOML providers in the chain. That is the ordering described above, and it is the one to design around.

### Airflow

**The Airflow provider is enabled by default and reads Airflow Variables — only Variables.** With `enable_airflow_secrets` on (its default) and `airflow.models` importable, dlt looks secrets up as Airflow Variables, and the blessed key is `dlt_secrets_toml`: an Airflow Variable holding the *contents* of a `secrets.toml`, which dlt parses as a whole document. Set that one Variable and every secret inside it resolves.

**Airflow Connections are not read.** dlt's config system has no Connection provider — an Airflow Connection is invisible to it, so a credential that lives only in a Connection will not resolve. Either mirror it into a Variable or hand it over as an environment variable.

`dlt-ops` also ships an Airflow secret backend on its own `secret_backend` [plugin axis](../concepts/plugins.md), which is a different mechanism: it fetches a named Variable at run start and writes it into `dlt.secrets` for that process. A source engages it by declaring `airflow_var` in its `[sources.<X>.dlt_ops]` table. Sources that declare nothing fall through to dlt's own providers, which read `.dlt/secrets.toml` and the rest natively.

### Writing your own provider

**`register_provider` appends to the end of the chain, so a custom provider answers only when nothing above it did.** That is deliberate — registration cannot silently shadow an operator's environment or files — but it means a custom vault provider is the *last* place dlt looks, not the first. If it must win, the reliable pattern is to fetch at startup and export into the environment (or write `dlt.secrets`) rather than to rely on provider ordering.

**Only three concrete vault providers ship in OSS dlt: Google, AWS, and Airflow.** `VaultDocProvider` in `dlt/common/configuration/providers/vault.py` is an abstract base class — it declares abstract `_look_vault` and `_list_vault` and never implements `name`, so it cannot be instantiated. Its docstring mentions Hashicorp as the kind of thing the base exists to support; no Hashicorp provider ships. Subclass the base if you want one, and expect to write the two lookup methods yourself.

## Telemetry

**dlt collects anonymous usage telemetry by default.** The setting is `dlthub_telemetry`, which defaults to `True`, and the endpoint is `https://telemetry.scalevector.ai`. It is disabled automatically in exactly one case — on a platform without threading support (Pyodide/WASM), because sending it uses a background thread. `dlt-ops` does not change the setting in either direction; it is dlt's, and it stays whatever your environment resolves it to.

Set it deliberately. Three equivalent ways, in the same precedence as any other dlt config:

```bash
# environment variable — the route for containers and CI
export RUNTIME__DLTHUB_TELEMETRY=false
```

```toml
# .dlt/config.toml — ships with the checkout, so the whole project inherits it
[runtime]
dlthub_telemetry = false
```

```bash
# dlt's own CLI flags: these WRITE the setting into config.toml (local and global)
dlt --disable-telemetry pipeline --list-pipelines
dlt --enable-telemetry pipeline --list-pipelines
```

`dlt telemetry` prints the current status without changing it. Note that dlt's `--non-interactive` and `-y` / `--yes` global flags do **not** affect telemetry — they only control prompt and confirmation behaviour — so a non-interactive scheduled run does not implicitly change the setting. If you want it off in production, set it explicitly by one of the three routes above.

## Where next

- [Deployment and scheduling](deployment.md) — the rungs from a dev loop to an orchestrator, and where these settings apply
- [Configuration](../configuration/index.md) — the config model and the `[dlt_ops]` namespace
- [Plugins](../concepts/plugins.md) — the `secret_backend` axis and the other extension points
