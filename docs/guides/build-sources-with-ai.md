---
description: Hand dlt source-writing to your AI assistant and keep the proving for dlt-ops — ground the assistant in current dlt and dlt-ops docs via context7, let it write a declarative rest_api config or a @dlt.resource, then close the deterministic validate, fix, run loop that makes an agent-written source trustworthy.
---

# Build sources with AI

`dlt-ops` ships no connectors — every source is a `@dlt.source` you write, and in 2026 an AI assistant writes most of them well. What an assistant cannot do is prove its output is safe to schedule. This guide hands the writing to the assistant and keeps the proving for `dlt-ops pipeline validate` — the deterministic, local, vendor-neutral check that turns a plausible source into a trusted one. Read [ingest your data](../getting-started/ingest-your-data.md) first if you have not picked a source type and destination yet.

**Prerequisites**

- `dlt-ops` with the DuckDB extra ([installation](../getting-started/installation.md)) — the worked loop runs fully offline, no credentials.
- An AI coding assistant that reads your repository. Claude Code, Cursor, and Codex all work; `dlt-ops` is assistant-neutral and adds no agent of its own.
- The worked loop below uses a custom `@dlt.resource` so it runs with no network; a real REST source follows the identical loop.

**The loop at a glance**

1. Ground the assistant in current dlt and `dlt-ops` docs via context7, so it writes against real APIs instead of hallucinating.
2. Let it write the source — a declarative `rest_api` config for a REST API, or a `@dlt.resource` for anything custom.
3. `dlt-ops pipeline validate` returns structural findings — no model, no network.
4. Feed the findings back; the assistant fixes exactly those.
5. `dlt-ops pipeline run -s <source> -y` proves it, and `pipeline status` records the outcome.

## Generation is solved; trust is the bottleneck

**dltHub's own numbers frame the problem this page solves.** By January 2026, 91% of new dlt pipelines were agent-authored ([dlthub.com/blog/agentic-data-engineering-course](https://dlthub.com/blog/agentic-data-engineering-course)), and dltHub puts the remaining gap plainly — "the bottleneck in data engineering has moved. It's no longer writing the code. It's trusting the code." ([dlthub.com/blog/ai-workbench](https://dlthub.com/blog/ai-workbench)). An assistant emits a `rest_api` config or a `@dlt.resource` in seconds; whether it references APIs that exist, keeps credentials out of the code, and runs its I/O where it belongs is the open question.

**That question is exactly what `dlt-ops pipeline validate` answers.** It runs no model and touches no network: it scans the layout, imports each source in a sandbox, and reports structural findings — a missing column model, a missing schedule, a broken naming chain, an import-time side effect. Because the feedback is deterministic and specific, the assistant fixes it without a human decoding a traceback. dltHub sells this trust layer as a managed product; `dlt-ops` gives you an open CLI you run locally and in CI, against whatever assistant you already use ([validation](../concepts/validation.md)).

## Ground the assistant in current APIs

**An assistant hallucinates when it writes from stale training data, so point it at the current docs before it writes a line.** dlt is indexed on context7 as `/dlt-hub/dlt`; append `use context7` to a prompt (or name the id directly) and the assistant fetches version-correct dlt APIs instead of inventing them:

```text
Write a dlt source for the GitHub issues API using the declarative rest_api config.
Ground every dlt API on /dlt-hub/dlt, and follow the dlt-ops project layout. use context7
```

Point it at `dlt-ops` too — this documentation site, which ships a `context7.json` so its pages resolve on context7 — so the assistant writes against the layout and config conventions `validate` enforces rather than guessing at them.

**These accelerants are dlt and dltHub tools, not `dlt-ops` features, and none is required:**

- **dlt-mcp** (`uv run --with dlt-mcp[duckdb] dlt-mcp`; repo `dlt-hub/dlt-mcp`) exposes a `search_docs` tool — a second in-loop channel for the assistant to verify a dlt feature before it uses it.
- **dlt's AI Workbench** (`dlthub ai init --agent claude|cursor|codex`, then `dlthub ai toolkit install rest-api-pipeline`; repo `dlt-hub/dlthub-ai-workbench`) installs dlt-authoring skills, a secrets skill, and the MCP server into your assistant's config directory.
- dltHub publishes `llms.txt` indexes at `dlthub.com/llms.txt` and `dlthub.com/docs/llms.txt` for whole-corpus grounding.

Whichever you use, `dlt-ops pipeline validate` is the deterministic check underneath — the part that does not depend on a model being right.

## Let the AI write the source

**Two shapes cover almost every source, both dlt features you wire into the `dlt-ops` layout.** Favor the declarative `rest_api` config for a REST API: the assistant emits a typed `RESTAPIConfig` dict — base URL, auth, pagination, endpoints — that maps almost one-to-one onto the API's own documentation, so there is far less room to be subtly wrong than in hand-written pagination code. Wrapped as a `@dlt.source` for discovery:

```python title="my_pipeline/source/github_issues.py"
"""GitHub issues via dlt's declarative rest_api config."""

from datetime import datetime

import dlt
import pydantic


class Issue(pydantic.BaseModel):
    id: int
    number: int
    title: str
    state: str
    created_at: datetime


@dlt.source(name="github_issues")
def github_issues_source():
    # Import rest_api INSIDE the function. A module-top-level import of it opens
    # a socket during import, which dlt-ops's import-safety rule flags.
    from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com",
            "auth": {"token": dlt.secrets["access_token"]},  # from .dlt/secrets.toml — never hardcoded
            "paginator": "header_link",
        },
        "resources": [
            {
                "name": "issues",
                "primary_key": "id",
                "write_disposition": "merge",
                "columns": Issue,
                "endpoint": {"path": "repos/dlt-hub/dlt/issues", "params": {"state": "open"}},
            },
        ],
    }
    return rest_api_source(config)
```

Two `dlt-ops` specifics the assistant must respect: import `rest_api` inside the `@dlt.source` function (its import opens a socket, which import-safety flags — see the next section), and pull the token from `dlt.secrets`, which resolves from `.dlt/secrets.toml` or the environment. For anything a REST config does not fit, the assistant writes a plain `@dlt.resource` generator instead. The worked loop below uses that custom shape so it runs offline — the `validate → fix → run` cycle is identical either way.

## Validate, fix, run — the loop that earns trust

**The assistant's first draft looks right and hides a footgun.** Asked for a `github_issues` source, it grounded the schema correctly — a Pydantic `Issue` model, `columns=Issue` — but added a top-level "optimization" that fetches the repository's metadata once, at import:

```python title="my_pipeline/source/github_issues.py"
"""GitHub issues source — the assistant's first draft."""

from datetime import UTC, datetime

import dlt
import pydantic
import requests


class Issue(pydantic.BaseModel):
    id: int
    number: int
    title: str
    state: str
    created_at: datetime


# AI-added "optimization": grab the repo's metadata once, at import. The
# defensive try/except hides the problem on a laptop.
try:
    _REPO = requests.get("https://api.github.com/repos/dlt-hub/dlt", timeout=5).json()
except Exception:
    _REPO = {}


class GitHubIssuesClient:
    """Stand-in for the paginated issues endpoint; fixture rows keep this offline.
    A real client would page the REST API here."""

    _PAGES = [
        # ... four issue records across two pages ...
    ]

    def pages(self):
        yield from self._PAGES


@dlt.resource(name="issues", columns=Issue, primary_key="id", write_disposition="merge")
def issues():
    for page in GitHubIssuesClient().pages():
        yield page


@dlt.source(name="github_issues")
def github_issues_source():
    return issues
```

**`validate` catches it with no model and no network.** It imports the module in a throwaway sandbox behind a CPython audit hook and records every network call the import makes — even though the `try/except` swallowed the result:

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✗ 3 error(s):
  [github_issues] import_safety: Rule 15: network at import of github_issues.py — socket.bind(<socket.socket fd=4, family=30, type=1, proto=0, laddr=('::', 0, 0, 0)>, ('::1', 0))
  [github_issues] import_safety: Rule 15: network at import of github_issues.py — socket.getaddrinfo(api.github.com:443)
  [github_issues] import_safety: Rule 15: network at import of github_issues.py — socket.connect(('140.82.121.5', 443))
```

This is the class of bug that "runs on my laptop" and then fires on every scheduler heartbeat, from a process you never think about — no type checker or linter sees it, because the code is valid Python. [Discovery](../concepts/discovery.md) explains why the sandbox exists and what else the audit hook flags.

**The fix is mechanical, and the assistant makes it straight from the finding.** Import-time work moves inside the resource generator, where it runs when the pipeline runs — not when the file is parsed. Deleting the top-level block and the now-unused `import requests` is the whole change; the resource already pages lazily. Validate again:

```text
Validating sources

✓ All sources validated successfully
```

**`run` proves the source actually works, not just that it looks right.** The resolved configuration prints first, then the fixture rows load into local DuckDB:

```bash
dlt-ops pipeline run -s github_issues -y
```

```text
Pipeline Configuration
----------------------------------------
  Source: github_issues
  Function: github_issues_source
  Resources: all (1 total)
  Destination: duckdb
  Dataset: github_raw (from .dlt/config.toml)
  Capabilities: full

Starting pipeline...
...
1 load package(s) were loaded to destination duckdb and into dataset github_raw
Load package 1784327443.540658 is LOADED and contains no failed jobs
```

**`status` reads the outcome back from the `_dlt_ops_runs` ledger in the destination.** A green run is recorded evidence the source works, not a guess:

```bash
dlt-ops pipeline status
```

```text
Source: github_issues
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-17 22:30:43  2026-07-17 22:30:44  4         cli        -               93523185dbc8
```

Four typed rows landed, and the source is now safe to schedule. The same loop reports a missing `columns=` model, a missing schedule, or a broken naming chain just as precisely — [add a source](add-a-source.md) walks those by hand, and [validation](../concepts/validation.md) is the full rule model.

## Pitfalls the loop is built to catch

**AI-authored sources fail in three predictable ways; ground them out at authoring time, and let `dlt-ops` catch the residue.**

| Pitfall | Keep it out at authoring time | Where it surfaces |
|---|---|---|
| **Hallucinated APIs** — endpoints, params, or dlt kwargs that do not exist | Ground on context7 `/dlt-hub/dlt` and the target API's own docs; verify mid-loop with dlt-mcp `search_docs` | A hallucinated dlt kwarg breaks the sandbox import, so `validate` reports it; a wrong endpoint only 404s at `run` |
| **Leaked secrets** — a hardcoded token, or credentials in `config.toml` | dlt reads credentials only from `secrets.toml` or the environment — the correct path is the easy one | dlt resolves the secret at `validate`/`run` and names the key it probed; a literal token in code is a review catch, not a `validate` one |
| **Import-time side effects** — a top-level network or disk call | Keep every fetch and write inside the resource generator | `import_safety` refuses it at `validate` (shown above); the rule is not skipped by a CI that skips human review |

Read the schema the assistant chose before you trust it: the Pydantic model is your contract, the `pydantic_columns_required` rule insists every `@dlt.resource` declares one, and the [reconciler](../concepts/reconciler.md) flags the day the live schema drifts from it.

!!! warning
    Never let an assistant hardcode a credential or write one into `config.toml` — dlt does not read secrets from there, and a literal token in a source module is a leak waiting for `git push`. Secrets belong in `.dlt/secrets.toml` (git-ignored) or the environment, resolved through `dlt.secrets`. A durable rule for your `CLAUDE.md` / `AGENTS.md`: never put credentials in chat or in code; edit `secrets.toml` yourself.

## What dlt-ops does not do

**`dlt-ops` ships no AI agent, no codegen, and no model — it does not write your source.** It provides the mandatory layout the assistant writes into, the deterministic `validate` loop that returns structural feedback, and the operational verbs (`run`, `status`, `backfill`) that prove and run the result. Bring your own assistant and, if you like, dltHub's own AI tooling; `dlt-ops` is the guardrail, not the generator.

## Where next

- [Ingest your data](../getting-started/ingest-your-data.md) — pick a source type and destination before you prompt
- [Add a source](add-a-source.md) — the same loop by hand, with the naming chain and column model in depth
- [Validation](../concepts/validation.md) — every rule `validate` runs, and the two enforcement tiers
- [Discovery](../concepts/discovery.md) — the import-safety sandbox and why import time must stay side-effect-free
