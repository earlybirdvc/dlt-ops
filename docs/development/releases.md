---
description: How dlt-ops releases are cut — python-semantic-release computes the version from conventional-commit PR titles, and a manual workflow_dispatch runs the verify-CI, release, smoke, and PyPI-publish job chain. Covers the bump mapping, triggering, and one-time maintainer setup.
---

# Releases

Releases are cut by a manual GitHub Actions run and versioned automatically from conventional-commit history. This page covers how the next version is decided, how a maintainer triggers a release, the job chain that builds, tests, and publishes it, and the one-time setup behind it. Read [contributing](contributing.md) first for why PR titles are conventional commits — that is the input this whole pipeline consumes.

## How the version is decided

**[python-semantic-release](https://python-semantic-release.readthedocs.io) (PSR, v10) computes the next version from the conventional-commit messages on `main` since the last tag.** Because the repo is squash-merged, each of those messages is a PR title. The mapping (`[tool.semantic_release]` in `pyproject.toml`):

| PR title | Bump | Example |
|---|---|---|
| `feat:` | minor | 0.3.1 → 0.4.0 |
| `fix:` / `perf:` | patch | 0.3.1 → 0.3.2 |
| a breaking change (`feat!:` or a `BREAKING CHANGE:` footer) | minor — stays 0.x | 0.3.1 → 0.4.0 |
| `docs:` / `refactor:` / `chore:` / `style:` / `test:` / `build:` / `ci:` | none | no release |

Two config choices shape this:

- `major_on_zero = false` with `allow_zero_version = true` keeps the package on 0.x: while the major is 0, a breaking change bumps the minor instead of going to 1.0. 1.0 is cut deliberately, never by a commit — the criteria live in [versioning](../reference/versioning.md).
- `parse_squash_commits = false` means PSR treats each commit message atomically, so the squash PR title alone decides the bump. Body bullets and branch commits contribute nothing: a `feat:` buried in a PR body under a `chore:` title ships no feature bump.

The version itself is single-sourced in `dlt_ops/__init__.py::__version__` (hatchling reads it for the build), so PSR stamps only that one file and never writes a `[project].version` key. It sits at `0.0.0` until the first release.

## Triggering a release

**The `Semantic Release` workflow runs only on `workflow_dispatch` — a manual run from the Actions tab — never on push.** It takes two inputs:

- `force_level` — `none` (the default; derive the bump from commits) or `patch`/`minor`/`major` to override. `none` is passed to PSR as an empty string; the other three force that level.
- `prerelease` — cut the next version as a prerelease.

The first release is handled automatically: with no tags in the repo, PSR computes from the full history and lands `0.1.0` given a `feat:` commit.

## The job chain

**Five jobs run in sequence; a failure at any stage stops the release before anything is published.**

1. **`verify-ci`** refuses to release off a red `main`. It polls the latest CI run on `main` and proceeds only if that run concluded `success`; an in-progress run gets up to 30 minutes before the job gives up. No green CI, no release.
2. **`release`** mints a GitHub App installation token (scoped to `contents: write`), checks out full history with it, and runs PSR: compute the version, stamp `dlt_ops/__init__.py`, update `CHANGELOG.md`, commit `chore(release): X.Y.Z`, tag `vX.Y.Z`, and create the GitHub Release. The wheel and sdist are built inside the PSR action container (`build_command = "python -m pip install uv && uv build"`); a follow-up step attaches them to the release, and they are uploaded as a `dist` artifact for the later jobs. It runs as a GitHub App rather than the default token because the release commit and tag must land on `main` past branch protection, which the default `GITHUB_TOKEN` cannot bypass.
3. **`smoke`** proves the built artifact works, in a clean `python:3.12-slim` container with no repo sources on the path. It installs the downloaded wheel (`[duckdb]`) into a fresh venv, asserts `dlt_ops.__version__` equals the released version, runs `dlt-ops --help`, then copies `tests/` and `examples/` out of the checkout and runs the E2E suite against the installed package. One E2E step is deselected — it drives the console script through `uv run` against the repo checkout, which would exercise repo sources rather than the wheel; the console script is proven directly by the `--help` step instead.
4. **`publish-pypi`** uploads `dist/` to PyPI via trusted publishing: OIDC (`id-token: write`), no API token, running in the `pypi` GitHub environment. The publish action emits PEP 740 attestations by default. It runs only after smoke passes.
5. **`summary`** always runs, writing the version, GitHub Release URL, and PyPI result — or "no release — no version-bumping commits since the last tag" — to the job summary.

## The changelog is machine-owned

**`CHANGELOG.md` is written by PSR at release time (`changelog.mode = "update"`): new version sections are inserted below the `<!-- version list -->` marker from the commit history.** Do not hand-edit it. PSR's own `chore(release):` commits are excluded from it (`exclude_commit_patterns`), and your PR title is the changelog entry.

## Maintainer setup (one-time)

**Before the first release, these must exist (they are documented in the workflow header too):**

- A **GitHub App** with repository permission `Contents: read and write`, installed on the repo and added to branch protection's bypass list so the release commit and tag can land on `main`. Its id goes in the repo variable `SEMANTIC_RELEASE_APP_ID`, its key in the secret `SEMANTIC_RELEASE_PRIVATE_KEY`.
- A **PyPI trusted publisher** for the `dlt-ops` project, pointing at this repo, the workflow file `semantic-release.yml`, and the environment `pypi` — no PyPI API token anywhere.
- The **`pypi` GitHub environment**, optionally with required reviewers for a manual approval gate before the upload.

## Where next

- [Contributing](contributing.md) — why PR titles are conventional commits, the pipeline's input
- [Versioning](../reference/versioning.md) — the 0.x contract and the 1.0 criteria
- [Compatibility](../reference/compatibility.md) — the dlt floor that ships with each release
