"""Packaging invariants: version single-sourcing and the compat-table contract.

- ``dlt_ops.__version__`` is the single source of truth for the package
  version; hatchling injects it into the distribution metadata at build time
  (``[tool.hatch.version]`` in pyproject.toml). The metadata test catches both
  a broken hatch hookup and a version bump that skipped ``uv sync``.
- COMPATIBILITY.md's verified matrix and ``ci/dlt-versions.txt`` (the single
  source of truth for the verified dlt range) must list the same minors, and
  the ``dlt`` dependency must be a floor-only constraint anchored at the oldest
  verified minor — never an upper bound (users own their dlt version) — the
  same one-source-of-truth pattern tests/test_cleanup.py::TestCompatGuard
  enforces for ``dlt_ops._compat``.
"""

import importlib.metadata
import re
from pathlib import Path

import dlt_ops

REPO_ROOT = Path(__file__).parent.parent


def pinned_minors() -> tuple[str, ...]:
    pin_file = REPO_ROOT / "ci" / "dlt-versions.txt"
    return tuple(
        line.strip() for line in pin_file.read_text().splitlines() if line.strip() and not line.strip().startswith("#")
    )


class TestVersionSingleSourcing:
    def test_dunder_version_matches_distribution_metadata(self):
        assert importlib.metadata.version("dlt-ops") == dlt_ops.__version__


class TestCompatTable:
    def test_table_minors_match_ci_pin_file(self):
        """COMPATIBILITY.md rows and ci/dlt-versions.txt are one source of truth."""
        table = (REPO_ROOT / "COMPATIBILITY.md").read_text()
        table_minors = tuple(re.findall(r"^\| (\d+\.\d+) \|", table, flags=re.MULTILINE))
        assert table_minors == pinned_minors()

    def test_pyproject_dlt_floor_is_the_oldest_verified_minor(self):
        """Floor-only dlt constraint anchored at the pin file; an upper bound must never sneak back in."""
        minors = pinned_minors()
        pyproject = (REPO_ROOT / "pyproject.toml").read_text()
        assert f'"dlt>={minors[0]}"' in pyproject
        assert not re.search(r'"dlt>=[\d.]+,<', pyproject)
