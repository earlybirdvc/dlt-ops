"""Packaging invariants: version single-sourcing and the compat-table contract.

- ``dlt_ops.__version__`` is the single source of truth for the package
  version; hatchling injects it into the distribution metadata at build time
  (``[tool.hatch.version]`` in pyproject.toml). The metadata test catches both
  a broken hatch hookup and a version bump that skipped ``uv sync``.
- COMPATIBILITY.md's verified matrix and ``ci/dlt-versions.txt`` (the single
  source of truth for the CI-verified dlt range) must list the same minors, and
  the ``dlt`` dependency must be a floor-only constraint anchored at the oldest
  verified minor — never an upper bound (users own their dlt version).
- No module may gate a feature on an allowlist of dlt minors. The verified
  matrix says what CI exercised; it must never become a runtime refusal, or a
  fresh install breaks the day dlt ships a minor this repo has not seen.
"""

import ast
import importlib.metadata
import re
from pathlib import Path

import pytest

import dlt_ops

REPO_ROOT = Path(__file__).parent.parent


def pinned_minors() -> tuple[str, ...]:
    pin_file = REPO_ROOT / "ci" / "dlt-versions.txt"
    return tuple(
        line.strip()
        for line in pin_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    )


class TestVersionSingleSourcing:
    def test_dunder_version_matches_distribution_metadata(self):
        assert importlib.metadata.version("dlt-ops") == dlt_ops.__version__


class TestLongDescription:
    """README reaches the distribution metadata, which is the PyPI project page.

    Nothing in a source checkout depends on this, so an omission stays invisible
    until the page renders empty on a release that cannot be re-uploaded under
    the same version.
    """

    def test_metadata_carries_the_readme(self):
        meta = importlib.metadata.metadata("dlt-ops")
        assert meta.get("Description-Content-Type", "").startswith("text/markdown")
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        # get_payload() is where the long description lands in this metadata version.
        assert meta.get_payload().strip().startswith(readme.strip()[:80])

    def test_readme_links_are_absolute(self):
        """A relative link resolves against the rendering host, and PyPI is not GitHub.

        The same README serves both, so `docs/x.md` — correct on GitHub — becomes
        pypi.org/project/dlt-ops/docs/x.md there. Absolute is the only form that
        holds on every surface the long description reaches.
        """
        targets = re.findall(r"\]\(([^)]+)\)", (REPO_ROOT / "README.md").read_text(encoding="utf-8"))
        relative = [t for t in targets if not t.startswith(("http://", "https://", "#", "mailto:"))]
        assert relative == []


class TestClassifiers:
    def test_python_classifiers_match_the_ci_matrix(self):
        """The pyversions badge and PyPI's filters read these, and nothing else does.

        A Python version added to the CI matrix but not here is tested and
        undeclared; one declared but not in the matrix is advertised untested.
        """
        declared = {
            c.rsplit(" :: ", 1)[1]
            for c in importlib.metadata.metadata("dlt-ops").get_all("Classifier") or []
            if c.startswith("Programming Language :: Python :: ") and c.rsplit(" :: ", 1)[1][0].isdigit()
        }
        workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        matrix = set(re.search(r"python: \[([^\]]+)\]", workflow).group(1).replace('"', "").split(", "))
        assert declared == matrix


class TestCompatTable:
    def test_table_minors_match_ci_pin_file(self):
        """COMPATIBILITY.md rows and ci/dlt-versions.txt are one source of truth."""
        table = (REPO_ROOT / "COMPATIBILITY.md").read_text(encoding="utf-8")
        table_minors = tuple(re.findall(r"^\| (\d+\.\d+) \|", table, flags=re.MULTILINE))
        assert table_minors == pinned_minors()

    def test_pyproject_dlt_floor_is_the_oldest_verified_minor(self):
        """Floor-only dlt constraint anchored at the pin file; an upper bound must never sneak back in."""
        minors = pinned_minors()
        pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        assert f'"dlt>={minors[0]}"' in pyproject
        assert not re.search(r'"dlt>=[\d.]+,<', pyproject)

    def test_no_module_gates_a_feature_on_a_dlt_minor_allowlist(self):
        """The verified matrix is a CI fact, never a runtime ceiling.

        A hardcoded set of dlt minors in package code is what made a fresh
        install lose a feature the day dlt shipped a new minor; every verb now
        runs on any dlt at or above the floor.
        """
        offenders = [
            path.relative_to(REPO_ROOT).as_posix()
            for path in (REPO_ROOT / "dlt_ops").rglob("*.py")
            if re.search(r"SUPPORTED_DLT_MINORS|is_dlt_version_supported", path.read_text(encoding="utf-8"))
        ]
        assert offenders == []


class TestTextIoPinsEncoding:
    """No text file I/O may resolve its codec from the platform locale.

    Windows reads ``write_text``/``read_text`` without ``encoding=`` as cp1252,
    so a body carrying any non-ASCII character — an em dash is enough — is
    written in one codec and read back in another. Pinning only the package
    side is what left the gap: a fixture that wrote a source module through the
    locale codec produced a file the UTF-8-pinned AST reader could not decode,
    which reads as a parse failure in a source the test never made invalid.
    Both sides are scanned here because the round trip is what has to hold.
    """

    @pytest.mark.parametrize("package", ["dlt_ops", "tests"])
    def test_every_text_io_call_names_its_encoding(self, package):
        offenders = []
        for path in sorted((REPO_ROOT / package).rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in {"write_text", "read_text"}
                    and not any(kw.arg == "encoding" for kw in node.keywords)
                ):
                    offenders.append(f"{path.relative_to(REPO_ROOT).as_posix()}:{node.lineno}")
        assert offenders == []
