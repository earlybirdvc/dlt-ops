"""Rule 15 / two-phase discovery tests: Phase-2 sandbox, validators, CLI.

The sandbox fixtures are self-contained: the network fixture attempts a
localhost connect to the discard port (the ATTEMPT is the violation — no
real network, no dependency on connectivity), the disk-write fixture drops a
canary file next to itself, and the read fixture loads a sibling TOML.
"""

import os
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from dlt_ops import ValidationContext, ValidationError, validate_sources
from dlt_ops.cli.cli import cli
from dlt_ops.discovery import ImportViolation, SourceInfo, discover, discover_sources, introspect
from dlt_ops.discovery.phase2 import SOURCE_MODULE_NAMESPACE
from dlt_ops.discovery.validators.import_safety import (
    COVERAGE_FIELD,
    validate_import_errors,
    validate_import_safety,
)

NETWORK_AT_IMPORT = """
    import socket

    import dlt

    _probe = socket.socket()
    _probe.settimeout(0.05)
    try:
        _probe.connect(("127.0.0.1", 9))  # attempt is the violation; port expected closed
    except OSError:
        pass
    finally:
        _probe.close()

    @dlt.resource(name="metrics")
    def metrics():
        yield {"id": 1}

    @dlt.source(name="web_metrics")
    def web_metrics_source():
        return metrics
"""

DISK_WRITE_AT_IMPORT = """
    from pathlib import Path

    import dlt

    Path(__file__).with_name("canary.txt").write_text("side effect")

    @dlt.resource(name="orders")
    def orders():
        yield {"id": 1}

    @dlt.source(name="orders_api")
    def orders_api_source():
        return orders
"""

TOML_READ_AT_IMPORT = """
    import tomllib
    from pathlib import Path

    import dlt

    _settings = tomllib.loads(Path(__file__).with_name("settings.toml").read_text())

    @dlt.resource(name="items")
    def items():
        yield {"page_size": _settings["page_size"]}

    @dlt.source(name="catalog_api")
    def catalog_api_source():
        return items
"""

PIPELINE_RUN_AT_IMPORT = """
    from pathlib import Path

    import dlt

    _pipeline = dlt.pipeline(
        pipeline_name="import_time_probe",
        destination="duckdb",
        pipelines_dir=str(Path(__file__).parent / "_work"),  # keep writes inside tmp
    )

    @dlt.source(name="runner_api")
    def runner_api_source():
        return []
"""

RAISES_AT_IMPORT = """
    import dlt

    raise RuntimeError("boom at import")

    @dlt.source(name="broken_api")
    def broken_api_source():
        return []
"""

HEALTHY_SOURCE = """
    import dlt

    @dlt.resource(name="rows")
    def rows():
        yield {"id": 1}

    @dlt.source(name="healthy_api")
    def healthy_api_source():
        return rows
"""

# `requests` is a hard dependency of dlt, so it is present wherever this runs.
# Importing it executes urllib3's import-time IPv6 probe, which binds an
# AF_INET6 socket (urllib3/util/connection.py: HAS_IPV6 = _has_ipv6("::1")) —
# a socket.bind audit event fired by a library initialising itself.
IMPORTS_HTTP_LIBRARY = """
    import dlt
    import requests  # noqa: F401 — the import itself is the subject

    @dlt.resource(name="{name}_rows")
    def rows():
        yield {{"id": 1}}

    @dlt.source(name="{name}")
    def source_fn():
        return rows
"""

# The violation Rule 15 exists for: the user's module body calling out over the
# network at import. The attempt is the violation, so the refused connect is
# swallowed and the module still imports cleanly.
NETWORK_VIA_LIBRARY_AT_IMPORT = """
    import dlt
    import requests

    try:
        requests.get("http://127.0.0.1:9/probe", timeout=0.05)  # port expected closed
    except Exception:
        pass

    @dlt.resource(name="{name}_rows")
    def rows():
        yield {{"id": 1}}

    @dlt.source(name="{name}")
    def source_fn():
        return rows
"""

# Appends one line per EXECUTION of the module, so the file counts how many
# processes ran it (the disk write is also the Rule 15 violation). The sandbox
# child is expected to run it exactly once; a second line means the module also
# executed in the caller's process.
EXECUTION_COUNTING_SOURCE = """
    import os
    from pathlib import Path

    import dlt

    with Path(__file__).with_name("executions.log").open("a") as fh:
        fh.write(str(os.getpid()) + "\\n")

    @dlt.resource(name="{name}_rows")
    def rows():
        yield {{"id": 1}}

    @dlt.source(name="{name}")
    def source_fn():
        return rows
"""


def _two_phase(root: Path) -> dict[str, SourceInfo]:
    return introspect(root, discover(root))


class TestSandboxVerdicts:
    """introspect() flags Rule 15 violations and isolates import failures."""

    def test_malformed_payload_rejected_before_dispatch(self, tmp_path):
        """Mode dispatch in the child is key-based; a payload matching neither
        accepted shape fails the protocol loudly instead of guessing a branch."""
        from dlt_ops.discovery.phase2 import _spawn_sandbox_child

        verdict = _spawn_sandbox_child(
            {"project_root": str(tmp_path), "module_name": "x"},  # neither shape
            project_root=tmp_path,
        )
        assert verdict.sandbox_error is not None
        assert "unrecognized sandbox payload keys" in verdict.sandbox_error

    def test_network_at_import_flagged(self, make_project):
        root = make_project(files={"web/source/web_metrics.py": NETWORK_AT_IMPORT})
        info = _two_phase(root)["web_metrics"]

        assert {v.kind for v in info.import_violations} == {"network"}
        assert "socket.connect" in {v.event for v in info.import_violations}
        # The module imports cleanly (it swallows the refused connect) — and is
        # still withheld from the in-process import, because running it here is
        # the very side effect the sandbox exists to keep out of this process.
        assert info.is_introspected is False
        assert info.import_error is not None
        assert "Rule 15" in info.import_error

    def test_disk_write_at_import_flagged(self, make_project):
        root = make_project(files={"orders/source/orders_api.py": DISK_WRITE_AT_IMPORT})
        info = _two_phase(root)["orders_api"]

        write_violations = [v for v in info.import_violations if v.kind == "disk-write"]
        assert write_violations, info.import_violations
        assert any(v.target.endswith("canary.txt") for v in write_violations)
        assert info.is_introspected is False

    def test_violating_module_names_its_findings_and_the_opt_out(self, make_project):
        """The withholding error is the only detail every downstream consumer
        gets (backfill's gate, run's RuntimeError), so it must carry both."""
        root = make_project(files={"orders/source/orders_api.py": DISK_WRITE_AT_IMPORT})
        error = _two_phase(root)["orders_api"].import_error

        assert error is not None
        assert "disk-write" in error
        assert "canary.txt" in error
        assert "import_safety = false" in error

    def test_disk_read_at_import_passes(self, make_project):
        root = make_project(
            files={
                "catalog/source/catalog_api.py": TOML_READ_AT_IMPORT,
                "catalog/source/settings.toml": "page_size = 100\n",
            },
        )
        info = _two_phase(root)["catalog_api"]

        assert info.import_violations == ()
        assert info.import_error is None
        assert info.is_introspected is True
        assert info.resources == ("items",)

    def test_pipeline_construction_at_import_flagged(self, make_project):
        """dlt.pipeline(...) at import trips the pipeline-run marker (and the
        construction's own disk writes trip the hook independently)."""
        root = make_project(files={"runner/source/runner_api.py": PIPELINE_RUN_AT_IMPORT})
        info = _two_phase(root)["runner_api"]

        pipeline_hits = [v for v in info.import_violations if v.kind == "pipeline-run"]
        assert pipeline_hits, info.import_violations
        assert pipeline_hits[0].event == "dlt.pipeline"
        assert "import_time_probe" in pipeline_hits[0].target
        assert any(v.kind == "disk-write" for v in info.import_violations)

    def test_raise_at_import_isolated_sibling_unaffected(self, make_project):
        root = make_project(
            files={
                "broken/source/broken_api.py": RAISES_AT_IMPORT,
                "healthy/source/healthy_api.py": HEALTHY_SOURCE,
            },
        )
        result = _two_phase(root)

        # Phase 1 listed both; Phase 2 recorded the failure without dropping it.
        assert set(result) == {"broken_api", "healthy_api"}
        broken = result["broken_api"]
        assert broken.is_introspected is False
        assert broken.import_error is not None
        assert "boom at import" in broken.import_error
        healthy = result["healthy_api"]
        assert healthy.is_introspected is True
        assert healthy.resources == ("rows",)

    def test_composite_drops_failed_import_keeps_sibling(self, make_project):
        root = make_project(
            files={
                "broken/source/broken_api.py": RAISES_AT_IMPORT,
                "healthy/source/healthy_api.py": HEALTHY_SOURCE,
            },
        )
        assert set(discover_sources(root)) == {"healthy_api"}


class TestLibraryImportAttribution:
    """Rule 15 targets what the PROJECT's module does at import, not what the
    libraries it imports do to initialise themselves.

    The regression: `import requests` alone tripped the hook, because urllib3
    binds an AF_INET6 socket at its own import to probe for IPv6 support. Every
    REST source imports an HTTP client, so the rule excluded them all from
    Phase 2 — and with it from every other rule (see TestReducedCoverageIsLoud).
    """

    def _project(self, make_project, name: str, body: str):
        return make_project(
            config=(
                "[dlt_ops]\n"
                'default_destination = "duckdb"\n'
                'default_dataset = "raw"\n\n'
                f'[sources.{name}.dlt_ops]\nschedule = "@daily"\n'
            ),
            files={f"{name}/source/{name}.py": body.format(name=name)},
        )

    def test_importing_an_http_library_is_not_a_violation(self, make_project):
        name = "http_import_api"
        info = _two_phase(self._project(make_project, name, IMPORTS_HTTP_LIBRARY))[name]

        assert info.import_violations == (), "a library's own import-time work was blamed on the module"
        assert info.import_error is None
        # The operative consequence: the module is usable, not withheld.
        assert info.is_introspected is True
        assert info.resources == (f"{name}_rows",)

    def test_module_level_request_is_still_a_violation(self, make_project):
        """The boundary must not have been widened into blindness: a call the
        module body itself makes is attributed to the module body."""
        name = "http_call_api"
        info = _two_phase(self._project(make_project, name, NETWORK_VIA_LIBRARY_AT_IMPORT))[name]

        assert {v.kind for v in info.import_violations} == {"network"}
        assert "socket.connect" in {v.event for v in info.import_violations}
        assert info.is_introspected is False
        assert info.import_error is not None and "Rule 15" in info.import_error

    def test_only_the_library_probe_is_dropped_from_a_violating_module(self, make_project):
        """A module doing both keeps its own finding and loses only the library's.

        urllib3's bind fires in both modules; it must be absent from the
        violating one too, or the fix would merely be reordering noise.
        """
        name = "http_mixed_api"
        info = _two_phase(self._project(make_project, name, NETWORK_VIA_LIBRARY_AT_IMPORT))[name]

        assert "socket.bind" not in {v.event for v in info.import_violations}

    def test_validate_raises_no_import_health_finding_for_a_plain_http_import(self, make_project):
        """End-to-end shape of the defect: turning the rule on used to replace a
        REST source's real findings with import-health noise. Now the source
        reaches the rules — its ordinary findings are what validate reports."""
        name = "http_clean_api"
        errors = validate_sources(self._project(make_project, name, IMPORTS_HTTP_LIBRARY))

        health = {"import", "import_safety", COVERAGE_FIELD}
        assert [e for e in errors if e.field in health] == []
        # The rules ran: this fixture's resource carries no columns= hint.
        assert [e.field for e in errors if e.source_name == name] == [f"resource.{name}_rows"]


class TestAttributionUnits:
    """The path half of the attribution boundary, tested without a sandbox run."""

    @pytest.fixture
    def child(self, tmp_path):
        """The sandbox-child module with prefixes pinned to tmp_path, then restored."""
        from dlt_ops.discovery import _sandbox_child

        before = _sandbox_child._project_prefixes
        _sandbox_child._set_project_prefixes(str(tmp_path))
        yield _sandbox_child
        _sandbox_child._project_prefixes = before

    def test_project_source_is_project_code(self, child, tmp_path):
        assert child._is_project_file(str(tmp_path / "web" / "source" / "web_api.py")) is True

    def test_installed_library_is_not(self, child):
        assert child._is_project_file("/usr/lib/python3.11/site-packages/urllib3/util/connection.py") is False

    def test_a_venv_inside_the_project_is_still_a_library(self, child, tmp_path):
        """The common layout: .venv under the project root. Its packages are
        libraries, so a prefix match alone would misattribute all of them."""
        vendored = tmp_path / ".venv" / "lib" / "python3.11" / "site-packages" / "urllib3" / "connection.py"
        assert child._is_project_file(str(vendored)) is False

    def test_unpinned_prefixes_fail_open(self, child):
        """Attribution filters a safety rule. Losing it must make the rule noisy,
        never silently dead — the failure mode this whole rule keeps hitting."""
        child._project_prefixes = ()
        assert child._is_project_file("/anywhere/at/all.py") is True

    def test_non_file_code_objects_are_not_project_code(self, child):
        """exec'd strings and frozen modules carry no attributable path."""
        assert child._is_project_file("<string>") is False
        assert child._is_project_file("<frozen importlib._bootstrap>") is False


class TestReducedCoverageIsLoud:
    """Excluding a source from Phase 2 also excludes it from every rule that
    iterates ctx.sources. Silence there makes "clean" and "never checked"
    identical in validate output — the worst property this pass can have.
    """

    CONFIG = """
        [dlt_ops]
        default_destination = "duckdb"
        default_dataset = "raw"

        [sources.broken_api.dlt_ops]
        schedule = "@daily"
    """
    # Untyped resource: pydantic_columns_required fires on it whenever the rule
    # actually reaches the source.
    UNTYPED_SOURCE = """
        import dlt

        @dlt.resource(name="untyped_rows")
        def untyped_rows():
            yield {"id": 1}

        @dlt.source(name="broken_api")
        def broken_api_source():
            return untyped_rows

        raise RuntimeError("boom at import")
    """

    @pytest.fixture
    def errors(self, make_project):
        root = make_project(config=self.CONFIG, files={"broken/source/broken_api.py": self.UNTYPED_SOURCE})
        return validate_sources(root)

    def test_lost_coverage_is_reported(self, errors):
        coverage = [e for e in errors if e.field == COVERAGE_FIELD]
        assert [e.source_name for e in coverage] == ["broken_api"]
        assert "reduced rule coverage" in coverage[0].message

    def test_it_is_an_error_not_a_warning(self, errors):
        """A warning is filtered out of every non-strict run — which is exactly
        the run that must not imply it checked more than it did. `errors` comes
        from a non-strict validate_sources, so the finding being present here is
        itself half the assertion."""
        coverage = [e for e in errors if e.field == COVERAGE_FIELD]
        assert coverage and all(not e.is_warning for e in coverage)

    def test_the_notice_is_a_true_statement(self, errors):
        """Guard against the notice becoming decoration: the rule findings it
        says are missing must actually be missing."""
        assert [e for e in errors if e.field.startswith("resource.")] == []

    def test_an_importable_sibling_keeps_full_coverage(self, make_project):
        """Scoped per source: one broken module must not mute its neighbours."""
        root = make_project(
            config=self.CONFIG + '\n[sources.healthy_api.dlt_ops]\nschedule = "@daily"\n',
            files={
                "broken/source/broken_api.py": self.UNTYPED_SOURCE,
                "healthy/source/healthy_api.py": HEALTHY_SOURCE,
            },
        )
        errors = validate_sources(root)

        assert [e.source_name for e in errors if e.field == COVERAGE_FIELD] == ["broken_api"]
        # The healthy source was reached by the rules, untyped resource and all.
        assert [e.source_name for e in errors if e.field.startswith("resource.")] == ["healthy_api"]


class TestSandboxContainment:
    """The sandbox must contain what it detects: a module caught violating
    Rule 15 executes in the throwaway child and nowhere else. Detecting the
    violation and then importing the module anyway would run the offending
    `requests.get(...)` for real in the validating process."""

    # Pipeline/module names unique per test: the synthetic module namespace is
    # process-global, so a name another test imported would mask a regression
    # here (and vice versa).
    def _project(self, make_project, name: str, *, knob_off: bool = False) -> Path:
        config = f'[dlt_ops]\n\n[sources.{name}.dlt_ops]\nschedule = "@daily"\n'
        if knob_off:
            config += "\n[dlt_ops.rules]\nimport_safety = false\n"
        return make_project(
            config=config,
            files={f"{name}/source/{name}.py": EXECUTION_COUNTING_SOURCE.format(name=name)},
        )

    @staticmethod
    def _executions(root: Path, name: str) -> list[str]:
        log = root / name / "source" / "executions.log"
        return log.read_text().splitlines() if log.exists() else []

    def test_violating_module_executes_only_in_the_sandbox_child(self, make_project):
        name = "contained_probe_api"
        root = self._project(make_project, name)

        validate_sources(root)

        executions = self._executions(root, name)
        assert len(executions) == 1, f"module executed {len(executions)}x; the sandbox did not contain it"
        assert str(os.getpid()) not in executions, "violating module executed in the validating process"
        assert f"{SOURCE_MODULE_NAMESPACE}.{name}.source.{name}" not in sys.modules

    def test_validate_still_reports_the_violation_it_withheld(self, make_project):
        """Containment must not cost visibility: the Rule 15 finding survives,
        joined by the import error explaining why the module was not loaded."""
        name = "reported_probe_api"
        errors = validate_sources(self._project(make_project, name))

        assert [e.source_name for e in errors if e.field == "import_safety"] == [name]
        withheld = [e for e in errors if e.field == "import"]
        assert [e.source_name for e in withheld] == [name]
        assert "Rule 15" in withheld[0].message

    def test_knob_off_restores_the_in_process_import(self, make_project):
        """`import_safety = false` is the documented opt-out: no child, no
        containment, the module loads and runs as the project intends."""
        name = "optout_probe_api"
        root = self._project(make_project, name, knob_off=True)
        info = _two_phase(root)[name]

        assert info.is_introspected is True
        assert info.import_error is None
        assert self._executions(root, name) == [str(os.getpid())]


class TestImportSafetyKnob:
    """[dlt_ops.rules] import_safety = false: rule off, isolation stays."""

    KNOB_OFF_CONFIG = """
        [dlt_ops]

        [dlt_ops.rules]
        import_safety = false
    """

    def test_knob_off_skips_sandbox_but_isolates_errors(self, make_project):
        root = make_project(
            config=self.KNOB_OFF_CONFIG,
            files={
                "orders/source/orders_api.py": DISK_WRITE_AT_IMPORT,
                "broken/source/broken_api.py": RAISES_AT_IMPORT,
            },
        )
        result = _two_phase(root)

        # No sandbox pass — no violations recorded, module still enriched.
        orders = result["orders_api"]
        assert orders.import_violations == ()
        assert orders.is_introspected is True
        # A raising module is still isolated: recorded, sibling unaffected.
        broken = result["broken_api"]
        assert broken.is_introspected is False
        assert broken.import_error is not None
        assert "boom at import" in broken.import_error

    def test_knob_off_validate_reports_import_failure_but_no_rule15(self, make_project):
        root = make_project(
            config=self.KNOB_OFF_CONFIG,
            files={
                "orders/source/orders_api.py": DISK_WRITE_AT_IMPORT,
                "broken/source/broken_api.py": RAISES_AT_IMPORT,
            },
        )
        errors = validate_sources(root)

        assert [e.source_name for e in errors if e.field == "import"] == ["broken_api"]
        assert [e for e in errors if e.field == "import_safety"] == []


class TestPerSourceExemptionRefused:
    """`import_safety` is the one rule with no per-source exemption.

    Every other exemption filters findings out of a rule that only ever
    reported. This one also decides whether Phase 2 imports the module into the
    calling process — a per-module decision, taken in a discovery pass every
    consumer runs. The regression this guards: the exemption used to be
    accepted, silence the Rule 15 finding, and change nothing else, so the user
    got a bare `import` error for a module they had explicitly opted out.
    """

    def _config(self, source: str, *, rule: str = "import_safety") -> str:
        return (
            f'[dlt_ops]\n\n[sources.{source}.dlt_ops]\nschedule = "@daily"\n\n'
            f'[sources.{source}.dlt_ops.rule_exemptions]\n{rule} = "vendor SDK phones home at import"\n'
        )

    def test_exemption_is_a_config_error(self, make_project):
        root = make_project(
            config=self._config("exempt_probe_api"),
            files={
                "exempt_probe/source/exempt_probe_api.py": HEALTHY_SOURCE.replace("healthy_api", "exempt_probe_api")
            },
        )
        errors = validate_sources(root)

        refusals = [e for e in errors if e.field == "rule_exemptions.import_safety"]
        assert [e.source_name for e in refusals] == ["exempt_probe_api"]
        assert not refusals[0].is_warning
        assert "cannot be exempted per source" in refusals[0].message
        assert "[dlt_ops.rules] import_safety = false" in refusals[0].message

    def test_exemption_does_not_restore_the_withheld_import(self, make_project):
        """The whole reason the middle state was broken: opting out per source
        must not look like it worked."""
        root = make_project(
            config=self._config("withheld_probe_api"),
            files={
                "withheld_probe/source/withheld_probe_api.py": DISK_WRITE_AT_IMPORT.replace(
                    "orders_api", "withheld_probe_api"
                ).replace('name="orders"', 'name="withheld_rows"')
            },
        )
        errors = validate_sources(root)

        assert [e.source_name for e in errors if e.field == "rule_exemptions.import_safety"] == ["withheld_probe_api"]
        # Still withheld, and the import error still names the working opt-out.
        withheld = [e for e in errors if e.field == "import"]
        assert [e.source_name for e in withheld] == ["withheld_probe_api"]
        assert "import_safety = false" in withheld[0].message

    def test_other_rules_stay_exemptable(self, make_project):
        """The refusal is scoped to this rule — it must not leak into the
        exemption mechanism every other rule shares."""
        root = make_project(
            config=self._config("other_probe_api", rule="schedule_required"),
            files={"other_probe/source/other_probe_api.py": HEALTHY_SOURCE.replace("healthy_api", "other_probe_api")},
        )
        errors = validate_sources(root)

        assert [e for e in errors if e.field.startswith("rule_exemptions.")] == []

    def test_refusal_survives_the_exemption_it_refuses(self):
        """It rides the always-on path for exactly this reason: routed through
        the rule framework, the exemption would suppress its own refusal."""
        ctx = ValidationContext(
            sources={},
            config={},
            project_root=Path("/proj"),
            introspected={},
            exemptions={"web_metrics": {"import_safety": "documented reason"}},
        )
        errors = validate_import_errors(ctx)

        assert [(e.source_name, e.field) for e in errors] == [("web_metrics", "rule_exemptions.import_safety")]


class TestValidateSurfacesFindings:
    def test_validate_reports_rule15_violation_as_error(self, make_project):
        root = make_project(
            config="""
            [dlt_ops]

            [sources.orders_api.dlt_ops]
            schedule = "@daily"
            """,
            files={"orders/source/orders_api.py": DISK_WRITE_AT_IMPORT},
        )
        errors = validate_sources(root)

        rule15 = [e for e in errors if e.field == "import_safety"]
        assert rule15, errors
        assert all(not e.is_warning for e in rule15)
        message = rule15[0].message
        assert "orders_api.py" in message  # names the module
        assert "disk-write" in message  # names the event type
        assert "canary.txt" in message  # names the offending target

    def test_validate_reports_import_failure(self, make_project):
        root = make_project(files={"broken/source/broken_api.py": RAISES_AT_IMPORT})
        errors = validate_sources(root)

        import_errors = [e for e in errors if e.field == "import"]
        assert [e.source_name for e in import_errors] == ["broken_api"]
        assert "boom at import" in import_errors[0].message

    def test_validate_reports_unparseable_module(self, make_project):
        """A module that does not parse never reaches the importer, so it used
        to vanish from discovery entirely — invisible to validation."""
        root = make_project(files={"mixed/source/broken.py": "def not valid python (("})
        errors = validate_sources(root)

        import_errors = [e for e in errors if e.field == "import"]
        assert [e.source_name for e in import_errors] == ["broken"]
        assert "broken.py" in import_errors[0].message
        assert "could not be parsed" in import_errors[0].message
        assert all(not e.is_warning for e in import_errors)


class TestValidateCliExitCode:
    """A project whose ONLY defect is the broken module must still fail the
    command — the finding is worthless if `validate` exits 0 beside it."""

    CONFIG = """
        [dlt_ops]
        default_destination = "duckdb"
        default_dataset = "raw"

        [sources.healthy_api.dlt_ops]
        schedule = "@daily"
    """
    # Fully rule-clean: typed columns, a model whose extra="forbid" derives the
    # canonical schema contract, named source.
    CLEAN_SOURCE = """
        import dlt
        import pydantic

        class Row(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra="forbid")

            id: int

        @dlt.resource(name="rows", columns=Row)
        def rows():
            yield {"id": 1}

        @dlt.source(name="healthy_api")
        def healthy_api_source():
            return rows
    """

    def _validate(self, root: Path):
        return CliRunner().invoke(cli, ["--root", str(root), "pipeline", "validate"])

    def test_clean_project_passes(self, make_project):
        """Guard for the test below: without the broken file this project is green,
        so a non-zero exit there can only come from the broken file."""
        root = make_project(config=self.CONFIG, files={"mixed/source/healthy_api.py": self.CLEAN_SOURCE})
        result = self._validate(root)
        assert result.exit_code == 0, result.output

    def test_unparseable_sibling_fails_the_run(self, make_project):
        root = make_project(
            config=self.CONFIG,
            files={
                "mixed/source/healthy_api.py": self.CLEAN_SOURCE,
                "mixed/source/broken.py": "def not valid python ((",
            },
        )
        result = self._validate(root)

        assert result.exit_code == 1, result.output
        assert "All sources validated successfully" not in result.output
        assert "broken.py" in result.output


class TestValidatorUnits:
    """Validator behavior over hand-built contexts (no sandbox runs)."""

    def _ctx(self, config: dict, **source_kwargs) -> ValidationContext:
        info = SourceInfo(
            name="web_metrics",
            pipeline_name="web",
            path=Path("/proj/web"),
            function_name="web_metrics_source",
            resources=("metrics",),
            module_stem="web_metrics",
            **source_kwargs,
        )
        return ValidationContext(
            sources={},
            config=config,
            project_root=Path("/proj"),
            introspected={"web_metrics": info},
        )

    def test_violation_error_names_module_event_and_target(self):
        ctx = self._ctx(
            {},
            import_violations=(ImportViolation(kind="network", event="socket.connect", target="('127.0.0.1', 9)"),),
        )
        errors = validate_import_safety(ctx)

        assert len(errors) == 1
        assert isinstance(errors[0], ValidationError)
        assert errors[0].source_name == "web_metrics"
        assert "network" in errors[0].message
        assert "web_metrics.py" in errors[0].message
        assert "socket.connect" in errors[0].message
        assert "127.0.0.1" in errors[0].message

    def test_validator_is_knob_free(self):
        """The [dlt_ops.rules] knob is applied by the rule framework
        (rule id `import_safety`), not by the validator: called directly it
        always reports. End-to-end knob-off behavior is covered by
        TestImportSafetyKnob above."""
        ctx = self._ctx(
            {"dlt_ops": {"rules": {"import_safety": False}}},
            import_violations=(ImportViolation(kind="network", event="socket.connect", target="x"),),
        )
        assert len(validate_import_safety(ctx)) == 1

    def test_rule_on_when_knob_missing(self):
        ctx = self._ctx(
            {"dlt_ops": {}},
            import_violations=(ImportViolation(kind="disk-write", event="open", target="/tmp/x"),),
        )
        assert len(validate_import_safety(ctx)) == 1

    def test_import_error_reported_regardless_of_knob(self):
        ctx = self._ctx(
            {"dlt_ops": {"rules": {"import_safety": False}}},
            import_error="module raised at import: RuntimeError: nope",
        )
        errors = validate_import_errors(ctx)
        # The import failure, plus the rule coverage that failure cost this run.
        assert [e.field for e in errors] == ["import", COVERAGE_FIELD]
        assert "nope" in errors[0].message


class TestCliNeverImportsOnReadOnlyVerbs:
    """`pipeline list` / `pipeline resources` are Phase-1-only: the canary
    side-effect file must not appear and the module must not hit sys.modules."""

    CONFIG = """
        [dlt_ops]

        [sources.listing_probe_api.dlt_ops]
        schedule = "@daily"
    """
    # Unique pipeline/module names so sys.modules state from other tests
    # can't mask a regression.
    SOURCE = DISK_WRITE_AT_IMPORT.replace("orders_api", "listing_probe_api").replace(
        'name="orders"', 'name="probe_rows"'
    )

    @pytest.fixture
    def project(self, make_project) -> Path:
        return make_project(
            config=self.CONFIG,
            files={"listing_probe/source/listing_probe_api.py": self.SOURCE},
        )

    def _assert_not_imported(self, project: Path) -> None:
        canary = project / "listing_probe" / "source" / "canary.txt"
        assert not canary.exists(), "read-only verb executed a source module (canary written)"
        assert f"{SOURCE_MODULE_NAMESPACE}.listing_probe.source.listing_probe_api" not in sys.modules

    def test_pipeline_list_does_not_import_sources(self, project):
        result = CliRunner().invoke(cli, ["--root", str(project), "pipeline", "list"])
        assert result.exit_code == 0, result.output
        assert "listing_probe_api" in result.output
        self._assert_not_imported(project)

    def test_pipeline_resources_does_not_import_sources(self, project):
        result = CliRunner().invoke(cli, ["--root", str(project), "pipeline", "resources", "-s", "listing_probe_api"])
        assert result.exit_code == 0, result.output
        assert "probe_rows" in result.output  # static resource list
        self._assert_not_imported(project)
