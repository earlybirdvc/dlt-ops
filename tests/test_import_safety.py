"""Rule 15 / two-phase discovery tests: Phase-2 sandbox, validators, CLI.

The sandbox fixtures are self-contained: the network fixture attempts a
localhost connect to the discard port (the ATTEMPT is the violation — no
real network, no dependency on connectivity), the disk-write fixture drops a
canary file next to itself, and the read fixture loads a sibling TOML.
"""

import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from dlt_ops import ValidationContext, ValidationError, validate_sources
from dlt_ops.cli.cli import cli
from dlt_ops.discovery import ImportViolation, SourceInfo, discover, discover_sources, introspect
from dlt_ops.discovery.phase2 import SOURCE_MODULE_NAMESPACE
from dlt_ops.discovery.validators.import_safety import validate_import_errors, validate_import_safety

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
        # The module itself imports fine (it swallows the refused connect):
        # the violation is reported, the source still enriched.
        assert info.is_introspected is True
        assert info.import_error is None

    def test_disk_write_at_import_flagged(self, make_project):
        root = make_project(files={"orders/source/orders_api.py": DISK_WRITE_AT_IMPORT})
        info = _two_phase(root)["orders_api"]

        write_violations = [v for v in info.import_violations if v.kind == "disk-write"]
        assert write_violations, info.import_violations
        assert any(v.target.endswith("canary.txt") for v in write_violations)
        assert info.is_introspected is True

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
        assert len(errors) == 1
        assert errors[0].field == "import"
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
