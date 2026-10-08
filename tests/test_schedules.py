"""Tests for declared schedules — ``dlt_ops._schedules`` and the ``[dlt_ops.schedules]`` table.

Each rejection case is built so that exactly one condition fails: an entry
rejected for the wrong reason would pass the test while leaving the intended
check unproven. ``match=`` pins the reason.
"""

import json
from pathlib import Path

import dlt
import pytest
from click.testing import CliRunner
from croniter import croniter

from dlt_ops import Schedule, SourceConfig, SourceInfo, ValidationContext
from dlt_ops._schedules import parse_declared_schedules, resolve_schedule
from dlt_ops.cli.cli import cli
from dlt_ops.config import ProjectConfigError, load_project_config
from dlt_ops.discovery.phase1 import discover
from dlt_ops.discovery.validators.config import validate_schedules
from dlt_ops.discovery.validators.platform_rules import validate_incremental_cursor_required
from dlt_ops.plugins import registry as registry_mod

VALID_CRON = "0 2 * * *"
INVALID_CRON = "0 2 30 2 *"  # February 30 never occurs

SOURCE_TEMPLATE = """
    import dlt

    @dlt.resource(name="rows")
    def rows():
        yield {{"id": 1}}

    @dlt.source(name="{name}")
    def {name}_source():
        return rows
"""


@pytest.fixture(autouse=True)
def _clean_plugin_registry():
    # Loading a project config installs [dlt_ops.plugins] into the process-wide registry.
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


def _config(schedules: dict[str, str], **sources: str) -> str:
    """A .dlt/config.toml body: one [dlt_ops.schedules] table and one schedule per source."""
    lines = ["[dlt_ops]", 'default_destination = "duckdb"', 'default_dataset = "raw"', "[dlt_ops.schedules]"]
    lines += [f'"{name}" = "{cron}"' for name, cron in schedules.items()]
    for source, schedule in sources.items():
        lines += [f"[sources.{source}.dlt_ops]", f'schedule = "{schedule}"']
    return "\n".join(lines) + "\n"


def _project(make_project, schedules: dict[str, str], **sources: str) -> Path:
    files = {f"{name}/source/{name}.py": SOURCE_TEMPLATE.format(name=name) for name in sources}
    return make_project(config=_config(schedules, **sources), files=files)


def _source_info(name: str, schedule: str | None = None, source_fn=None) -> SourceInfo:
    return SourceInfo(
        name=name,
        pipeline_name=name,
        path=Path("/proj") / name,
        function_name=f"{name}_source",
        resources=("rows",),
        module_stem=name,
        config=SourceConfig(schedule=schedule) if schedule else None,
        source_fn=source_fn,
    )


class TestAcceptedSchedules:
    def test_absent_table_is_empty(self):
        assert parse_declared_schedules(None) == {}

    def test_valid_entries_are_returned_as_a_new_dict(self):
        raw = {"@daily0200": VALID_CRON, "@every15": "*/15 * * * *"}
        parsed = parse_declared_schedules(raw)
        assert parsed == raw
        assert parsed is not raw

    @pytest.mark.parametrize("cron", ["0 2 * MAR *", "0 2 * * FRI", "0 2 * APR,MAR *"])
    def test_month_and_day_names_containing_r_are_not_the_random_field(self, cron):
        assert parse_declared_schedules({"@named": cron}) == {"@named": cron}

    def test_name_with_underscore_dot_and_dash(self):
        assert parse_declared_schedules({"@a_b.c-d": VALID_CRON}) == {"@a_b.c-d": VALID_CRON}


class TestRejectedSchedules:
    """One test per condition; every other condition holds in each case."""

    @pytest.mark.parametrize("raw", [["@daily0200", VALID_CRON], VALID_CRON])
    def test_not_a_table(self, raw):
        with pytest.raises(ValueError, match="must be a table"):
            parse_declared_schedules(raw)

    @pytest.mark.parametrize("name", ["daily0200", "@daily 0200", "@daily/0200", "@"])
    def test_name_outside_the_allowed_characters(self, name):
        with pytest.raises(ValueError, match="schedule name must be '@' followed by"):
            parse_declared_schedules({name: VALID_CRON})

    @pytest.mark.parametrize("name", ["@daily", "@manual"])
    def test_builtin_name_cannot_be_redefined(self, name):
        with pytest.raises(ValueError, match="is a built-in schedule"):
            parse_declared_schedules({name: VALID_CRON})

    def test_cron_not_a_string(self):
        with pytest.raises(ValueError, match="must be a string, got int"):
            parse_declared_schedules({"@daily0200": 2})

    @pytest.mark.parametrize(
        ("cron", "count"),
        [("0 2 * *", 4), ("0 2 * * * *", 6), ("0 0 2 * * * 2030", 7), ("@daily", 1)],
    )
    def test_cron_without_exactly_five_fields(self, cron, count):
        with pytest.raises(ValueError, match=f"exactly 5 fields, got {count}"):
            parse_declared_schedules({"@named": cron})

    def test_six_field_form_is_otherwise_valid_for_croniter(self):
        """Proves the six-field case reaches only the field-count check."""
        assert croniter.is_valid("0 2 * * * *", strict=True)

    @pytest.mark.parametrize("cron", ["R 2 * * *", "0 R * * *", "r 2 * * *", "R(1-5) 2 * * *", "R/15 2 * * *"])
    def test_random_field(self, cron):
        assert croniter.is_valid(cron, strict=True)  # croniter accepts it, so only the R check rejects it
        with pytest.raises(ValueError, match="random 'R' field"):
            parse_declared_schedules({"@named": cron})

    @pytest.mark.parametrize("cron", ["61 2 * * *", "*/0 * * * *", "0 2 30 2 *"])
    def test_cron_croniter_rejects(self, cron):
        """Out-of-range value, step 0, and an impossible date (Feb 30)."""
        with pytest.raises(ValueError, match="is not a valid cron expression"):
            parse_declared_schedules({"@named": cron})


class TestResolveSchedule:
    def test_builtin_returns_a_plain_string(self):
        resolved = resolve_schedule("@daily", {})
        assert resolved == "@daily"
        assert type(resolved) is str

    def test_enum_member_returns_a_plain_string(self):
        resolved = resolve_schedule(Schedule.HOURLY, {})
        assert resolved == "@hourly"
        assert type(resolved) is str

    def test_declared_name(self):
        resolved = resolve_schedule("@daily0200", {"@daily0200": VALID_CRON})
        assert resolved == "@daily0200"
        assert type(resolved) is str

    def test_unknown_name_lists_builtins_then_declared(self):
        declared = {"@zeta": VALID_CRON, "@alpha": VALID_CRON}
        valid = [s.value for s in Schedule] + ["@alpha", "@zeta"]
        with pytest.raises(ValueError) as exc_info:
            resolve_schedule("@nope", declared)
        assert str(exc_info.value) == f"Invalid schedule '@nope'. Valid: {valid}"

    def test_message_matches_the_enum_parser_without_declared_names(self):
        with pytest.raises(ValueError) as declared_exc:
            resolve_schedule("daily", {})
        with pytest.raises(ValueError) as enum_exc:
            Schedule.from_string("daily")
        assert str(declared_exc.value) == str(enum_exc.value)


class TestLoadProjectConfig:
    def test_absent_table_is_empty(self, make_project):
        assert load_project_config(make_project()).schedules == {}

    def test_declared_schedules_are_loaded(self, make_project):
        root = make_project(config='[dlt_ops]\n[dlt_ops.schedules]\n"@daily0200" = "0 2 * * *"\n')
        config = load_project_config(root)
        assert config.schedules == {"@daily0200": "0 2 * * *"}
        assert config.unknown_keys == ()

    def test_invalid_entry_fails_loading(self, make_project):
        root = make_project(config='[dlt_ops]\n[dlt_ops.schedules]\n"@daily0200" = "61 2 * * *"\n')
        with pytest.raises(ProjectConfigError, match=r"\[dlt_ops\.schedules\]: .*not a valid cron expression"):
            load_project_config(root)


class TestSourceConfig:
    def test_enum_member_is_stored_as_a_plain_string(self):
        schedule = SourceConfig(schedule=Schedule.DAILY).schedule
        assert schedule == "@daily"
        assert type(schedule) is str
        assert f"{schedule}" == "@daily"


class TestDiscover:
    def test_declared_schedule_is_attached_to_the_source(self, make_project):
        root = _project(make_project, {"@daily0200": VALID_CRON}, orders_api="@daily0200")
        assert discover(root)["orders_api"].config.schedule == "@daily0200"

    def test_invalid_table_fails_discovery(self, make_project):
        root = _project(make_project, {"@daily0200": INVALID_CRON}, orders_api="@daily")
        with pytest.raises(ProjectConfigError, match=r"\[dlt_ops\.schedules\]"):
            discover(root)

    def test_unknown_schedule_leaves_the_source_without_config(self, make_project):
        root = _project(make_project, {"@daily0200": VALID_CRON}, orders_api="@nope")
        assert discover(root)["orders_api"].config is None


class TestValidateSchedulesRule:
    @staticmethod
    def _ctx(cron: str, schedule: str) -> ValidationContext:
        config = {
            "dlt_ops": {"schedules": {"@daily0200": cron}},
            "sources": {"orders_api": {"dlt_ops": {"schedule": schedule}}},
        }
        sources = {"orders_api": _source_info("orders_api")}
        return ValidationContext(sources=sources, config=config, project_root=Path("/proj"))

    def test_declared_name_is_accepted(self):
        assert validate_schedules(self._ctx(VALID_CRON, "@daily0200")) == []

    def test_unknown_name_is_reported_with_the_declared_names(self):
        findings = validate_schedules(self._ctx(VALID_CRON, "@nope"))
        assert [(f.source_name, f.field) for f in findings] == [("orders_api", "schedule")]
        assert "Invalid schedule '@nope'" in findings[0].message
        assert "'@daily0200'" in findings[0].message

    def test_invalid_table_is_one_project_finding(self):
        findings = validate_schedules(self._ctx(INVALID_CRON, "@daily"))
        assert len(findings) == 1
        assert findings[0].source_name == "dlt_ops.schedules"
        assert findings[0].field == "schedules"
        assert findings[0].message.startswith("[dlt_ops.schedules]: ")
        assert findings[0].is_warning is False


def test_declared_schedule_is_in_scope_of_incremental_cursor_required():
    """Only @manual is out of scope; a declared name recurs like a built-in one."""

    @dlt.resource(name="rows")
    def rows():
        yield {"id": 1}

    info = _source_info("orders_api", "@daily0200", source_fn=lambda: dlt.source(lambda: rows, name="orders_api")())
    ctx = ValidationContext(sources={"orders_api": info}, config={}, project_root=Path("/proj"))
    findings = validate_incremental_cursor_required(ctx)
    assert [f.field for f in findings] == ["incremental.rows"]
    assert "every @daily0200 run" in findings[0].message


class TestCli:
    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_validate_reports_an_unknown_schedule(self, runner, make_project):
        root = _project(make_project, {"@daily0200": VALID_CRON}, orders_api="@nope")
        result = runner.invoke(cli, ["--root", str(root), "pipeline", "validate", "--json"])
        assert result.exit_code == 1, result.output
        schedule_findings = [f for f in json.loads(result.stdout) if f["field"] == "schedule"]
        assert [f["source"] for f in schedule_findings] == ["orders_api"]
        assert "Invalid schedule '@nope'" in schedule_findings[0]["message"]

    def test_validate_accepts_a_declared_schedule(self, runner, make_project):
        """The counterpart of the unknown-schedule case: only the schedule value differs."""
        root = _project(make_project, {"@daily0200": VALID_CRON}, orders_api="@daily0200")
        result = runner.invoke(cli, ["--root", str(root), "pipeline", "validate", "--json"])
        assert [f for f in json.loads(result.stdout) if f["field"] == "schedule"] == []

    @pytest.mark.parametrize(
        "argv",
        [
            ["list"],
            ["list", "--json"],
            ["validate"],
            ["validate", "--json"],
            ["resources", "--json"],
            ["run", "--source", "orders_api"],
            ["clean", "--source", "orders_api"],
            ["reconcile", "--all"],
            ["status"],
        ],
        ids=lambda argv: " ".join(argv),
    )
    def test_invalid_table_is_a_clean_error(self, runner, make_project, argv):
        """Every verb resolves the project root first, and that step rejects the table."""
        root = _project(make_project, {"@daily0200": INVALID_CRON}, orders_api="@daily")
        result = runner.invoke(cli, ["--root", str(root), "pipeline", *argv])
        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit)  # no traceback
        assert "Error: [dlt_ops.schedules]" in result.stderr

    def test_list_filters_by_a_declared_schedule(self, runner, make_project):
        root = _project(make_project, {"@daily0200": VALID_CRON}, orders_api="@daily0200", events_api="@daily")
        result = runner.invoke(cli, ["--root", str(root), "pipeline", "list", "--schedule", "@daily0200"])
        assert result.exit_code == 0, result.output
        assert "Found 1 source(s)" in result.output
        assert "orders_api" in result.output
        assert "events_api" not in result.output

    def test_list_keeps_columns_aligned_for_a_long_declared_name(self, runner, make_project):
        long_name = "@weekdays_0600_utc"  # longer than every built-in name
        root = _project(make_project, {long_name: VALID_CRON}, orders_api=long_name, events_api="@daily")
        result = runner.invoke(cli, ["--root", str(root), "pipeline", "list"])
        assert result.exit_code == 0, result.output
        rows = [line for line in result.output.splitlines() if line.startswith(("orders_api", "events_api"))]
        assert len(rows) == 2
        assert len({row.rindex(" ") for row in rows}) == 1  # the resource counts start in the same column

    def test_list_rejects_an_unknown_filter_with_the_declared_names(self, runner, make_project):
        root = _project(make_project, {"@daily0200": VALID_CRON}, orders_api="@daily0200")
        result = runner.invoke(cli, ["--root", str(root), "pipeline", "list", "--schedule", "@nope"])
        assert result.exit_code == 1
        assert "Invalid schedule '@nope'" in result.output
        assert "'@daily0200'" in result.output

    def test_list_json_shows_the_declared_schedule(self, runner, make_project):
        root = _project(make_project, {"@daily0200": VALID_CRON}, orders_api="@daily0200")
        result = runner.invoke(cli, ["--root", str(root), "pipeline", "list", "--json"])
        assert result.exit_code == 0, result.output
        assert [(s["name"], s["schedule"]) for s in json.loads(result.stdout)] == [("orders_api", "@daily0200")]
