"""Tests for declared schedules — ``dlt_ops._schedules`` and the ``[dlt_ops.schedules]`` table.

Each rejection case is built so that exactly one condition fails: an entry
rejected for the wrong reason would pass the test while leaving the intended
check unproven. ``match=`` pins the reason.
"""

import pytest
from croniter import croniter

from dlt_ops import Schedule
from dlt_ops._schedules import parse_declared_schedules, resolve_schedule
from dlt_ops.config import ProjectConfigError, load_project_config
from dlt_ops.plugins import registry as registry_mod

VALID_CRON = "0 2 * * *"


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
    @pytest.fixture(autouse=True)
    def _clean_plugin_registry(self):
        # load_project_config installs [dlt_ops.plugins] into the process-wide registry.
        registry_mod._reset_for_tests()
        yield
        registry_mod._reset_for_tests()

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
