import attrs
import pytest

from dlt_ops.config import (
    ProjectConfig,
    ProjectConfigError,
    ProjectConfigParseError,
    ProjectRootNotFoundError,
    UnresolvedDatasetError,
    UnresolvedDestinationError,
    find_project_root,
    load_project_config,
    load_raw_config,
    resolve_dataset,
    resolve_destination,
)
from dlt_ops.discovery.models import Schedule, SourceConfig
from dlt_ops.plugins import registry as registry_mod


@pytest.fixture(autouse=True)
def _clean_plugin_registry():
    # load_project_config installs [dlt_ops.plugins] into the process-wide
    # registry; keep that state from leaking across tests.
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


class TestFindProjectRoot:
    def test_start_dir_is_root(self, make_project):
        root = make_project()
        assert find_project_root(start=root) == root

    def test_walks_up_from_nested_start(self, make_project):
        root = make_project()
        nested = root / "pipelines" / "deep" / "deeper"
        nested.mkdir(parents=True)
        assert find_project_root(start=nested) == root

    def test_resolves_from_cwd_by_default(self, make_project, monkeypatch):
        root = make_project()
        nested = root / "somewhere"
        nested.mkdir()
        monkeypatch.chdir(nested)
        assert find_project_root() == root

    def test_nearest_marker_wins(self, make_project):
        outer = make_project(name="outer")
        inner = make_project(name="outer/inner")
        assert find_project_root(start=inner) == inner
        assert find_project_root(start=outer) == outer

    def test_explicit_wins_over_start(self, make_project):
        chosen = make_project(name="chosen")
        other = make_project(name="other")
        assert find_project_root(start=other, explicit=chosen) == chosen

    def test_explicit_non_project_raises_with_init_hint(self, tmp_path):
        with pytest.raises(ProjectRootNotFoundError, match="dlt-ops init"):
            find_project_root(explicit=tmp_path)

    def test_explicit_is_never_widened_to_parents(self, make_project):
        root = make_project()
        non_project = root / "pipelines"
        non_project.mkdir()
        # Explicit means "this exact dir", even though its parent qualifies.
        with pytest.raises(ProjectRootNotFoundError):
            find_project_root(explicit=non_project)

    def test_miss_raises_with_init_hint(self, tmp_path):
        with pytest.raises(ProjectRootNotFoundError, match="dlt-ops init"):
            find_project_root(start=tmp_path)

    def test_marker_without_dlt_ops_table_is_not_a_project(self, make_project):
        root = make_project(
            config="""
            [sources.demo_api.dlt_ops]
            schedule = "@daily"
            """
        )
        with pytest.raises(ProjectRootNotFoundError, match="dlt-ops init"):
            find_project_root(start=root)

    def test_broken_toml_is_hard_error_not_a_miss(self, make_project):
        root = make_project(config="not valid toml [[[")
        with pytest.raises(ProjectConfigParseError):
            find_project_root(start=root)

    def test_broken_toml_on_explicit_root_is_hard_error(self, make_project):
        root = make_project(config="not valid toml [[[")
        with pytest.raises(ProjectConfigParseError):
            find_project_root(explicit=root)

    def test_errors_share_a_catchable_base(self):
        assert issubclass(ProjectRootNotFoundError, ProjectConfigError)
        assert issubclass(ProjectConfigParseError, ProjectConfigError)
        assert issubclass(UnresolvedDestinationError, ProjectConfigError)
        assert issubclass(UnresolvedDatasetError, ProjectConfigError)


class TestLoadRawConfig:
    def test_missing_file_is_empty_dict(self, tmp_path):
        assert load_raw_config(tmp_path) == {}

    def test_broken_toml_is_hard_error(self, make_project):
        root = make_project(config="not valid toml [[[")
        with pytest.raises(ProjectConfigParseError):
            load_raw_config(root)

    def test_returns_whole_document(self, make_project):
        root = make_project(
            config="""
            [dlt_ops]
            default_destination = "duckdb"

            [sources.demo_api.dlt_ops]
            schedule = "@daily"
            """
        )
        raw = load_raw_config(root)
        assert raw["dlt_ops"]["default_destination"] == "duckdb"
        assert raw["sources"]["demo_api"]["dlt_ops"]["schedule"] == "@daily"


class TestLoadProjectConfig:
    def test_parses_defaults_rules_and_plugins(self, make_project):
        root = make_project(
            config="""
            [dlt_ops]
            default_destination = "duckdb"
            default_dataset = "raw_data"

            [dlt_ops.rules]
            import_safety = false

            [dlt_ops.plugins.destination]
            snowflake = "acme_dlt_snowflake"
            """
        )
        config = load_project_config(root)
        assert config.default_destination == "duckdb"
        assert config.default_dataset == "raw_data"
        assert config.rules == {"import_safety": False}
        assert config.plugins == {"destination": {"snowflake": "acme_dlt_snowflake"}}
        assert config.unknown_keys == ()

    def test_unknown_plugin_axis_is_a_config_error(self, make_project):
        root = make_project(
            config="""
            [dlt_ops]
            [dlt_ops.plugins.flux_capacitor]
            x = "y"
            """
        )
        with pytest.raises(ProjectConfigError, match=r"\[dlt_ops\.plugins\].*flux_capacitor"):
            load_project_config(root)

    def test_empty_table_yields_defaults(self, make_project):
        config = load_project_config(make_project())
        assert config.default_destination is None
        assert config.default_dataset is None
        assert config.rules == {}
        assert config.plugins == {}
        assert config.unknown_keys == ()

    def test_reserved_future_keys_pass_through_raw_without_warning(self, make_project):
        root = make_project(
            config="""
            [dlt_ops]
            load_timestamp_column = "loaded_at"
            injected_columns = ["region_id"]
            """
        )
        config = load_project_config(root)
        assert config.unknown_keys == ()
        assert config.raw["load_timestamp_column"] == "loaded_at"
        assert config.raw["injected_columns"] == ["region_id"]

    def test_unknown_keys_collected_not_raised(self, make_project):
        root = make_project(
            config="""
            [dlt_ops]
            default_destination = "duckdb"
            default_datset = "typo"
            wat = 1
            """
        )
        config = load_project_config(root)
        assert config.unknown_keys == ("default_datset", "wat")
        assert config.default_destination == "duckdb"

    def test_missing_table_raises_with_init_hint(self, tmp_path, make_project):
        root = make_project(config="[sources]\n")
        with pytest.raises(ProjectRootNotFoundError, match="dlt-ops init"):
            load_project_config(root)
        with pytest.raises(ProjectRootNotFoundError):
            load_project_config(tmp_path)

    def test_frozen(self):
        config = ProjectConfig(default_destination="duckdb")
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            config.default_destination = "postgres"


class TestRequireDestinationAdapter:
    def test_absent_key_defaults_false(self, make_project):
        config = load_project_config(make_project())
        assert config.require_destination_adapter is False

    def test_true_parses(self, make_project):
        root = make_project(config="[dlt_ops]\nrequire_destination_adapter = true\n")
        config = load_project_config(root)
        assert config.require_destination_adapter is True
        assert config.unknown_keys == ()

    def test_false_parses(self, make_project):
        root = make_project(config="[dlt_ops]\nrequire_destination_adapter = false\n")
        assert load_project_config(root).require_destination_adapter is False

    def test_non_bool_value_is_lenient_false(self, make_project):
        root = make_project(config='[dlt_ops]\nrequire_destination_adapter = "yes"\n')
        assert load_project_config(root).require_destination_adapter is False


class TestDestinationResolution:
    def test_source_override_beats_project_default(self):
        source = SourceConfig(schedule=Schedule.DAILY, destination="duckdb")
        project = ProjectConfig(default_destination="postgres")
        assert resolve_destination(source, project) == "duckdb"

    def test_project_default_when_no_override(self):
        source = SourceConfig(schedule=Schedule.DAILY)
        project = ProjectConfig(default_destination="postgres")
        assert resolve_destination(source, project) == "postgres"

    def test_none_source_config_uses_project_default(self):
        project = ProjectConfig(default_destination="postgres")
        assert resolve_destination(None, project) == "postgres"

    def test_unresolved_names_both_config_keys(self):
        with pytest.raises(UnresolvedDestinationError) as exc:
            resolve_destination(SourceConfig(schedule=Schedule.DAILY), ProjectConfig())
        message = str(exc.value)
        assert "[dlt_ops].default_destination" in message
        assert "[sources.<section>.dlt_ops].destination" in message

    def test_empty_string_override_falls_through(self):
        source = SourceConfig(schedule=Schedule.DAILY, destination="")
        project = ProjectConfig(default_destination="postgres")
        assert resolve_destination(source, project) == "postgres"


class TestDatasetResolution:
    def test_source_override_beats_project_default(self):
        source = SourceConfig(schedule=Schedule.DAILY, dataset="scratch")
        project = ProjectConfig(default_dataset="raw_data")
        assert resolve_dataset(source, project) == "scratch"

    def test_project_default_when_no_override(self):
        source = SourceConfig(schedule=Schedule.DAILY)
        project = ProjectConfig(default_dataset="raw_data")
        assert resolve_dataset(source, project) == "raw_data"

    def test_none_source_config_uses_project_default(self):
        project = ProjectConfig(default_dataset="raw_data")
        assert resolve_dataset(None, project) == "raw_data"

    def test_unresolved_names_both_config_keys(self):
        with pytest.raises(UnresolvedDatasetError) as exc:
            resolve_dataset(SourceConfig(schedule=Schedule.DAILY), ProjectConfig())
        message = str(exc.value)
        assert "[dlt_ops].default_dataset" in message
        assert "[sources.<section>.dlt_ops].dataset" in message
