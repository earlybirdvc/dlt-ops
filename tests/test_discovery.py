import logging
import sys
from pathlib import Path
from textwrap import dedent

import pytest

from dlt_ops import (
    Schedule,
    SourceConfig,
    SourceInfo,
    ValidationContext,
    ValidationError,
    discover_sources,
)
from dlt_ops.config import ProjectConfigParseError
from dlt_ops.discovery.phase1 import _parse_source_config, _scan_module, discover
from dlt_ops.discovery.phase2 import SOURCE_MODULE_NAMESPACE
from dlt_ops.discovery.scanner import get_sources_by_schedule
from dlt_ops.discovery.validators.resources import validate_no_resource_overlap

EVENTS_SOURCE = """
    import dlt

    @dlt.resource(name="events")
    def events():
        yield {"id": 1}

    @dlt.source(name="events_api")
    def events_api_source():
        return events
"""

ORDERS_SOURCE = """
    import dlt

    @dlt.resource(name="orders")
    def orders():
        yield {"id": 1}

    @dlt.source(name="orders_api")
    def orders_api_source():
        return orders
"""

CHECKPOINTED_SHOP_SOURCE = """
    import dlt

    from dlt_ops import with_checkpoints

    @dlt.resource(name="orders")
    @with_checkpoints(cursor_field="ts")
    def orders(ts=dlt.sources.incremental("ts")):
        yield {"ts": "2026-01-01"}

    @dlt.source(name="shop_api")
    def shop_api_source():
        return orders
"""


class TestSchedule:
    def test_from_string_valid(self):
        assert Schedule.from_string("@hourly") == Schedule.HOURLY
        assert Schedule.from_string("@2hourly") == Schedule.TWO_HOURLY
        assert Schedule.from_string("@daily") == Schedule.DAILY
        assert Schedule.from_string("@weekly") == Schedule.WEEKLY
        assert Schedule.from_string("@monthly") == Schedule.MONTHLY
        assert Schedule.from_string("@manual") == Schedule.MANUAL

    def test_from_string_invalid(self):
        with pytest.raises(ValueError) as exc:
            Schedule.from_string("invalid")
        assert "Invalid schedule" in str(exc.value)

    def test_from_string_without_at(self):
        with pytest.raises(ValueError):
            Schedule.from_string("daily")


class TestSourceConfig:
    def test_default_values(self):
        config = SourceConfig(schedule=Schedule.DAILY)
        assert config.schedule == Schedule.DAILY
        assert config.destination is None
        assert config.dataset is None
        assert config.airflow_var is None

    def test_custom_values(self):
        config = SourceConfig(
            schedule=Schedule.HOURLY,
            destination="duckdb",
            dataset="legacy_export",
            airflow_var="my-api-key",
        )
        assert config.schedule == Schedule.HOURLY
        assert config.destination == "duckdb"
        assert config.dataset == "legacy_export"
        assert config.airflow_var == "my-api-key"

    def test_carries_no_plugin_owned_secret_keys(self):
        """Core's model holds no backend trigger key — plugins read raw config.

        `airflow_var_key` used to sit here with an Airflow default that nothing
        read; the Airflow backend has always taken it off the raw ext table.
        """
        assert not hasattr(SourceConfig(schedule=Schedule.DAILY), "airflow_var_key")

    def test_immutable(self):
        config = SourceConfig(schedule=Schedule.DAILY)
        with pytest.raises(AttributeError):
            config.schedule = Schedule.HOURLY

    def test_is_schema_contract_evolve_true_for_non_empty_reason(self):
        config = SourceConfig(
            schedule=Schedule.HOURLY,
            schema_contract_evolve_reason="Provider ships nullable additive fields.",
        )
        assert config.is_schema_contract_evolve is True

    def test_is_schema_contract_evolve_false_when_absent(self):
        assert SourceConfig(schedule=Schedule.HOURLY).is_schema_contract_evolve is False

    def test_is_schema_contract_evolve_false_for_empty_string(self):
        assert (
            SourceConfig(schedule=Schedule.HOURLY, schema_contract_evolve_reason="").is_schema_contract_evolve is False
        )

    def test_is_schema_contract_evolve_false_for_whitespace_only(self):
        assert (
            SourceConfig(schedule=Schedule.HOURLY, schema_contract_evolve_reason="   \n\t").is_schema_contract_evolve
            is False
        )

    def test_is_schema_contract_evolve_does_not_raise_on_non_string(self):
        """TOML values can be int/bool/list if hand-authored wrong. The scanner
        passes them through untouched (`ext.get("schema_contract_evolve_reason")`),
        so the property must not raise AttributeError from `.strip()` on non-str.
        Non-string values are treated as absence.
        """
        for bogus in (42, True, ["reason"], {"why": "..."}, 3.14):
            config = SourceConfig(
                schedule=Schedule.HOURLY,
                schema_contract_evolve_reason=bogus,  # type: ignore[arg-type]
            )
            assert config.is_schema_contract_evolve is False


class TestSourceInfo:
    def test_basic_creation(self):
        def dummy_source():
            pass

        info = SourceInfo(
            name="test",
            pipeline_name="test_pipeline",
            path=Path("/test"),
            function_name="test_source",
            source_fn=dummy_source,
            resources=("res1", "res2"),
            module_stem="test",
        )
        assert info.name == "test"
        assert info.pipeline_name == "test_pipeline"
        assert info.config_section == "test"  # property derived from name
        assert info.config is None
        assert info.module_stem == "test"
        assert info.is_introspected is True
        assert info.source_fn is dummy_source

    def test_phase1_record_has_no_source_fn(self):
        """A Phase-1 (static) record raises on source_fn access — consumers
        that execute sources must go through Phase 2 first."""
        info = SourceInfo(
            name="test",
            pipeline_name="test_pipeline",
            path=Path("/test"),
            function_name="test_source",
            resources=("res1",),
            module_stem="test",
        )
        assert info.is_introspected is False
        assert info.import_error is None
        assert info.import_violations == ()
        with pytest.raises(RuntimeError, match="no source_fn"):
            _ = info.source_fn

    def test_with_config(self):
        def dummy_source():
            pass

        config = SourceConfig(schedule=Schedule.DAILY)
        info = SourceInfo(
            name="test",
            pipeline_name="test_pipeline",
            path=Path("/test"),
            function_name="test_source",
            source_fn=dummy_source,
            resources=("res1",),
            module_stem="test",
            config=config,
        )
        assert info.config == config
        assert info.config.schedule == Schedule.DAILY
        assert info.config_section == "test"


class TestDiscoverSourcesProjectTree:
    """discover_sources against neutral tmp-path project trees."""

    @pytest.fixture
    def project_root(self, make_project):
        return make_project(
            config="""
            [dlt_ops]
            default_destination = "duckdb"

            [sources.events_api.dlt_ops]
            schedule = "@daily"

            [sources.orders_api.dlt_ops]
            schedule = "@weekly"
            dataset = "orders_raw"
            """,
            files={
                "events/source/events_api.py": EVENTS_SOURCE,
                "orders/source/orders_api.py": ORDERS_SOURCE,
            },
        )

    def test_discover_sources_finds_sources(self, project_root):
        sources = discover_sources(project_root)

        assert set(sources) == {"events_api", "orders_api"}
        for name, src in sources.items():
            assert src.name == name
            assert src.path.exists()
            assert src.function_name.endswith("_source")
            assert callable(src.source_fn)

        assert sources["events_api"].pipeline_name == "events"
        assert sources["events_api"].resources == ("events",)
        assert sources["events_api"].config.schedule == Schedule.DAILY
        assert sources["orders_api"].config.schedule == Schedule.WEEKLY
        assert sources["orders_api"].config.dataset == "orders_raw"

    def test_get_sources_by_schedule(self, project_root):
        sources = discover_sources(project_root)
        by_schedule = get_sources_by_schedule(sources)

        total = sum(len(lst) for lst in by_schedule.values())
        assert total == len(sources)
        assert [s.name for s in by_schedule[Schedule.DAILY]] == ["events_api"]
        assert [s.name for s in by_schedule[Schedule.WEEKLY]] == ["orders_api"]

    def test_source_without_config_section_has_no_config(self, make_project):
        root = make_project(files={"events/source/events_api.py": EVENTS_SOURCE})
        sources = discover_sources(root)
        assert sources["events_api"].config is None
        assert get_sources_by_schedule(sources)[Schedule.MANUAL][0].name == "events_api"

    def test_broken_config_toml_is_hard_error(self, make_project):
        root = make_project(
            config="not valid toml [[[",
            files={"events/source/events_api.py": EVENTS_SOURCE},
        )
        with pytest.raises(ProjectConfigParseError):
            discover_sources(root)


class TestPhase1Discover:
    """Phase-1 discover(): pure AST — zero project-code imports."""

    def test_lists_module_that_raises_at_import(self, make_project):
        """The whole point of Phase 1: a module whose body raises is still
        listed with correct name/pipeline/schedule/static resources, and is
        never imported."""
        root = make_project(
            config="""
            [dlt_ops]

            [sources.exploding_api.dlt_ops]
            schedule = "@daily"
            """,
            files={
                "exploding/source/exploding_api.py": """
                    import dlt

                    raise RuntimeError("boom at import")

                    @dlt.resource(name="rows")
                    def rows():
                        yield {"id": 1}

                    @dlt.source(name="exploding_api")
                    def exploding_api_source():
                        return rows
                    """,
            },
        )
        sources = discover(root)

        assert set(sources) == {"exploding_api"}
        info = sources["exploding_api"]
        assert info.pipeline_name == "exploding"
        assert info.config is not None and info.config.schedule == Schedule.DAILY
        assert info.resources == ("rows",)
        assert info.module_stem == "exploding_api"
        assert info.module_path == root / "exploding" / "source" / "exploding_api.py"
        assert info.decorator_name == "exploding_api"
        assert info.is_introspected is False
        assert f"{SOURCE_MODULE_NAMESPACE}.exploding.source.exploding_api" not in sys.modules

    def test_static_resources_union_module_and_resource_dir(self, make_project):
        """Static approximation: own-module @dlt.resource declarations plus
        resource/*.py siblings (a source may pull in either at runtime)."""
        root = make_project(
            files={
                "shop/source/shop_api.py": """
                    import dlt

                    @dlt.resource(name="orders")
                    def orders():
                        yield {}

                    @dlt.source(name="shop_api")
                    def shop_api_source():
                        return orders
                    """,
                "shop/resource/shared.py": """
                    import dlt

                    @dlt.resource(name="customers")
                    def customers():
                        yield {}
                    """,
            },
        )
        assert discover(root)["shop_api"].resources == ("orders", "customers")

    @pytest.fixture
    def unparseable_project(self, make_project) -> Path:
        return make_project(
            files={
                "mixed/source/broken.py": "def not valid python ((",
                "mixed/source/healthy_api.py": """
                    import dlt

                    @dlt.source(name="healthy_api")
                    def healthy_api_source():
                        return []
                    """,
            },
        )

    def test_unparseable_module_skipped_sibling_survives(self, unparseable_project):
        """Default discovery lists runnable sources only — a file that does not
        parse declares no source. The parse-free consumers (`pipeline list`, the
        orchestrator DAG factory) depend on this: neither may offer a task for a
        module nobody could read."""
        assert set(discover(unparseable_project)) == {"healthy_api"}

    def test_unparseable_module_surfaces_as_import_error_record(self, unparseable_project):
        """...but `validate` must not be blind to it: opted in, the same scan
        yields a placeholder carrying the reason as `import_error`, the always-on
        channel a module that raises at import already travels."""
        sources = discover(unparseable_project, include_unloadable=True)
        assert set(sources) == {"healthy_api", "broken"}

        broken = sources["broken"]
        assert broken.import_error is not None
        assert "could not be parsed" in broken.import_error
        assert "SyntaxError" in broken.import_error
        assert broken.is_introspected is False
        assert broken.pipeline_name == "mixed"
        assert broken.module_stem == "broken"
        assert broken.module_path == unparseable_project / "mixed" / "source" / "broken.py"
        # Nothing may call into a module that does not parse.
        assert broken.resources == ()
        assert broken.function_name == ""
        # The healthy sibling is untouched by the placeholder merge.
        assert sources["healthy_api"].import_error is None

    def test_unparseable_placeholder_never_displaces_a_real_source(self, make_project):
        """A broken file whose stem collides with a parsed source's config
        section keys off its pipeline dir instead of overwriting it."""
        root = make_project(
            files={
                "alpha/source/api.py": "def not valid python ((",
                "beta/source/beta_api.py": """
                    import dlt

                    @dlt.source(name="api")
                    def beta_api_source():
                        return []
                    """,
            },
        )
        sources = discover(root, include_unloadable=True)
        assert set(sources) == {"api", "alpha.api"}
        assert sources["api"].import_error is None
        assert sources["api"].pipeline_name == "beta"
        assert sources["alpha.api"].import_error is not None

    def test_name_falls_back_to_function_name_minus_suffix(self, make_project):
        root = make_project(
            files={
                "plain/source/plain_api.py": """
                    import dlt

                    @dlt.source
                    def plain_api_source():
                        return []
                    """,
            },
        )
        sources = discover(root)
        assert set(sources) == {"plain_api"}
        assert sources["plain_api"].decorator_name is None


class TestPipelineDirNaming:
    """Discovery excludes no directory by NAME. Only two structural rules
    decide: the '.'/'_' prefix, and a source/ dir holding a non-underscore .py.

    The regression: a hardcoded set excluded `common` and `logs` — private
    monorepo layout conventions in a package that is generic across every
    user's project — so a pipeline legitimately named either was invisible,
    with no warning anywhere.
    """

    SOURCE = """
        import dlt

        @dlt.resource(name="{name}_rows")
        def rows():
            yield {{"id": 1}}

        @dlt.source(name="{name}")
        def source_fn():
            return rows
    """

    @pytest.mark.parametrize("dirname", ["common", "logs"])
    def test_formerly_excluded_names_are_discovered(self, make_project, dirname):
        root = make_project(files={f"{dirname}/source/{dirname}_api.py": self.SOURCE.format(name=f"{dirname}_api")})
        sources = discover(root)

        assert set(sources) == {f"{dirname}_api"}
        assert sources[f"{dirname}_api"].pipeline_name == dirname

    @pytest.mark.parametrize("dirname", ["__pycache__", ".dlt", "_scratch", ".hidden"])
    def test_dot_and_underscore_prefixes_still_skipped(self, make_project, dirname):
        """The prefix rule already covered every entry the set duplicated —
        removing the set must not widen the scan to build artifacts."""
        root = make_project(files={f"{dirname}/source/probe_api.py": self.SOURCE.format(name="probe_api")})
        assert discover(root) == {}

    def test_skipped_directory_leaves_a_debug_trace(self, make_project, caplog):
        """Silent skipping is the underlying defect: a directory an operator
        believes is a pipeline must say why it was not one."""
        root = make_project(files={"almost/notsource/thing_api.py": self.SOURCE.format(name="thing_api")})

        with caplog.at_level(logging.DEBUG, logger="dlt_ops.discovery.phase1"):
            assert discover(root) == {}

        messages = [record.getMessage() for record in caplog.records]
        assert any("almost" in message and "no source/ subdirectory" in message for message in messages)

    def test_empty_source_dir_says_so(self, make_project, caplog):
        root = make_project(files={"almost/source/_private.py": self.SOURCE.format(name="private_api")})

        with caplog.at_level(logging.DEBUG, logger="dlt_ops.discovery.phase1"):
            assert discover(root) == {}

        assert any("no non-underscore .py file" in record.getMessage() for record in caplog.records)


class TestCheckpointDetection:
    """Static `uses_checkpoints`: terminal-name decorator match across the
    source's own module and its resource/*.py siblings."""

    def test_bare_decorator_in_resource_module(self, make_project):
        """A checkpointed shared resource marks every source in the pipeline
        dir — any of them may select it at runtime."""
        root = make_project(
            files={
                "events/source/events_api.py": EVENTS_SOURCE,
                "events/resource/shared.py": """
                    import dlt

                    from dlt_ops import with_checkpoints

                    @dlt.resource(name="customers")
                    @with_checkpoints(cursor_field="updated_at")
                    def customers(updated_at=dlt.sources.incremental("updated_at")):
                        yield {"updated_at": "2026-01-01"}
                    """,
            },
        )
        assert discover(root)["events_api"].uses_checkpoints is True

    def test_attribute_form_decorator(self, make_project):
        root = make_project(
            files={
                "events/source/events_api.py": EVENTS_SOURCE,
                "events/resource/shared.py": """
                    import dlt
                    import dlt_ops

                    @dlt.resource(name="customers")
                    @dlt_ops.with_checkpoints(cursor_field="updated_at")
                    def customers(updated_at=dlt.sources.incremental("updated_at")):
                        yield {"updated_at": "2026-01-01"}
                    """,
            },
        )
        assert discover(root)["events_api"].uses_checkpoints is True

    def test_decorator_in_source_module(self, make_project):
        root = make_project(files={"shop/source/shop_api.py": CHECKPOINTED_SHOP_SOURCE})
        assert discover(root)["shop_api"].uses_checkpoints is True

    def test_no_usage_is_false(self, make_project):
        root = make_project(
            files={
                "events/source/events_api.py": EVENTS_SOURCE,
                "events/resource/shared.py": """
                    import dlt

                    @dlt.resource(name="customers")
                    def customers():
                        yield {}
                    """,
            },
        )
        assert discover(root)["events_api"].uses_checkpoints is False

    def test_aliased_import_escapes_detection(self, make_project):
        """The reach of the name match: `import ... as wc` hides the terminal
        name, so the static flag stays False; the runtime typed error at
        checkpoint entry remains the backstop for such usage."""
        root = make_project(
            files={
                "shop/source/shop_api.py": """
                    import dlt

                    from dlt_ops import with_checkpoints as wc

                    @dlt.resource(name="orders")
                    @wc(cursor_field="ts")
                    def orders(ts=dlt.sources.incremental("ts")):
                        yield {"ts": "2026-01-01"}

                    @dlt.source(name="shop_api")
                    def shop_api_source():
                        return orders
                    """,
            },
        )
        assert discover(root)["shop_api"].uses_checkpoints is False

    def test_flag_survives_phase2_introspection(self, make_project):
        """Phase-2 enrichment rebuilds records (source_fn, live resources) —
        the Phase-1 checkpoint flag must ride along."""
        root = make_project(files={"shop/source/shop_api.py": CHECKPOINTED_SHOP_SOURCE})
        info = discover_sources(root)["shop_api"]
        assert info.is_introspected is True
        assert info.resources == ("orders",)
        assert info.uses_checkpoints is True


class TestFilePathImportMechanics:
    """Source modules load by file path under synthetic module names —
    no sys.path mutation, no package-name assumptions about the root."""

    def test_two_source_modules_and_shared_resource_import(self, make_project):
        root = make_project(
            files={
                "web_events/source/page_views.py": """
                    import dlt

                    from ..resource.shared import shared_rows

                    @dlt.source(name="page_views")
                    def page_views_source():
                        return shared_rows

                    """,
                "web_events/source/clicks.py": """
                    import dlt

                    from ..resource.shared import shared_rows

                    @dlt.resource(name="clicks")
                    def clicks():
                        yield {"id": 2}

                    @dlt.source(name="clicks_api")
                    def clicks_api_source():
                        return clicks

                    """,
                "web_events/resource/shared.py": """
                    import dlt

                    @dlt.resource(name="shared_rows")
                    def shared_rows():
                        yield {"id": 1}

                    """,
            },
        )

        sys_path_before = list(sys.path)
        sources = discover_sources(root)

        assert set(sources) == {"page_views", "clicks_api"}
        assert sources["page_views"].resources == ("shared_rows",)
        assert sources["clicks_api"].resources == ("clicks",)
        # Both source modules of the pipeline registered under the synthetic namespace
        assert f"{SOURCE_MODULE_NAMESPACE}.web_events.source.page_views" in sys.modules
        assert f"{SOURCE_MODULE_NAMESPACE}.web_events.source.clicks" in sys.modules
        # File-path loading never touches sys.path
        assert sys.path == sys_path_before

    def test_underscore_sibling_module_importable_but_not_scanned(self, make_project):
        root = make_project(
            files={
                "catalog/source/catalog_api.py": """
                    import dlt

                    from ._helpers import RESOURCE_NAME

                    @dlt.resource(name=RESOURCE_NAME)
                    def items():
                        yield {"id": 1}

                    @dlt.source(name="catalog_api")
                    def catalog_api_source():
                        return items

                    """,
                "catalog/source/_helpers.py": 'RESOURCE_NAME = "items"\n',
            },
        )
        sources = discover_sources(root)
        assert set(sources) == {"catalog_api"}
        assert sources["catalog_api"].resources == ("items",)

    def test_same_pipeline_name_in_another_root_reloads(self, make_project):
        """Synthetic names collide across roots within one process; the loader
        must re-load from the new file instead of serving the cached module."""
        first = make_project(name="first", files={"dup_pipe/source/events_api.py": EVENTS_SOURCE})
        second = make_project(
            name="second",
            files={"dup_pipe/source/events_api.py": EVENTS_SOURCE.replace("events_api", "renamed_api")},
        )

        assert set(discover_sources(first)) == {"events_api"}
        assert set(discover_sources(second)) == {"renamed_api"}


class TestParseSourceConfig:
    """Tests for _parse_source_config with dlt_ops namespace."""

    def test_parses_schedule_from_dlt_ops(self):
        config = {
            "sources": {
                "my_source": {
                    "dlt_ops": {
                        "schedule": "@daily",
                        "airflow_var": "my-api-key",
                    },
                }
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is not None
        assert result.schedule == Schedule.DAILY
        assert result.airflow_var == "my-api-key"
        assert result.dataset is None

    def test_parses_dataset_from_dlt_ops(self):
        config = {
            "sources": {
                "db_export": {
                    "dlt_ops": {
                        "schedule": "@hourly",
                        "dataset": "legacy_export",
                        "airflow_var": "db-export-credentials",
                        "airflow_var_key": "credentials",
                    },
                }
            }
        }
        result = _parse_source_config(config, "db_export")
        assert result is not None
        assert result.schedule == Schedule.HOURLY
        assert result.dataset == "legacy_export"
        assert result.airflow_var == "db-export-credentials"
        # `airflow_var_key` is present in the ext table above and deliberately
        # not parsed: an unknown plugin key must pass through the core scan
        # untouched rather than fail it. The Airflow backend reads it raw
        # (tests/test_airflow_runtime.py::test_claims_with_custom_key).
        assert not hasattr(result, "airflow_var_key")

    def test_parses_destination_from_dlt_ops(self):
        config = {
            "sources": {
                "my_source": {
                    "dlt_ops": {
                        "schedule": "@daily",
                        "destination": "duckdb",
                    },
                }
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is not None
        assert result.destination == "duckdb"

    def test_destination_defaults_to_none(self):
        config = {
            "sources": {
                "my_source": {"dlt_ops": {"schedule": "@daily"}},
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is not None
        assert result.destination is None

    def test_returns_none_without_dlt_ops_section(self):
        config = {
            "sources": {
                "my_source": {
                    "some_dlt_native_key": "value",
                    # Missing dlt_ops section
                }
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is None

    def test_returns_none_without_schedule(self):
        config = {
            "sources": {
                "my_source": {
                    "dlt_ops": {
                        "dataset": "some_dataset",
                        # Missing schedule
                    }
                }
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is None

    def test_returns_none_for_invalid_schedule(self):
        config = {
            "sources": {
                "my_source": {
                    "dlt_ops": {
                        "schedule": "invalid",
                    }
                }
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is None

    def test_parses_injected_columns_from_dlt_ops(self):
        """The reconciler treats platform-injected keys as expected, not
        drift; the TOML list flows through as a tuple on SourceConfig."""
        config = {
            "sources": {
                "events_api": {
                    "dlt_ops": {
                        "schedule": "@hourly",
                        "injected_columns": ["region_id"],
                    }
                }
            }
        }
        result = _parse_source_config(config, "events_api")
        assert result is not None
        assert result.injected_columns == ("region_id",)

    def test_injected_columns_defaults_to_empty_tuple(self):
        """Absence collapses to empty tuple — the reconciler still ignores
        the universal `loaded_at`."""
        config = {
            "sources": {
                "my_source": {"dlt_ops": {"schedule": "@daily"}},
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is not None
        assert result.injected_columns == ()

    def test_injected_columns_filters_non_string_entries(self):
        """A mistyped `["region_id", 42, True]` degrades to just
        `("region_id",)` — a hand-authored TOML typo can't crash discovery."""
        config = {
            "sources": {
                "my_source": {
                    "dlt_ops": {
                        "schedule": "@daily",
                        "injected_columns": ["region_id", 42, True],
                    }
                }
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is not None
        assert result.injected_columns == ("region_id",)

    def test_injected_columns_non_list_value_collapses_to_empty(self):
        """A hand-authored `injected_columns = "region_id"` (string, not
        list) is bad TOML shape but must not crash — collapses to empty."""
        config = {
            "sources": {
                "my_source": {
                    "dlt_ops": {
                        "schedule": "@daily",
                        "injected_columns": "region_id",  # wrong shape
                    }
                }
            }
        }
        result = _parse_source_config(config, "my_source")
        assert result is not None
        assert result.injected_columns == ()


class TestValidation:
    def test_validation_error_creation(self):
        error = ValidationError(
            source_name="test",
            field="schedule",
            message="Missing schedule field",
        )
        assert error.source_name == "test"
        assert error.field == "schedule"
        assert error.is_warning is False

    def test_validation_warning(self):
        warning = ValidationError(
            source_name="orphan",
            field="config_section",
            message="Orphan config",
            is_warning=True,
        )
        assert warning.is_warning is True


class TestModuleScan:
    """Tests for the Phase-1 AST module scan (decorator + name parsing)."""

    def _scan(self, tmp_path, body: str):
        source_file = tmp_path / "test.py"
        source_file.write_text(dedent(body))
        return _scan_module(source_file)

    def test_source_with_explicit_name(self, tmp_path):
        scan = self._scan(
            tmp_path,
            """
            import dlt

            @dlt.source(name="my_api")
            def my_api_source():
                pass
            """,
        )
        assert scan.sources == (("my_api_source", "my_api"),)

    def test_source_without_name(self, tmp_path):
        scan = self._scan(
            tmp_path,
            """
            import dlt

            @dlt.source
            def my_api_source():
                pass
            """,
        )
        assert scan.sources == (("my_api_source", None),)

    def test_source_with_other_params(self, tmp_path):
        scan = self._scan(
            tmp_path,
            """
            import dlt

            @dlt.source(name="my_api", max_table_nesting=0)
            def my_api_source():
                pass
            """,
        )
        assert scan.sources == (("my_api_source", "my_api"),)

    def test_undecorated_function_not_listed(self, tmp_path):
        scan = self._scan(
            tmp_path,
            """
            def helper_source():
                pass
            """,
        )
        assert scan.sources == ()
        assert scan.resources == ()

    def test_invalid_file_raises_value_error(self, tmp_path):
        """The message is the bare reason — call sites supply the framing
        (`discover` turns it into a SourceInfo.import_error naming the file)."""
        source_file = tmp_path / "invalid.py"
        source_file.write_text("this is not valid python {{{")
        with pytest.raises(ValueError, match="SyntaxError"):
            _scan_module(source_file)

    def test_non_string_name_ignored(self, tmp_path):
        """@dlt.source(name=123) — non-string name is ignored, decorator name
        stays None (the decorator-name validator flags it)."""
        scan = self._scan(
            tmp_path,
            """
            import dlt

            @dlt.source(name=123)  # Invalid - name must be string
            def my_api_source():
                pass
            """,
        )
        assert scan.sources == (("my_api_source", None),)

    def test_resources_use_name_or_function_name_fallback(self, tmp_path):
        scan = self._scan(
            tmp_path,
            """
            import dlt

            @dlt.resource(name="named_rows")
            def rows_fn():
                yield {}

            @dlt.resource
            def bare_rows():
                yield {}
            """,
        )
        assert scan.resources == ("named_rows", "bare_rows")

    def test_with_checkpoints_detected_when_uncalled(self, tmp_path):
        """The name match is form-insensitive: a bare (uncalled) decorator
        still flags checkpoint usage."""
        scan = self._scan(
            tmp_path,
            """
            import dlt

            from dlt_ops import with_checkpoints

            @dlt.resource
            @with_checkpoints
            def rows():
                yield {}
            """,
        )
        assert scan.uses_checkpoints is True

    def test_resources_nested_in_source_body_are_scanned(self, tmp_path):
        """Resources declared inside a source function body (idiomatic dlt)
        are part of the static approximation."""
        scan = self._scan(
            tmp_path,
            """
            import dlt

            @dlt.source(name="my_api")
            def my_api_source():
                @dlt.resource(name="inner_rows")
                def inner_rows():
                    yield {}

                return inner_rows
            """,
        )
        assert scan.sources == (("my_api_source", "my_api"),)
        assert scan.resources == ("inner_rows",)


class TestMultiSourceDiscovery:
    """Tests for multi-source per directory support."""

    def test_phase1_discover_returns_multiple(self, make_project):
        """Phase-1 discover finds all sources in a pipeline dir —
        no __init__.py files, no sys.path setup, no imports required."""
        root = make_project(
            files={
                "multi_source/source/api_one.py": """
                    import dlt

                    @dlt.source(name="api_one")
                    def api_one_source():
                        pass
                    """,
                "multi_source/source/api_two.py": """
                    import dlt

                    @dlt.source(name="api_two")
                    def api_two_source():
                        pass
                    """,
            },
        )
        results = discover(root)
        assert set(results) == {"api_one", "api_two"}
        assert {info.function_name for info in results.values()} == {"api_one_source", "api_two_source"}

    def test_discover_sources_keys_by_source_name(self, make_project):
        """discover_sources keys by config_section (decorator name), not
        directory name."""
        root = make_project(
            files={
                "test_pipeline/source/my_api.py": """
                    import dlt

                    @dlt.source(name="my_api")
                    def my_api_source():
                        return []
                    """,
            },
        )
        sources = discover_sources(root)
        # Key should be "my_api" (source name), not "test_pipeline" (dir name)
        assert "my_api" in sources
        assert sources["my_api"].name == "my_api"
        assert sources["my_api"].pipeline_name == "test_pipeline"


class TestResourceOverlapValidation:
    """Tests for resource overlap validation."""

    def _make_ctx(self, sources: dict) -> ValidationContext:
        """Helper to create ValidationContext for tests."""
        return ValidationContext(sources=sources, config={}, project_root=Path("/test"))

    def test_no_overlap_passes(self):
        """No errors when resources are unique across sources."""

        def dummy_source():
            pass

        sources = {
            "source_a": SourceInfo(
                name="source_a",
                pipeline_name="pipeline",
                path=Path("/test"),
                function_name="source_a_source",
                source_fn=dummy_source,
                resources=("res1", "res2"),
                module_stem="source_a",
            ),
            "source_b": SourceInfo(
                name="source_b",
                pipeline_name="pipeline",
                path=Path("/test"),
                function_name="source_b_source",
                source_fn=dummy_source,
                resources=("res3", "res4"),
                module_stem="source_b",
            ),
        }
        errors = validate_no_resource_overlap(self._make_ctx(sources))
        assert len(errors) == 0

    def test_overlap_in_same_pipeline_fails(self):
        """Error when two sources in same pipeline share a resource."""

        def dummy_source():
            pass

        sources = {
            "source_a": SourceInfo(
                name="source_a",
                pipeline_name="pipeline",
                path=Path("/test"),
                function_name="source_a_source",
                source_fn=dummy_source,
                resources=("shared_res", "res1"),
                module_stem="source_a",
            ),
            "source_b": SourceInfo(
                name="source_b",
                pipeline_name="pipeline",
                path=Path("/test"),
                function_name="source_b_source",
                source_fn=dummy_source,
                resources=("shared_res", "res2"),
                module_stem="source_b",
            ),
        }
        errors = validate_no_resource_overlap(self._make_ctx(sources))
        assert len(errors) == 1
        assert errors[0].field == "resources"
        assert "shared_res" in errors[0].message
        assert "source_a" in errors[0].message

    def test_same_resource_in_different_pipelines_ok(self):
        """No error when same resource name exists in different pipelines."""

        def dummy_source():
            pass

        sources = {
            "source_a": SourceInfo(
                name="source_a",
                pipeline_name="pipeline_one",
                path=Path("/test"),
                function_name="source_a_source",
                source_fn=dummy_source,
                resources=("companies", "users"),
                module_stem="source_a",
            ),
            "source_b": SourceInfo(
                name="source_b",
                pipeline_name="pipeline_two",
                path=Path("/test"),
                function_name="source_b_source",
                source_fn=dummy_source,
                resources=("companies", "orders"),
                module_stem="source_b",
            ),
        }
        errors = validate_no_resource_overlap(self._make_ctx(sources))
        assert len(errors) == 0
