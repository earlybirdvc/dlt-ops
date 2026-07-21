"""Tests for the DestinationAdapter boundary.

Covers plugin registration, the capability-tier surface (`has_adapter`, the
canonical gated-feature list, core-mode messaging), identifier grammar,
transpile snapshots for the SQL shapes checkpoints/cleanup/reconciler need,
param binding (including the SQL-injection probe), a live DuckDB round trip
through dlt's real sql_client, and the BigQuery adapter without credentials
(plus an integration-marked live test that skips without them).
"""

import datetime as dt
import json
import logging
import os
import re
import subprocess
import sys
import typing
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import attrs
import dlt
import pytest
from dlt.common.destination import Destination
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from dlt_ops import DestinationAdapter
from dlt_ops.destinations import (
    ADAPTER_GATED_FEATURES,
    CI_VERIFIED_DESTINATIONS,
    ColumnInfo,
    Cursor,
    UnderivableDestinationError,
    UnregisteredDestinationError,
    adapter_for_pipeline,
    core_mode_notice,
    derivable_destinations,
    derived_adapter,
    engine_name,
    get_adapter,
    has_adapter,
    is_capability_derived,
    register_derived_adapter,
)
from dlt_ops.destinations._base import SqlAdapterBase, _MaterializedCursor
from dlt_ops.destinations._capabilities import derive_capabilities
from dlt_ops.destinations.bigquery import BigQueryAdapter
from dlt_ops.destinations.duckdb import DuckDBAdapter
from dlt_ops.destinations.protocol import (
    CANONICAL_IDENTIFIER_RE,
    render_canonical_identifier,
    render_canonical_table_ref,
)
from dlt_ops.plugins import names
from dlt_ops.plugins import registry as registry_mod

ADAPTERS = [DuckDBAdapter(), BigQueryAdapter()]
ADAPTER_IDS = [adapter.name for adapter in ADAPTERS]


@pytest.fixture
def clean_registry():
    """Fresh plugin registry per test — runtime adapter registrations must not leak."""
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


class RecordingClient:
    """Stands in for a dlt sql_client: records the native SQL + bound params."""

    def __init__(self, rows: list | None = None, error: Exception | None = None):
        self.calls: list[tuple[str, str, tuple]] = []
        self._rows = rows or []
        self._error = error

    def execute_sql(self, sql, *args):
        self.calls.append(("execute_sql", sql, args))
        if self._error is not None:
            raise self._error
        return None

    @contextmanager
    def execute_query(self, sql, *args):
        self.calls.append(("execute_query", sql, args))
        if self._error is not None:
            raise self._error
        yield SimpleNamespace(fetchall=lambda: list(self._rows))


def canonical_shapes(adapter: DestinationAdapter) -> dict[str, tuple[str, int]]:
    """The canonical (DuckDB-dialect) SQL shapes the port tickets will emit."""
    ref = adapter.render_table_ref("ds", "cp")
    now = adapter.timestamp_now_sql
    return {
        "create_table": (
            f"CREATE TABLE IF NOT EXISTS {ref} ("
            "pipeline_name VARCHAR NOT NULL, resource_name VARCHAR NOT NULL, run_id VARCHAR, "
            "checkpoint_value VARCHAR NOT NULL, page_number BIGINT, records_processed BIGINT, "
            "status VARCHAR DEFAULT 'active', "
            f"created_at TIMESTAMPTZ DEFAULT {now}, updated_at TIMESTAMPTZ DEFAULT {now})",
            0,
        ),
        "insert": (
            f"INSERT INTO {ref} (pipeline_name, resource_name, run_id, checkpoint_value, "
            "page_number, records_processed, status) VALUES (?, ?, ?, ?, ?, ?, 'active')",
            6,
        ),
        "update": (
            f"UPDATE {ref} SET status = 'completed', updated_at = {now} "
            "WHERE pipeline_name = ? AND resource_name = ? AND status = 'active'",
            2,
        ),
        "delete_old": (
            f"DELETE FROM {ref} WHERE status = 'completed' AND created_at < {adapter.timestamp_sub_days_sql(7)}",
            0,
        ),
        "select_latest": (
            f"SELECT checkpoint_value FROM {ref} WHERE pipeline_name = ? AND resource_name = ? "
            "AND status = 'active' AND run_id IS NULL ORDER BY created_at DESC LIMIT 1",
            2,
        ),
        "alter_add_column": (f"ALTER TABLE {ref} ADD COLUMN IF NOT EXISTS run_id VARCHAR", 0),
    }


# Snapshot-locked native renderings. The BigQuery strings are asserted exactly:
# they must stay valid GoogleSQL (backticks, %s placeholders, CURRENT_TIMESTAMP(),
# STRING/INT64 types, interval arithmetic).
EXPECTED_SQL = {
    ("duckdb", "create_table"): (
        'CREATE TABLE IF NOT EXISTS "ds"."cp" (pipeline_name TEXT NOT NULL, resource_name TEXT NOT NULL, '
        "run_id TEXT, checkpoint_value TEXT NOT NULL, page_number BIGINT, records_processed BIGINT, "
        "status TEXT DEFAULT 'active', created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
        "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"
    ),
    ("duckdb", "insert"): (
        'INSERT INTO "ds"."cp" (pipeline_name, resource_name, run_id, checkpoint_value, page_number, '
        "records_processed, status) VALUES (?, ?, ?, ?, ?, ?, 'active')"
    ),
    ("duckdb", "update"): (
        'UPDATE "ds"."cp" SET status = \'completed\', updated_at = CURRENT_TIMESTAMP '
        "WHERE pipeline_name = ? AND resource_name = ? AND status = 'active'"
    ),
    ("duckdb", "delete_old"): (
        "DELETE FROM \"ds\".\"cp\" WHERE status = 'completed' AND created_at < CURRENT_TIMESTAMP - INTERVAL '7' DAYS"
    ),
    ("duckdb", "select_latest"): (
        'SELECT checkpoint_value FROM "ds"."cp" WHERE pipeline_name = ? AND resource_name = ? '
        "AND status = 'active' AND run_id IS NULL ORDER BY created_at DESC LIMIT 1"
    ),
    ("duckdb", "alter_add_column"): 'ALTER TABLE "ds"."cp" ADD COLUMN IF NOT EXISTS run_id TEXT',
    ("bigquery", "create_table"): (
        "CREATE TABLE IF NOT EXISTS `ds`.`cp` (pipeline_name STRING NOT NULL, resource_name STRING NOT NULL, "
        "run_id STRING, checkpoint_value STRING NOT NULL, page_number INT64, records_processed INT64, "
        "status STRING DEFAULT 'active', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), "
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP())"
    ),
    ("bigquery", "insert"): (
        "INSERT INTO `ds`.`cp` (pipeline_name, resource_name, run_id, checkpoint_value, page_number, "
        "records_processed, status) VALUES (%s, %s, %s, %s, %s, %s, 'active')"
    ),
    ("bigquery", "update"): (
        "UPDATE `ds`.`cp` SET status = 'completed', updated_at = CURRENT_TIMESTAMP() "
        "WHERE pipeline_name = %s AND resource_name = %s AND status = 'active'"
    ),
    ("bigquery", "delete_old"): (
        "DELETE FROM `ds`.`cp` WHERE status = 'completed' AND created_at < CURRENT_TIMESTAMP() - INTERVAL '7' DAY"
    ),
    ("bigquery", "select_latest"): (
        "SELECT checkpoint_value FROM `ds`.`cp` WHERE pipeline_name = %s AND resource_name = %s "
        "AND status = 'active' AND run_id IS NULL ORDER BY created_at DESC LIMIT 1"
    ),
    ("bigquery", "alter_add_column"): "ALTER TABLE `ds`.`cp` ADD COLUMN IF NOT EXISTS run_id STRING",
}

EXPECTED_COLUMNS_SQL = {
    "duckdb": (
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position"
    ),
    "bigquery": (
        "SELECT column_name, data_type FROM `ds`.INFORMATION_SCHEMA.COLUMNS "
        "WHERE table_name = %s ORDER BY ordinal_position NULLS LAST"
    ),
}


class TestRegistrationAndProtocol:
    def test_first_party_adapters_registered_via_entry_points(self):
        registered = names("destination")
        assert "duckdb" in registered
        assert "bigquery" in registered
        assert registry_mod.source("destination", "duckdb").dist == "dlt-ops"
        assert registry_mod.source("destination", "bigquery").dist == "dlt-ops"

    def test_get_adapter_resolves_and_instantiates(self):
        assert isinstance(get_adapter("duckdb"), DuckDBAdapter)
        assert isinstance(get_adapter("bigquery"), BigQueryAdapter)

    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    def test_adapter_satisfies_protocol(self, adapter):
        assert isinstance(adapter, DestinationAdapter)

    def test_capability_surface(self):
        duckdb_adapter, bigquery_adapter = ADAPTERS
        assert (duckdb_adapter.name, duckdb_adapter.placeholder_style) == ("duckdb", "?")
        assert (bigquery_adapter.name, bigquery_adapter.placeholder_style) == ("bigquery", "%s")
        for adapter in ADAPTERS:
            assert adapter.supports_if_exists is True
        assert duckdb_adapter.supports_create_schema_if_not_exists is True
        assert bigquery_adapter.supports_create_schema_if_not_exists is False

    def test_timestamp_fragments_are_shared_not_per_adapter(self):
        """One canonical fragment for every adapter; the dialect writer spells it.

        Each destination's native rendering (``CURRENT_TIMESTAMP()`` and its
        interval arithmetic) is still snapshot-locked in EXPECTED_SQL — what is
        asserted here is that no adapter restates in Python what transpilation
        already performs.
        """
        for adapter in ADAPTERS:
            assert adapter.timestamp_now_sql == "CURRENT_TIMESTAMP"
            assert adapter.timestamp_sub_days_sql(7) == "CURRENT_TIMESTAMP - INTERVAL '7 days'"

    def test_column_info_is_frozen(self):
        column = ColumnInfo(name="a", data_type="VARCHAR")
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            column.name = "b"

    def test_package_import_loads_no_adapter_implementation(self):
        """Tier questions must stay cheap: answering "is there an adapter"
        loads neither an adapter nor the transpile machinery. The derivation
        helpers are re-exported lazily (PEP 562) for exactly this reason."""
        code = "import json, sys, dlt_ops.destinations; print(json.dumps(sorted(sys.modules)))"
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        loaded = set(json.loads(proc.stdout))
        implementations = {"_base", "_capabilities", "derived", "duckdb", "postgres", "bigquery"}
        assert loaded & {f"dlt_ops.destinations.{module}" for module in implementations} == set()


class TestCapabilityTiers:
    """`has_adapter` is the tier switch; the gated-feature list is canonical."""

    def test_has_adapter_true_for_registered_engines(self):
        assert has_adapter("duckdb") is True
        assert has_adapter("bigquery") is True

    @pytest.mark.parametrize("ref", ["filesystem", "snowflake"])
    def test_has_adapter_false_for_core_tier_destinations(self, ref):
        assert has_adapter(ref) is False

    def test_has_adapter_normalizes_module_path_refs(self):
        assert has_adapter("dlt.destinations.duckdb") is True

    def test_has_adapter_unresolvable_ref_is_false(self):
        """Resolvability is preflight's question — this predicate just answers 'no'."""
        assert has_adapter("definitely_not_a_destination") is False

    def test_has_adapter_never_loads_the_adapter(self):
        class MembershipOnlyRegistry:
            def names(self, axis):
                assert axis == "destination"
                return ("snowflake",)

            def get(self, axis, name):
                raise AssertionError("has_adapter must not load the adapter")

        assert has_adapter("snowflake", registry=MembershipOnlyRegistry()) is True

    def test_engine_name_is_the_shared_normalization(self):
        assert engine_name(Destination.from_reference("dlt.destinations.duckdb")) == "duckdb"

    def test_adapter_gated_features_is_the_canonical_list(self):
        assert ADAPTER_GATED_FEATURES == (
            "runs ledger and status",
            "checkpoints",
            "backfill",
            "clean (remote)",
            "reconcile",
            "assertion quarantine",
        )

    def test_core_mode_notice_renders_every_gated_feature(self):
        notice = core_mode_notice("filesystem")
        assert "'filesystem'" in notice
        assert "core mode" in notice
        for feature in ADAPTER_GATED_FEATURES:
            assert feature in notice

    def test_unregistered_destination_error_frames_core_mode(self):
        pipeline = SimpleNamespace(
            pipeline_name="probe",
            destination=SimpleNamespace(destination_type="dlt.destinations.motherduck"),
        )
        with pytest.raises(UnregisteredDestinationError) as excinfo:
            adapter_for_pipeline(pipeline)
        message = str(excinfo.value)
        assert "'probe'" in message
        assert "'motherduck'" in message
        assert "core mode" in message
        assert "'duckdb'" in message  # registered-adapter list is dynamic, from the registry
        assert "docs/reference/destinations.md" in message
        for feature in ADAPTER_GATED_FEATURES:
            assert feature in message


class TestCapabilityDerivation:
    """dlt publishes most of an adapter; the base reads it instead of restating it.

    The derivation is offline and SDK-free (dlt synthesizes mock credentials to
    describe a destination), so these run in the credential-free lane against
    destinations this package ships no adapter for.
    """

    def test_dialect_is_derived_and_is_not_the_engine_name(self):
        """The fact a hand-written adapter would most easily get wrong.

        `name` is the registry key and `dialect` the transpile target; both
        T-SQL engines prove they are two facts, not one spelled twice.
        """
        assert derive_capabilities("mssql").dialect == "tsql"
        assert derive_capabilities("synapse").dialect == "tsql"
        assert derive_capabilities("snowflake").dialect == "snowflake"

    def test_derived_adapter_transpiles_into_the_derived_dialect(self):
        """End to end: no hand-written class, and the SQL comes out native."""
        client = RecordingClient()
        derived_adapter("mssql").execute_sql(client, 'INSERT INTO "ds"."cp" (a, b) VALUES (?, ?)', "x", "y")
        (_, sql, args) = client.calls[0]
        assert sql == "INSERT INTO [ds].[cp] (a, b) VALUES (?, ?)"
        assert args == ("x", "y")

    def test_ddl_flag_comes_from_dlt_not_from_optimism(self):
        """dlt knows which destinations reject `IF NOT EXISTS` on table DDL."""
        assert derive_capabilities("mssql").supports_if_exists is False
        assert derive_capabilities("duckdb").supports_if_exists is True

    def test_no_if_exists_falls_back_to_probe_then_drop(self):
        """The derived flag reaches behaviour, not just the attribute."""
        client = RecordingClient(rows=[])  # fetch_columns finds nothing -> table absent
        derived_adapter("mssql").drop_table_if_exists(client, "ds", "cp")
        assert [call[0] for call in client.calls] == ["execute_query"]  # probed, never dropped

    def test_placeholder_style_is_derived_from_the_dialect(self):
        """A dialect fact, so it needs no declaring where driver and dialect agree."""
        assert derive_capabilities("postgres").placeholder_style == "%s"
        assert derive_capabilities("duckdb").placeholder_style == "?"
        assert get_adapter("postgres").placeholder_style == "%s"

    def test_a_driver_that_disagrees_with_its_dialect_must_declare(self):
        """The counterexample that keeps placeholder_style declarable.

        GoogleSQL writes `?`, but the DB-API behind it binds `%s` — a driver
        fact, which no capability dlt publishes describes.
        """
        assert derive_capabilities("bigquery").placeholder_style == "?"
        assert BigQueryAdapter().placeholder_style == "%s"

    @pytest.mark.parametrize(
        ("ref", "reason"),
        [
            ("sqlalchemy", "no sqlglot_dialect"),
            ("weaviate", "no sqlglot_dialect"),
            ("definitely_not_a_destination", "cannot resolve"),
        ],
    )
    def test_underivable_destinations_refuse_instead_of_guessing(self, ref, reason):
        """Never guess a dialect: SQL in the wrong dialect parses and lies.

        The refusal has to be loud and specific — it names the destination, the
        reason derivation failed, and the tier the caller falls back to.
        """
        assert derive_capabilities(ref) is None
        with pytest.raises(UnderivableDestinationError) as excinfo:
            derived_adapter(ref)
        message = str(excinfo.value)
        assert ref in message
        assert reason in message
        assert "core mode" in message

    def test_a_dialect_with_unrenderable_placeholders_is_underivable(self):
        """The dialect is declared, but its writer turns `?` into structure
        rather than a token — deriving would emit SQL no driver can bind."""
        assert derive_capabilities("clickhouse") is None

    def test_derivation_is_opt_in(self):
        """Publishing enough to derive is not the same as being supported.

        Registration stays the tier switch, so a destination this package has
        never run against does not silently become full tier — see
        `register_derived_adapter` for why that matters even when derivation
        is dialect-correct.
        """
        assert "filesystem" in derivable_destinations()
        assert "snowflake" in derivable_destinations()
        assert has_adapter("filesystem") is False
        assert has_adapter("snowflake") is False

    def test_derivable_destinations_covers_the_engines_dlt_describes(self):
        for engine in ("snowflake", "databricks", "redshift", "athena", "mssql", "synapse", "fabric", "dremio"):
            assert engine in derivable_destinations()

    def test_registering_a_derived_adapter_reaches_full_tier(self, clean_registry):
        """One line, no adapter class — and every gated feature lights up."""
        register_derived_adapter("snowflake")
        assert has_adapter("snowflake") is True
        pipeline = SimpleNamespace(
            pipeline_name="probe",
            destination=SimpleNamespace(destination_type="dlt.destinations.snowflake"),
        )
        adapter = adapter_for_pipeline(pipeline)
        assert adapter.name == "snowflake"
        assert isinstance(adapter, DestinationAdapter)

    def test_a_derived_adapter_satisfies_the_preflight_capability_check(self, clean_registry):
        """Full tier means passing the same gate a hand-written adapter passes.

        Preflight hard-fails a registered-but-incomplete adapter, so this is
        what proves derivation produces a whole one rather than a plausible
        object that fails at the first gated feature.
        """
        from dlt_ops.preflight import check_destination_capability

        register_derived_adapter("snowflake")
        check_destination_capability("snowflake", uses_checkpoints=True, require_adapter=True)

    def test_registration_announces_itself_as_unverified(self, clean_registry, caplog):
        """The honest signal: usable, and it says what it has not been through."""
        with caplog.at_level(logging.WARNING):
            register_derived_adapter("snowflake")
        assert "capability-derived" in caplog.text
        assert "not exercised by dlt-ops CI" in caplog.text
        assert "'snowflake'" in caplog.text

    def test_ci_verified_roster_stays_separate_from_the_registry(self):
        """Registration answers "can this run"; this answers "has anyone checked"."""
        assert CI_VERIFIED_DESTINATIONS == ("duckdb", "postgres")
        assert "bigquery" not in CI_VERIFIED_DESTINATIONS  # live lane exists but is credential-gated

    def test_is_capability_derived_separates_derived_from_hand_written(self):
        assert is_capability_derived(derived_adapter("snowflake")) is True
        assert is_capability_derived(DuckDBAdapter()) is False
        assert is_capability_derived(BigQueryAdapter()) is False

    @pytest.mark.parametrize(
        "ident",
        ["bad-name", "bad.name", "bad name", "x'); DROP TABLE t;--", "", '"quoted"', "`backtick`", "[bracket]"],
    )
    def test_derived_adapters_inherit_the_identifier_grammar(self, ident):
        """Derivation adds capabilities; it never relaxes the injection defence.

        Notably the T-SQL bracket, which is that dialect's own quoting
        character — the grammar rejects it before any quoting happens.
        """
        with pytest.raises(ValueError, match="identifier"):
            derived_adapter("mssql").render_identifier(ident)

    def test_derived_adapter_binds_hostile_values_as_params(self):
        hostile = "x'); DROP TABLE t;--"
        client = RecordingClient()
        derived_adapter("snowflake").execute_sql(client, 'INSERT INTO "ds"."cp" (a) VALUES (?)', hostile)
        (_, sql, args) = client.calls[0]
        assert hostile not in sql
        assert args == (hostile,)


class TestRenderIdentifier:
    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    def test_valid_identifier_is_canonically_quoted(self, adapter):
        assert adapter.render_identifier("my_table_1") == '"my_table_1"'

    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    @pytest.mark.parametrize(
        "ident",
        ["bad-name", "bad.name", "bad name", "x'); DROP TABLE t;--", "", '"quoted"', "`backtick`"],
        ids=["dash", "dot", "space", "injection", "empty", "dquote", "backtick"],
    )
    def test_identifier_outside_grammar_raises(self, adapter, ident):
        with pytest.raises(ValueError, match="identifier"):
            adapter.render_identifier(ident)

    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    def test_render_table_ref_validates_both_parts(self, adapter):
        assert adapter.render_table_ref("ds", "cp") == '"ds"."cp"'
        with pytest.raises(ValueError, match="identifier"):
            adapter.render_table_ref("ds", "cp; DROP TABLE t")


class TestCanonicalIdentifierGrammar:
    """The one validate-and-quote implementation the whole package shares.

    Adapters render identifiers through it, and so does every other caller
    building canonical SQL (the reconciler's detectors). A second copy is how
    a tightened grammar stops reaching one of the SQL paths.
    """

    def test_adapters_and_reconciler_share_one_implementation(self):
        """Not "equivalent code" — literally the same callable."""
        from dlt_ops.reconciler.common import canonical_ident, canonical_table_ref

        assert canonical_ident is render_canonical_identifier
        assert canonical_table_ref is render_canonical_table_ref

    def test_base_adapter_defaults_to_the_shared_grammar(self):
        """A new adapter inherits the grammar instead of restating the regex."""
        assert SqlAdapterBase._identifier_re is CANONICAL_IDENTIFIER_RE
        for adapter in ADAPTERS:
            assert adapter._identifier_re.pattern == CANONICAL_IDENTIFIER_RE.pattern

    @pytest.mark.parametrize(
        "ident",
        ["bad-name", "bad.name", "bad name", "x'); DROP TABLE t;--", "", '"quoted"', "`backtick`", "a\nb"],
        ids=["dash", "dot", "space", "injection", "empty", "dquote", "backtick", "newline"],
    )
    def test_rejects_anything_outside_the_grammar(self, ident):
        with pytest.raises(ValueError, match="identifier"):
            render_canonical_identifier(ident)

    @pytest.mark.parametrize("ident", [None, 42, b"bytes", ["col"]], ids=["none", "int", "bytes", "list"])
    def test_rejects_non_strings(self, ident):
        """A non-string never reaches ``fullmatch`` as a coerced value."""
        with pytest.raises(ValueError, match="identifier"):
            render_canonical_identifier(ident)

    def test_valid_identifier_is_quoted(self):
        assert render_canonical_identifier("my_table_1") == '"my_table_1"'
        assert render_canonical_table_ref("ds", "cp") == '"ds"."cp"'

    def test_a_tightened_grammar_is_honoured(self):
        """An adapter may narrow the grammar; nothing may widen it."""
        lowercase_only = re.compile(r"[a-z_]+")
        assert render_canonical_identifier("ok_name", grammar=lowercase_only) == '"ok_name"'
        with pytest.raises(ValueError, match="must match"):
            render_canonical_identifier("MixedCase", grammar=lowercase_only)

    def test_subject_names_the_rejected_value(self):
        """Adapters pass their own name so the error says which grammar failed."""
        with pytest.raises(ValueError, match="invalid duckdb identifier"):
            DuckDBAdapter().render_identifier("bad-name")
        with pytest.raises(ValueError, match="invalid identifier"):
            render_canonical_identifier("bad-name")


class TestProtocolStaysAdapterAgnostic:
    """The port must not enumerate the adapters that happen to sit behind it."""

    def test_placeholder_style_is_an_open_type(self):
        """A closed set here is a conformance wall.

        An adapter whose driver binds on ':1' or '%(name)s' is a legitimate
        implementation of this Protocol; a Literal of the first-party styles
        makes it impossible to type-conform to the package's own public API.
        """
        hints = typing.get_type_hints(DestinationAdapter)
        assert hints["placeholder_style"] is str

    def test_adapter_with_an_unlisted_placeholder_style_conforms(self):
        class NamedStyleAdapter(SqlAdapterBase):
            name = "duckdb"
            placeholder_style = ":1"
            supports_if_exists = True
            supports_create_schema_if_not_exists = True
            timestamp_now_sql = "now()"

            def timestamp_sub_days_sql(self, days):
                return "now()"

            def _columns_query(self, dataset, table):
                return ("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", (table,))

        assert isinstance(NamedStyleAdapter(), DestinationAdapter)

    def test_every_protocol_member_has_a_consumer(self):
        """A member no package code reads is a conformance tax, not a contract.

        The Tier-2 preflight fails an adapter missing ANY Protocol member, and
        the authoring guide tells third parties to implement the Protocol
        structurally rather than inherit SqlAdapterBase — so each capability
        flag is work every external adapter must do. `supports_alter_add_column
        _if_not_exists` was carried with zero branches on it anywhere (its
        docstring named a checkpoint `run_id` migration that does not exist —
        the column ships in the initial CREATE TABLE). Adding a flag is cheap
        when a consumer arrives; carrying one that has none is not.
        """
        from dlt_ops.preflight import _protocol_members

        flags = {m for m in _protocol_members(DestinationAdapter) if m.startswith("supports_")}
        package = Path(__file__).resolve().parent.parent / "dlt_ops"
        sources = [p.read_text(encoding="utf-8") for p in package.rglob("*.py") if p.name != "protocol.py"]
        for flag in sorted(flags):
            readers = [text for text in sources if flag in text]
            assert readers, f"Protocol member {flag!r} has no consumer in dlt_ops/ — drop it or use it"

    def test_a_structural_adapter_needs_no_unused_flags(self):
        """The removal is only real if an adapter that never heard of the flag
        passes the capability probe."""
        from dlt_ops.preflight import _protocol_members

        assert "supports_alter_add_column_if_not_exists" not in _protocol_members(DestinationAdapter)

    def test_capability_flags_are_described_not_attributed(self):
        """Capability docs must say WHAT the flag means, not which destination
        happens to set it — a caller reading the port should not learn the
        adapter roster from it.

        The canonical dialect is named once in the module docstring, as the
        interchange grammar every adapter transpiles FROM; the exception proves
        the rule.
        """
        from dlt_ops.destinations import protocol as protocol_mod

        source = Path(protocol_mod.__file__).read_text(encoding="utf-8")
        capability_docs = source.split("supports_if_exists: bool", 1)[1]
        for brand in ("duckdb", "bigquery", "postgres"):
            assert brand not in capability_docs.lower(), brand


class TestTranspileSnapshots:
    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    @pytest.mark.parametrize(
        "shape", ["create_table", "insert", "update", "delete_old", "select_latest", "alter_add_column"]
    )
    def test_canonical_shape_renders_native_sql(self, adapter, shape):
        canonical_sql, param_count = canonical_shapes(adapter)[shape]
        client = RecordingClient()
        params = tuple(f"p{i}" for i in range(param_count))
        adapter.execute_sql(client, canonical_sql, *params)
        (method, sql, args) = client.calls[0]
        assert method == "execute_sql"
        assert sql == EXPECTED_SQL[(adapter.name, shape)]
        assert args == params

    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    def test_information_schema_columns_query(self, adapter):
        client = RecordingClient(rows=[("pipeline_name", "VARCHAR")])
        adapter.fetch_columns(client, "ds", "cp")
        (method, sql, args) = client.calls[0]
        assert method == "execute_query"
        assert sql == EXPECTED_COLUMNS_SQL[adapter.name]
        assert args == (("ds", "cp") if adapter.name == "duckdb" else ("cp",))


class _DollarStubAdapter(SqlAdapterBase):
    """Minimal $n-placeholder adapter with NULL inlining, to exercise the
    positional-numbering compression the first-party adapters don't reach
    (duckdb uses '?', bigquery/postgres use '%s')."""

    name = "duckdb"  # a real sqlglot dialect so statement.sql(...) renders
    placeholder_style = "$1"
    supports_if_exists = True
    supports_create_schema_if_not_exists = True
    timestamp_now_sql = "now()"
    _identifier_re = re.compile(r"[A-Za-z0-9_]+")
    inline_null_params = True


class TestParamBinding:
    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    def test_param_count_mismatch_raises_before_execution(self, adapter):
        client = RecordingClient()
        with pytest.raises(ValueError, match="placeholder/param mismatch"):
            adapter.execute_sql(client, 'SELECT * FROM "ds"."t" WHERE a = ? AND b = ?', "only-one")
        assert client.calls == []

    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    def test_named_placeholders_rejected(self, adapter):
        with pytest.raises(ValueError, match="positional"):
            adapter.execute_sql(RecordingClient(), 'SELECT * FROM "ds"."t" WHERE a = $name', "x")

    @pytest.mark.parametrize("adapter", ADAPTERS, ids=ADAPTER_IDS)
    def test_multiple_statements_rejected(self, adapter):
        with pytest.raises(ValueError, match="exactly one"):
            adapter.execute_sql(RecordingClient(), "SELECT 1; SELECT 2")

    def test_question_mark_inside_string_literal_is_not_a_placeholder(self):
        client = RecordingClient()
        BigQueryAdapter().execute_sql(client, 'SELECT \'?\' AS q, a FROM "ds"."t" WHERE a = ?', "x")
        sql = client.calls[0][1]
        assert "'?'" in sql
        assert sql.count("%s") == 1

    def test_injection_value_stays_out_of_sql_text(self):
        hostile = "x'); DROP TABLE t;--"
        client = RecordingClient()
        BigQueryAdapter().execute_sql(client, 'INSERT INTO "ds"."cp" (pipeline_name) VALUES (?)', hostile)
        (_, sql, args) = client.calls[0]
        assert hostile not in sql
        assert args == (hostile,)

    def test_bigquery_inlines_none_params_as_null(self):
        """BigQuery's DB-API can't type a bound None, so the adapter inlines a
        NULL literal and drops it from the bound params — the surviving
        placeholders stay aligned. This is the runs-ledger write path
        (resource_name/backfill_id are None on a plain source run)."""
        client = RecordingClient()
        BigQueryAdapter().execute_sql(client, 'INSERT INTO "ds"."cp" (a, b, c) VALUES (?, ?, ?)', "x", None, "z")
        (_, sql, args) = client.calls[0]
        assert args == ("x", "z")
        assert sql.count("%s") == 2
        assert "NULL" in sql

    def test_bigquery_binds_all_params_when_no_none(self):
        """No None present -> unchanged transpile path, every param bound."""
        client = RecordingClient()
        BigQueryAdapter().execute_sql(client, 'INSERT INTO "ds"."cp" (a, b) VALUES (?, ?)', "x", "y")
        (_, sql, args) = client.calls[0]
        assert args == ("x", "y")
        assert sql.count("%s") == 2
        assert "NULL" not in sql

    def test_duckdb_binds_none_natively_without_inlining(self):
        """Adapters that bind NULL (the default) keep None as a bound param."""
        client = RecordingClient()
        DuckDBAdapter().execute_sql(client, 'INSERT INTO "ds"."cp" (a, b) VALUES (?, ?)', "x", None)
        (_, sql, args) = client.calls[0]
        assert args == ("x", None)
        assert "NULL" not in sql

    def test_bigquery_all_none_params_inline_fully(self):
        """Every param None -> all placeholders become NULL literals, nothing bound."""
        client = RecordingClient()
        BigQueryAdapter().execute_sql(client, 'INSERT INTO "ds"."cp" (a, b) VALUES (?, ?)', None, None)
        (_, sql, args) = client.calls[0]
        assert args == ()
        assert sql.count("NULL") == 2
        assert "%s" not in sql

    def test_bigquery_zero_params_takes_default_path(self):
        """No params -> the default (non-inlining) path; no crash, nothing bound."""
        client = RecordingClient()
        BigQueryAdapter().execute_sql(client, 'DELETE FROM "ds"."cp"')
        (_, sql, args) = client.calls[0]
        assert args == ()
        assert "NULL" not in sql

    def test_bigquery_question_mark_literal_survives_alongside_dropped_none(self):
        """A '?' inside a string literal is not a placeholder, and a real None
        placeholder still inlines to NULL — the two don't interfere."""
        client = RecordingClient()
        BigQueryAdapter().execute_sql(client, 'INSERT INTO "ds"."cp" (q, a, b) VALUES (\'?\', ?, ?)', "keep", None)
        (_, sql, args) = client.calls[0]
        assert "'?'" in sql
        assert args == ("keep",)
        assert sql.count("%s") == 1
        assert "NULL" in sql

    def test_dollar_numbering_compresses_when_none_dropped(self):
        """$n numbering stays contiguous over the surviving (bound) params: a
        dropped None must not leave a gap ($1, NULL, $2 — never $1, NULL, $3)."""
        sql, bound = _DollarStubAdapter()._prepare_params(
            'INSERT INTO "d"."t" (a, b, c) VALUES (?, ?, ?)', ("x", None, "z")
        )
        assert bound == ("x", "z")
        assert "$1" in sql and "$2" in sql and "$3" not in sql
        assert "NULL" in sql


class TestMaterializedCursor:
    def test_fetchone_then_fetchall_drain(self):
        cursor = _MaterializedCursor([(1,), (2,), (3,)])
        assert isinstance(cursor, Cursor)
        assert cursor.fetchone() == (1,)
        assert cursor.fetchall() == [(2,), (3,)]
        assert cursor.fetchone() is None
        assert cursor.fetchall() == []


DATASET = "adapter_ds"


@pytest.fixture(scope="module")
def duckdb_pipeline(tmp_path_factory):
    """A real dlt pipeline on DuckDB — the adapter must work through dlt's own sql_client."""
    tmp = tmp_path_factory.mktemp("dest_duckdb")
    pipeline = dlt.pipeline(
        pipeline_name="dest_adapter_test",
        destination=dlt.destinations.duckdb(str(tmp / "adapter_test.duckdb")),
        dataset_name=DATASET,
        pipelines_dir=str(tmp / "pipelines"),
    )
    pipeline.run([{"seed": 1}], table_name="seed_rows")
    return pipeline


@pytest.fixture
def duckdb_client(duckdb_pipeline):
    with duckdb_pipeline.sql_client() as client:
        yield client


@pytest.fixture
def duckdb_adapter():
    return get_adapter("duckdb")


class TestDuckDBLive:
    def _create(self, adapter, client, table):
        shapes = canonical_shapes(adapter)
        ddl, _ = shapes["create_table"]
        adapter.execute_sql(client, ddl.replace('"ds"."cp"', adapter.render_table_ref(DATASET, table)))

    def _shape(self, adapter, table, name):
        sql, _ = canonical_shapes(adapter)[name]
        return sql.replace('"ds"."cp"', adapter.render_table_ref(DATASET, table))

    def test_insert_select_round_trip(self, duckdb_adapter, duckdb_client):
        adapter, client = duckdb_adapter, duckdb_client
        self._create(adapter, client, "cp_roundtrip")
        insert = self._shape(adapter, "cp_roundtrip", "insert")
        adapter.execute_sql(client, insert, "pipe", "res", None, "cursor-1", 1, 10)
        adapter.execute_sql(client, insert, "pipe", "res", None, "cursor-2", 2, 20)
        cursor = adapter.execute_query(client, self._shape(adapter, "cp_roundtrip", "select_latest"), "pipe", "res")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] in ("cursor-1", "cursor-2")  # same-timestamp tie; both are valid "latest"
        assert cursor.fetchone() is None  # LIMIT 1

    def test_injection_probe_round_trips_as_data(self, duckdb_adapter, duckdb_client):
        adapter, client = duckdb_adapter, duckdb_client
        hostile = "x'); DROP TABLE t;--"
        adapter.execute_sql(client, f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(DATASET, 't')} (x BIGINT)")
        self._create(adapter, client, "cp_probe")
        adapter.execute_sql(client, self._shape(adapter, "cp_probe", "insert"), hostile, "res", None, "v", 1, 1)
        cursor = adapter.execute_query(
            client,
            f"SELECT pipeline_name FROM {adapter.render_table_ref(DATASET, 'cp_probe')} WHERE pipeline_name = ?",
            hostile,
        )
        assert cursor.fetchall() == [(hostile,)]
        # The decoy table survived: the hostile value never reached the SQL text.
        assert adapter.table_exists(client, DATASET, "t")

    def test_update_and_delete_old_shapes(self, duckdb_adapter, duckdb_client):
        adapter, client = duckdb_adapter, duckdb_client
        self._create(adapter, client, "cp_lifecycle")
        table_ref = adapter.render_table_ref(DATASET, "cp_lifecycle")
        adapter.execute_sql(client, self._shape(adapter, "cp_lifecycle", "insert"), "pipe", "res", None, "v", 1, 1)
        # Backdate a second, already-completed checkpoint so delete_old has a target.
        adapter.execute_sql(
            client,
            f"INSERT INTO {table_ref} (pipeline_name, resource_name, checkpoint_value, status, created_at) "
            "VALUES (?, ?, ?, 'completed', ?)",
            "pipe",
            "res",
            "old",
            dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
        )
        adapter.execute_sql(client, self._shape(adapter, "cp_lifecycle", "update"), "pipe", "res")
        adapter.execute_sql(client, self._shape(adapter, "cp_lifecycle", "delete_old"))
        cursor = adapter.execute_query(client, f"SELECT checkpoint_value, status FROM {table_ref}")
        rows = cursor.fetchall()
        assert rows == [("v", "completed")]  # fresh row completed, backdated row purged

    def test_fetch_columns_existing_table(self, duckdb_adapter, duckdb_client):
        adapter, client = duckdb_adapter, duckdb_client
        self._create(adapter, client, "cp_columns")
        columns = adapter.fetch_columns(client, DATASET, "cp_columns")
        assert columns is not None
        assert [column.name for column in columns][:4] == [
            "pipeline_name",
            "resource_name",
            "run_id",
            "checkpoint_value",
        ]
        assert columns[0] == ColumnInfo(name="pipeline_name", data_type="VARCHAR")
        assert ColumnInfo(name="page_number", data_type="BIGINT") in columns

    def test_fetch_columns_absent_returns_none(self, duckdb_adapter, duckdb_client):
        assert duckdb_adapter.fetch_columns(duckdb_client, DATASET, "no_such_table") is None
        assert duckdb_adapter.fetch_columns(duckdb_client, "no_such_dataset", "no_such_table") is None

    def test_table_exists_and_drop_if_exists(self, duckdb_adapter, duckdb_client):
        adapter, client = duckdb_adapter, duckdb_client
        self._create(adapter, client, "cp_drop")
        assert adapter.table_exists(client, DATASET, "cp_drop") is True
        adapter.drop_table_if_exists(client, DATASET, "cp_drop")
        assert adapter.table_exists(client, DATASET, "cp_drop") is False
        adapter.drop_table_if_exists(client, DATASET, "cp_drop")  # idempotent

    def test_ensure_schema_then_create_table(self, duckdb_adapter, duckdb_client):
        adapter, client = duckdb_adapter, duckdb_client
        adapter.ensure_schema(client, "adapter_ds_extra")
        adapter.ensure_schema(client, "adapter_ds_extra")  # idempotent
        adapter.execute_sql(
            client, f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref('adapter_ds_extra', 'probe')} (x BIGINT)"
        )
        assert adapter.table_exists(client, "adapter_ds_extra", "probe")

    def test_alter_add_column_if_not_exists(self, duckdb_adapter, duckdb_client):
        adapter, client = duckdb_adapter, duckdb_client
        self._create(adapter, client, "cp_alter")
        alter = self._shape(adapter, "cp_alter", "alter_add_column")
        adapter.execute_sql(client, alter)  # column already exists; IF NOT EXISTS absorbs it
        columns = adapter.fetch_columns(client, DATASET, "cp_alter")
        assert columns is not None
        assert sum(1 for column in columns if column.name == "run_id") == 1


class TestBigQueryOffline:
    def test_ensure_schema_is_a_no_op(self):
        client = RecordingClient()
        BigQueryAdapter().ensure_schema(client, "ds")
        assert client.calls == []

    def test_fetch_columns_maps_rows(self):
        client = RecordingClient(rows=[("pipeline_name", "STRING"), ("page_number", "INT64")])
        columns = BigQueryAdapter().fetch_columns(client, "ds", "cp")
        assert columns == [
            ColumnInfo(name="pipeline_name", data_type="STRING"),
            ColumnInfo(name="page_number", data_type="INT64"),
        ]

    def test_fetch_columns_no_rows_means_absent_table(self):
        assert BigQueryAdapter().fetch_columns(RecordingClient(rows=[]), "ds", "cp") is None

    def test_fetch_columns_absent_dataset_maps_to_none(self):
        client = RecordingClient(error=DatabaseUndefinedRelation(ValueError("404 dataset not found")))
        assert BigQueryAdapter().fetch_columns(client, "ds", "cp") is None

    def test_drop_table_if_exists_uses_if_exists_ddl(self):
        client = RecordingClient()
        BigQueryAdapter().drop_table_if_exists(client, "ds", "cp")
        assert client.calls == [("execute_sql", "DROP TABLE IF EXISTS `ds`.`cp`", ())]

    def test_adapter_import_pulls_no_google_sdk(self):
        """The BigQuery adapter must load without the BQ SDK (or credentials)."""
        code = "import json, sys, dlt_ops.destinations.bigquery; print(json.dumps(sorted(sys.modules)))"
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        loaded = json.loads(proc.stdout)
        forbidden = ("google.cloud", "google.api_core", "google.auth")
        offenders = [
            module for module in loaded if any(module == pkg or module.startswith(f"{pkg}.") for pkg in forbidden)
        ]
        assert offenders == []


@pytest.mark.integration
def test_bigquery_live_round_trip(tmp_path):
    """Live BigQuery lane: skips cleanly without the SDK or credentials."""
    pytest.importorskip("google.cloud.bigquery")
    if not (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("BQ_SERVICE_ACCOUNT_JSON")):
        pytest.skip("BigQuery credentials not configured")

    dataset, table = "dlt_ext_adapter_ci", "adapter_probe"
    adapter = get_adapter("bigquery")
    pipeline = dlt.pipeline(
        pipeline_name="dest_adapter_bq_ci",
        destination="bigquery",
        dataset_name=dataset,
        pipelines_dir=str(tmp_path),
    )
    pipeline.run([{"seed": 1}], table_name="seed_rows")
    hostile = "x'); DROP TABLE t;--"
    with pipeline.sql_client() as client:
        table_ref = adapter.render_table_ref(dataset, table)
        try:
            adapter.execute_sql(
                client,
                f"CREATE TABLE IF NOT EXISTS {table_ref} "
                f"(pipeline_name VARCHAR NOT NULL, created_at TIMESTAMPTZ DEFAULT {adapter.timestamp_now_sql})",
            )
            adapter.execute_sql(client, f"INSERT INTO {table_ref} (pipeline_name) VALUES (?)", hostile)
            cursor = adapter.execute_query(
                client, f"SELECT pipeline_name FROM {table_ref} WHERE pipeline_name = ?", hostile
            )
            assert [row[0] for row in cursor.fetchall()] == [hostile]
            columns = adapter.fetch_columns(client, dataset, table)
            assert columns is not None
            assert [column.name for column in columns] == ["pipeline_name", "created_at"]
            assert adapter.fetch_columns(client, dataset, "definitely_absent_table") is None
        finally:
            adapter.drop_table_if_exists(client, dataset, table)
