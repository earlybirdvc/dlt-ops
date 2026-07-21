"""Tests for the schema-drift reconciler.

Covers the modules ``additive``, ``removal``, ``_emission`` as independent
units plus DuckDB end-to-end runs through the real DestinationAdapter
boundary. Fake ``SourceInfo`` fixtures + Protocol-shaped fakes
(``FakeSchemaFetcher`` / ``FakeQueryRunner`` / ``RecordingSink``) drive the
detectors without any live destination or alerting SDK — the injection seam
the public API exposes (``fetcher=`` / ``runner=`` / ``sink=``).

The end-to-end class seeds real DuckDB files (one per source pipeline, the
package's per-source destination convention) and runs the default
adapter-backed path: canonical SQL transpiled and executed by the DuckDB
adapter against the source's own resolved destination + dataset.
"""

from __future__ import annotations

import re
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pydantic
import pytest

from dlt_ops import Schedule, SourceConfig, SourceInfo
from dlt_ops.config import ProjectConfig
from dlt_ops.destinations import ColumnInfo
from dlt_ops.reconciler import _emission as emission_mod
from dlt_ops.reconciler import additive as additive_mod
from dlt_ops.reconciler import removal as removal_mod
from dlt_ops.reconciler.models import DriftFinding, DriftKind
from dlt_ops.reconciler.protocols import TableRef

# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


class OrderItemModel(pydantic.BaseModel):
    """Minimal neutral resource shape used across the detector tests."""

    api_id: str
    order_id: str
    name: str | None = None
    discount_code: str | None = None


class AliasedModel(pydantic.BaseModel):
    """Exercises a Pydantic-v2 alias.

    The alias ``camelCase`` and the attribute ``snake_case`` both flow through
    dlt's destination-side ``snake_case`` normalizer, so the destination
    column that actually lands is ``camel_case`` — the reconciler's
    known-column set must include the normalized form (not the raw alias) to
    match.
    """

    model_config = pydantic.ConfigDict(populate_by_name=True)

    api_id: str
    snake_case: str | None = pydantic.Field(default=None, alias="camelCase")


class UpstreamCamelModel(pydantic.BaseModel):
    """CamelCase attributes preserved from an upstream ORM-shaped schema.

    The attributes are ``startTime`` / ``endTime`` / ``completionStartTime``
    on the source side but land as ``start_time`` / ``end_time`` /
    ``completion_start_time`` after dlt's destination normalizer.
    """

    request_id: str
    startTime: datetime  # noqa: N815 - name mirrors the upstream column
    endTime: datetime  # noqa: N815 - name mirrors the upstream column
    completionStartTime: datetime | None = None  # noqa: N815 - name mirrors the upstream column


class KeywordAliasModel(pydantic.BaseModel):
    """Python-keyword-workaround alias exercised end-to-end.

    ``FROM`` is a real upstream column but ``from`` is a Python keyword, so
    the model uses attribute ``from_`` with ``alias="FROM"``. dlt's
    destination normalizer lowercases the alias to the destination column
    ``from`` — the attribute-name normalization ``from_ -> fromx`` is a red
    herring; the alias path is what has to match.
    """

    id: int
    from_: str | None = pydantic.Field(default=None, alias="FROM")


def _make_fake_resource(model: type[pydantic.BaseModel]) -> Any:
    """A dlt-shaped resource: `resource.validator.model` is the Pydantic model."""
    return types.SimpleNamespace(validator=types.SimpleNamespace(model=model))


def _make_source(
    *,
    name: str = "orders_api",
    pipeline_name: str = "orders",
    resources: dict[str, type[pydantic.BaseModel] | None],
    injected_columns: tuple[str, ...] = ("region_id",),
    naming: Any = None,
) -> SourceInfo:
    """Fake SourceInfo whose ``source_fn()`` yields the given resources.

    A resource with ``model=None`` deliberately has no Pydantic model — the
    detectors must skip it. ``injected_columns`` defaults to a per-source
    stamped key to mirror the shape of ``[sources.<X>.dlt_ops]``;
    tests exercising a source without one pass an empty tuple.

    ``naming`` is an optional stand-in for the source's dlt Schema naming
    convention. When set, the fake source instance exposes ``schema.naming``
    so ``resolve_source_naming`` reads it back — exercise the dynamic
    naming-derivation path without spinning up a real ``DltSource``. When
    ``None`` (the default), the fake source has no schema attribute and the
    reconciler falls back to the default snake_case convention.
    """
    rs = {
        r_name: (_make_fake_resource(model) if model is not None else types.SimpleNamespace(validator=None))
        for r_name, model in resources.items()
    }
    fake_instance = types.SimpleNamespace(resources=rs)
    if naming is not None:
        fake_instance.schema = types.SimpleNamespace(naming=naming)
    config = SourceConfig(
        schedule=Schedule.HOURLY,
        injected_columns=injected_columns,
    )
    return SourceInfo(
        name=name,
        pipeline_name=pipeline_name,
        path=Path("/tmp/nowhere"),
        function_name=f"{name}_source",
        source_fn=lambda inst=fake_instance: inst,
        resources=tuple(resources.keys()),
        module_stem=name,
        config=config,
    )


def _project_config(
    *,
    load_timestamp_column: str | None = "loaded_at",
    injected_columns: tuple[str, ...] = (),
    default_destination: str | None = None,
    default_dataset: str | None = None,
) -> ProjectConfig:
    """Project-level [dlt_ops] view the reconciler reads.

    ``load_timestamp_column`` defaults to set ("loaded_at") because that is
    the fully-featured configuration; unset-degradation tests pass ``None``.
    """
    raw: dict[str, Any] = {}
    if load_timestamp_column is not None:
        raw["load_timestamp_column"] = load_timestamp_column
    if injected_columns:
        raw["injected_columns"] = list(injected_columns)
    return ProjectConfig(
        default_destination=default_destination,
        default_dataset=default_dataset,
        raw=raw,
    )


class FakeSchemaFetcher:
    """SchemaFetcher fake: table name -> ColumnInfo tuple (None = absent)."""

    def __init__(self, tables: dict[str, tuple[ColumnInfo, ...] | None]) -> None:
        self.tables = tables
        self.requested: list[TableRef] = []

    def fetch(self, refs: list[TableRef]) -> dict[TableRef, tuple[ColumnInfo, ...] | None]:
        self.requested.extend(refs)
        return {ref: self.tables.get(ref.table) for ref in refs}


class FakeQueryRunner:
    """QueryRunner fake driven by preloaded positional rows.

    - ``sample_rows``: tuples returned for the additive sample-values SELECT
      (positionally aligned to the drifted-column SELECT list).
    - ``coverage``: {column_alias: (recent, baseline)} for the removal
      coverage SELECT; every alias not listed gets ``default_coverage``. The
      fake parses the ``AS recent_<alias>`` / ``AS baseline_<alias>`` SELECT
      list to build the positional result row, mirroring how the real
      adapter-backed runner returns rows in SELECT-list order.
    """

    def __init__(
        self,
        *,
        sample_rows: list[tuple] | None = None,
        coverage: dict[str, tuple[float | None, float | None]] | None = None,
        default_coverage: tuple[float | None, float | None] = (1.0, 1.0),
        error: Exception | None = None,
    ) -> None:
        self.sample_rows = sample_rows or []
        self.coverage = coverage or {}
        self.default_coverage = default_coverage
        self.error = error
        self.queries: list[tuple[str, tuple]] = []

    def query(self, sql: str, params: Any = ()) -> list[Any]:
        self.queries.append((sql, tuple(params)))
        if self.error is not None:
            raise self.error
        aliases = re.findall(r"AS (recent|baseline)_([A-Za-z0-9_]+)", sql)
        if aliases:
            row = []
            for kind, alias in aliases:
                recent, baseline = self.coverage.get(alias, self.default_coverage)
                row.append(recent if kind == "recent" else baseline)
            return [tuple(row)]
        return list(self.sample_rows)


class RecordingSink:
    """AlertSink fake: records every event, counts flushes."""

    def __init__(self) -> None:
        self.drifts: list[DriftFinding] = []
        self.errors: list[tuple[str, str | None, str]] = []
        self.flushes = 0

    def emit_drift(self, finding: DriftFinding) -> None:
        self.drifts.append(finding)

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None:
        self.errors.append((source_name, resource_name, context))

    def flush(self, timeout: float = 2.0) -> None:
        self.flushes += 1


def _cols(*names_and_types: tuple[str, str] | str) -> tuple[ColumnInfo, ...]:
    """Shorthand: "col" (VARCHAR) or ("col", "TIMESTAMP") -> ColumnInfo tuple."""
    infos = []
    for item in names_and_types:
        if isinstance(item, tuple):
            infos.append(ColumnInfo(name=item[0], data_type=item[1]))
        else:
            infos.append(ColumnInfo(name=item, data_type="VARCHAR"))
    return tuple(infos)


ORDER_ITEM_LIVE = _cols("api_id", "order_id", "name", "discount_code", ("loaded_at", "TIMESTAMP"))


# ---------------------------------------------------------------------------
# Additive detector
# ---------------------------------------------------------------------------


class TestAdditiveDetection:
    """Live destination schema vs Pydantic model diff."""

    def _reconcile(
        self,
        source: SourceInfo,
        fetcher: Any,
        runner: Any,
        *,
        project_config: ProjectConfig | None = None,
        sink: Any = None,
        dry_run: bool = True,
        dataset: str = "raw",
    ):
        return additive_mod.reconcile_source(
            source.name,
            dry_run=dry_run,
            fetcher=fetcher,
            runner=runner,
            dataset=dataset,
            sources={source.name: source},
            project_config=project_config if project_config is not None else _project_config(),
            sink=sink,
        )

    def test_extra_column_produces_finding(self):
        """Destination carries a column the model doesn't know → one finding."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE + _cols("surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("hello",)])

        result = self._reconcile(source, fetcher, runner)

        assert result.error is None
        assert len(result.findings) == 1
        finding = result.findings[0]
        assert finding.kind == DriftKind.ADDITIVE
        assert finding.pipeline_name == "orders"
        assert finding.source_name == "orders_api"
        assert finding.resource_name == "order_items"
        assert finding.columns == ("surprise_column",)
        assert finding.inferred_types == ("VARCHAR",)
        assert finding.sample_values["surprise_column"] == ["hello"]
        # The reproduce SQL rides on the finding (sinks stay SQL-free) and is
        # anchored on the configured load-timestamp column.
        assert finding.reproduce_sql is not None
        assert '"surprise_column"' in finding.reproduce_sql
        assert '"raw"."order_items"' in finding.reproduce_sql
        assert '"loaded_at" >= TIMESTAMP' in finding.reproduce_sql
        # The sample query is windowed + ordered on the same column with the
        # window bound parameter-bound, never inlined.
        [(sample_sql, sample_params)] = runner.queries
        assert 'ORDER BY "loaded_at" DESC' in sample_sql
        assert 'WHERE "loaded_at" >= ?' in sample_sql
        assert len(sample_params) == 1

    def test_injected_columns_ignored(self):
        """The stamped load-timestamp column (project config) + `region_id`
        (per-source config) are infrastructure keys; the diff must never
        surface either as drift."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher(
            {"order_items": _cols("api_id", "order_id", ("loaded_at", "TIMESTAMP"), "region_id")}
        )
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.findings == ()

    def test_project_level_injected_columns_ignored(self):
        """Project-wide [dlt_ops] injected_columns join the ignored set
        even when the source declares nothing per-source."""
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"order_items": _cols("api_id", "order_id", "tenant_id")})
        result = self._reconcile(
            source,
            fetcher,
            FakeQueryRunner(),
            project_config=_project_config(injected_columns=("tenant_id",)),
        )
        assert result.findings == ()

    def test_project_injected_camel_case_key_normalized(self):
        """A camelCase project-level injected key is normalized with the
        source's naming convention before the diff — `sessionId` in TOML must
        match the persisted `session_id` column."""
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"order_items": _cols("api_id", "order_id", "session_id")})
        result = self._reconcile(
            source,
            fetcher,
            FakeQueryRunner(),
            project_config=_project_config(injected_columns=("sessionId",)),
        )
        assert result.findings == ()

    def test_source_without_injected_config_flags_source_specific_key(self):
        """A source that DOESN'T declare ``region_id`` in its
        ``[sources.X.dlt_ops].injected_columns`` must flag it as
        drift — the reconciler hardcodes no per-source knowledge.
        """
        source = _make_source(resources={"generic_resource": OrderItemModel}, injected_columns=())
        fetcher = FakeSchemaFetcher(
            {"generic_resource": _cols("api_id", "order_id", ("loaded_at", "TIMESTAMP"), "region_id")}
        )
        runner = FakeQueryRunner(sample_rows=[("reg-x",)])
        result = self._reconcile(source, fetcher, runner)
        assert len(result.findings) == 1
        assert result.findings[0].columns == ("region_id",)

    def test_dlt_system_columns_ignored(self):
        """`_dlt_*` prefix stays transparent — including future dlt columns."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher(
            {"order_items": _cols("api_id", "order_id", "_dlt_load_id", "_dlt_id", "_dlt_root_id", "_dlt_future_col")}
        )
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.findings == ()

    def test_no_drift_returns_no_findings(self):
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE})
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.findings == ()

    def test_missing_table_skips_resource(self):
        """A resource that never landed (fetcher returns None) is skipped."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": None})
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.error is None
        assert result.findings == ()

    def test_dataset_override_reaches_schema_fetch(self):
        """The dataset= override (tests-only) flows into every TableRef."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE})
        self._reconcile(source, fetcher, FakeQueryRunner(), dataset="custom_ds")
        assert fetcher.requested == [TableRef(dataset="custom_ds", table="order_items")]

    def test_aliased_model_field_matches_normalized_column(self):
        """`Field(alias="camelCase")` lands as `camel_case` after dlt's
        destination-side normalizer — the persisted column must not read as
        drift."""
        source = _make_source(resources={"aliased_res": AliasedModel})
        fetcher = FakeSchemaFetcher({"aliased_res": _cols("api_id", "camel_case")})
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.findings == ()

    def test_camel_case_pydantic_attribute_matches_snake_case_column(self):
        """A camelCase Pydantic attribute (no alias) lands as snake_case.

        Regression pin: a model preserving upstream `startTime` / `endTime` /
        `completionStartTime` attributes persists `start_time` / `end_time` /
        `completion_start_time`. Comparing raw attribute names against the
        normalized column names would flag all three as additive drift — no
        fix should ever surface these as findings again.
        """
        source = _make_source(resources={"upstream_logs": UpstreamCamelModel}, injected_columns=())
        fetcher = FakeSchemaFetcher(
            {
                "upstream_logs": _cols(
                    "request_id",
                    ("start_time", "TIMESTAMP"),
                    ("end_time", "TIMESTAMP"),
                    ("completion_start_time", "TIMESTAMP"),
                    ("loaded_at", "TIMESTAMP"),
                )
            }
        )
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.findings == ()

    def test_source_schema_naming_convention_is_used_not_default(self):
        """Prove the reconciler derives the NamingConvention from the source's
        own dlt Schema, not from a hardcoded default.

        Uses a fake naming convention that uppercases every identifier. Under
        this convention the model's known columns become ``API_ID`` etc. —
        matching the uppercased live schema below. Under the default
        snake_case (a hardcoded path), the model would resolve to lowercase
        and every live column would surface as false-positive drift. Zero
        findings therefore prove the source-derived path is wired end-to-end.

        This also guards the ignored set: the source's injected `region_id`
        and the stamped `loaded_at` land as `REGION_ID` / `LOADED_AT` under
        the uppercase convention, and the ignored set must normalize the same
        way or both would leak through as drift.
        """

        class UppercaseNaming:
            def normalize_identifier(self, name: str) -> str:
                return name.upper()

        source = _make_source(resources={"order_items": OrderItemModel}, naming=UppercaseNaming())
        fetcher = FakeSchemaFetcher(
            {
                "order_items": _cols(
                    "API_ID", "ORDER_ID", "NAME", "DISCOUNT_CODE", ("LOADED_AT", "TIMESTAMP"), "REGION_ID"
                )
            }
        )
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.findings == ()

    def test_uppercased_alias_matches_lowercased_column(self):
        """Python-keyword-workaround alias survives destination normalization.

        Regression pin: `from_ = Field(alias="FROM")` was in the model, but
        the destination persisted `from` (lowercased). Comparing `{"from_",
        "FROM"}` against `{"from"}` would flag the column as additive drift.
        The alias normalizes to `from` cleanly; the attribute name's `from_`
        -> `fromx` normalization is intentionally allowed to be a dead end
        because the alias path covers the real destination column.
        """
        source = _make_source(resources={"share_emails": KeywordAliasModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"share_emails": _cols(("id", "INTEGER"), "from", ("loaded_at", "TIMESTAMP"))})
        result = self._reconcile(source, fetcher, FakeQueryRunner())
        assert result.findings == ()

    def test_unset_load_timestamp_degrades_sampling(self):
        """No load-timestamp column → unordered LIMIT-5 sampling and a
        reproduce SQL without a time predicate; additive detection itself
        keeps working."""
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"order_items": _cols("api_id", "order_id", "surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("v1",), ("v2",)])

        result = self._reconcile(source, fetcher, runner, project_config=_project_config(load_timestamp_column=None))

        assert len(result.findings) == 1
        finding = result.findings[0]
        assert finding.columns == ("surprise_column",)
        assert finding.sample_values["surprise_column"] == ["v1", "v2"]
        [(sample_sql, sample_params)] = runner.queries
        assert "ORDER BY" not in sample_sql
        assert "WHERE" not in sample_sql
        assert sample_sql.endswith("LIMIT 5")
        assert sample_params == ()
        assert finding.reproduce_sql is not None
        assert "WHERE" not in finding.reproduce_sql

    def test_sample_failure_keeps_finding_without_samples(self):
        """A failing sample query never blocks the alert — samples are a
        nice-to-have context field, not a correctness gate."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE + _cols("surprise_column")})
        runner = FakeQueryRunner(error=RuntimeError("permission denied"))
        result = self._reconcile(source, fetcher, runner)
        assert len(result.findings) == 1
        assert result.findings[0].sample_values == {}

    def test_source_fn_crash_captured_and_reconcile_returns_cleanly(self):
        """When `SourceInfo.source_fn()` itself raises, per-resource error
        handling must still surface findings=() and error=None (no
        source-level error), and each resource iteration should report one
        reconciler error under context="detect_resource_drift".
        """

        def _boom(*_args, **_kwargs):
            raise RuntimeError("source_fn crash")

        source = SourceInfo(
            name="orders_api",
            pipeline_name="orders",
            path=Path("/fake/orders"),
            function_name="orders_api_source",
            source_fn=_boom,
            resources=("res_a", "res_b"),
            module_stem="orders_api",
            config=SourceConfig(schedule=Schedule.HOURLY, injected_columns=("region_id",)),
        )
        fetcher = FakeSchemaFetcher({"res_a": _cols("id"), "res_b": _cols("id")})
        sink = RecordingSink()

        result = self._reconcile(source, fetcher, FakeQueryRunner(), sink=sink, dry_run=False)

        assert result.error is None
        assert result.findings == ()
        # One report per resource; each is a detect_resource_drift context.
        assert sink.errors == [
            ("orders_api", "res_a", "detect_resource_drift"),
            ("orders_api", "res_b", "detect_resource_drift"),
        ]

    def test_fetcher_failure_returns_error_result(self):
        """A failing ``SchemaFetcher`` (auth / permission / connection bug)
        must land in ``result.error``, not escape and fail the caller's
        sweep — a raise here defeats the "must never fail the task"
        contract.
        """
        source = _make_source(resources={"order_items": OrderItemModel})

        class BoomFetcher:
            def fetch(self, refs):
                raise RuntimeError("credentials not found")

        sink = RecordingSink()
        result = self._reconcile(source, BoomFetcher(), FakeQueryRunner(), sink=sink, dry_run=False)

        assert result.error is not None
        assert "source-level failure" in result.error
        assert "credentials not found" in result.error
        assert result.findings == ()
        # Only the inner ``_detect_source_drift`` catch reports the failure —
        # the outer wrapper builds ``result.error`` from the re-raise but
        # does NOT re-emit (a second event for one bug).
        assert sink.errors == [("orders_api", None, "fetch_schemas")]

    def test_per_resource_failure_wrapped_and_continues(self, monkeypatch):
        """A single-resource crash reports a reconciler error and continues
        to the next resource."""

        class OtherModel(pydantic.BaseModel):
            api_id: str

        source = _make_source(resources={"good_res": OrderItemModel, "bad_res": OtherModel})
        fetcher = FakeSchemaFetcher(
            {
                "good_res": ORDER_ITEM_LIVE + _cols("extra_column"),
                "bad_res": _cols("api_id", "extra_bad_column"),
            }
        )
        runner = FakeQueryRunner(sample_rows=[])

        original = additive_mod._detect_resource_drift

        def flaky(src, res, cols, *, runner, dataset, ignored_columns, naming, load_timestamp_column):
            if res == "bad_res":
                raise RuntimeError("simulated per-resource failure")
            return original(
                src,
                res,
                cols,
                runner=runner,
                dataset=dataset,
                ignored_columns=ignored_columns,
                naming=naming,
                load_timestamp_column=load_timestamp_column,
            )

        monkeypatch.setattr(additive_mod, "_detect_resource_drift", flaky)
        sink = RecordingSink()

        result = self._reconcile(source, fetcher, runner, sink=sink, dry_run=False)

        # Good resource surfaced its finding; bad resource was reported
        # through the sink's error path; result.error is None (source-level
        # survived).
        assert result.error is None
        assert [f.resource_name for f in result.findings] == ["good_res"]
        assert ("orders_api", "bad_res", "detect_resource_drift") in sink.errors

    def test_missing_source_returns_error_result(self):
        result = additive_mod.reconcile_source(
            "nope_api",
            dry_run=True,
            fetcher=FakeSchemaFetcher({}),
            runner=FakeQueryRunner(),
            dataset="raw",
            sources={},
            project_config=_project_config(),
        )
        assert result.error is not None
        assert "not found" in result.error


# ---------------------------------------------------------------------------
# Removal detector
# ---------------------------------------------------------------------------


class TestRemovalDetection:
    """Coverage-window diff against the resource's destination table."""

    def _detect(
        self,
        source: SourceInfo,
        runner: Any,
        *,
        project_config: ProjectConfig | None = None,
        sink: Any = None,
        dry_run: bool = True,
        **kwargs: Any,
    ):
        return removal_mod.detect_removal(
            source.name,
            dry_run=dry_run,
            runner=runner,
            dataset="raw",
            sources={source.name: source},
            project_config=project_config if project_config is not None else _project_config(),
            sink=sink,
            **kwargs,
        )

    def test_baseline_gt_threshold_recent_lt_threshold_produces_finding(self):
        """Baseline coverage > 20%, recent < 1% → removal finding."""
        source = _make_source(resources={"order_items": OrderItemModel})
        runner = FakeQueryRunner(coverage={"discount_code": (0.005, 0.42)})

        result = self._detect(source, runner)

        assert result.error is None
        assert len(result.findings) == 1
        finding = result.findings[0]
        assert finding.kind == DriftKind.REMOVAL
        assert finding.resource_name == "order_items"
        assert finding.columns == ("discount_code",)
        assert finding.sample_values == {"discount_code": []}
        assert finding.reproduce_sql is not None
        assert '"discount_code"' in finding.reproduce_sql

    def test_baseline_zero_no_finding(self):
        """New column (baseline had no coverage) → not a removal signal.

        NULL-safe division returns None for an empty window.
        """
        source = _make_source(resources={"order_items": OrderItemModel})
        runner = FakeQueryRunner(coverage={"discount_code": (None, None)})
        result = self._detect(source, runner)
        assert result.findings == ()

    def test_both_windows_in_range_no_finding(self):
        """Coverage steady across windows → no signal."""
        source = _make_source(resources={"order_items": OrderItemModel})
        runner = FakeQueryRunner(coverage={"discount_code": (0.42, 0.42)})
        result = self._detect(source, runner)
        assert result.findings == ()

    def test_baseline_below_threshold_no_finding(self):
        """Sub-threshold baseline can't produce a removal signal even if recent is 0."""
        source = _make_source(resources={"order_items": OrderItemModel})
        runner = FakeQueryRunner(coverage={"discount_code": (0.0, 0.15)})
        result = self._detect(source, runner)
        assert result.findings == ()

    def test_missing_pydantic_model_skips_resource(self):
        """A resource without `columns=<PydanticModel>` is skipped, not errored."""
        source = _make_source(resources={"anonymous_res": None})
        runner = FakeQueryRunner()
        result = self._detect(source, runner)
        assert result.error is None
        assert result.findings == ()
        assert runner.queries == []

    def test_runner_failure_reported_per_resource(self):
        """A failing ``QueryRunner`` lands in the sink's error path but
        doesn't stop the sweep — the source-level result stays clean."""
        source = _make_source(resources={"order_items": OrderItemModel})
        runner = FakeQueryRunner(error=RuntimeError("credentials not found"))
        sink = RecordingSink()

        result = self._detect(source, runner, sink=sink, dry_run=False)

        assert result.error is None
        assert result.findings == ()
        assert sink.errors == [("orders_api", "order_items", "detect_removal")]

    def test_coverage_query_carries_partition_lower_bound(self):
        """The compiled coverage query MUST carry a parameter-bound lower
        bound on the load-timestamp column in the WHERE clause so
        time-partitioned destinations prune to the trailing baseline window.
        Without it the sweep scans every historical partition on every
        resource.
        """
        now = datetime.now(tz=UTC)
        recent_start = now - timedelta(hours=6)
        baseline_start = now - timedelta(days=7)
        sql, params = removal_mod._build_coverage_query(
            "raw",
            "order_items",
            ("api_id", "discount_code"),
            load_timestamp_column="loaded_at",
            recent_start=recent_start,
            baseline_start=baseline_start,
        )
        assert 'WHERE "loaded_at" >= ?' in sql
        # Window bounds are bound positionally, never inlined into SQL text:
        # in_recent, in_baseline (2), then the pruning lower bound.
        assert params == (recent_start, baseline_start, recent_start, baseline_start)

    def test_coverage_query_transpiles_to_other_dialects(self):
        """The canonical coverage SQL must survive sqlglot transpilation —
        NULL-safe division and conditional counts are written in transpilable
        form (CAST + SUM(CASE) + NULLIF), never destination-native idioms.
        """
        import sqlglot

        now = datetime.now(tz=UTC)
        sql, _ = removal_mod._build_coverage_query(
            "raw",
            "order_items",
            ("api_id", "discount_code"),
            load_timestamp_column="loaded_at",
            recent_start=now - timedelta(hours=6),
            baseline_start=now - timedelta(days=7),
        )
        for dialect in ("bigquery", "postgres", "duckdb"):
            transpiled = sqlglot.transpile(sql, read="duckdb", write=dialect)
            assert len(transpiled) == 1 and transpiled[0]

    def test_source_schema_naming_convention_is_used_not_default(self):
        """Twin of the additive-side test: the removal detector must derive
        its naming from the source's own dlt Schema. Uses the same
        UppercaseNaming stub so the coverage query targets the uppercased
        columns dlt would write under this convention. If the reconciler
        hardcoded snake_case, the SQL would reference the wrong identifiers
        and no row would satisfy the coverage lookups.
        """

        class UppercaseNaming:
            def normalize_identifier(self, name: str) -> str:
                return name.upper()

        source = _make_source(resources={"order_items": OrderItemModel}, naming=UppercaseNaming())
        runner = FakeQueryRunner()

        result = self._detect(source, runner)

        assert result.error is None
        assert result.findings == ()
        [(sql, _params)] = runner.queries
        assert '"API_ID"' in sql
        assert '"api_id"' not in sql

    def test_coverage_query_uses_destination_normalized_column_names(self):
        """The coverage projection MUST target destination-normalized column
        names — a camelCase Pydantic attribute like `startTime` would
        otherwise compile as `"startTime" IS NOT NULL`, which the destination
        rejects because the persisted column is `start_time`.
        """
        source = _make_source(resources={"upstream_logs": UpstreamCamelModel}, injected_columns=())
        runner = FakeQueryRunner()

        result = self._detect(source, runner)

        assert result.error is None
        assert result.findings == ()
        [(sql, _params)] = runner.queries
        assert '"start_time"' in sql
        assert '"end_time"' in sql
        assert '"completion_start_time"' in sql
        assert '"startTime"' not in sql
        assert '"endTime"' not in sql
        assert '"completionStartTime"' not in sql

    def test_thresholds_and_windows_are_overridable(self):
        """Every knob is a keyword-only parameter — a tighter baseline
        threshold catches a coverage collapse the module default would
        miss."""
        source = _make_source(resources={"order_items": OrderItemModel})
        # 15% baseline, 0.5% recent — below the default `baseline > 0.20`
        # gate, so the default run must skip this as noise.
        runner = FakeQueryRunner(coverage={"discount_code": (0.005, 0.15)})

        default_run = self._detect(source, runner)
        assert default_run.findings == ()  # sub-threshold on default

        override_run = self._detect(source, runner, baseline_threshold=0.10)
        assert len(override_run.findings) == 1
        assert override_run.findings[0].columns == ("discount_code",)

    def test_unset_load_timestamp_skips_detection_with_warning(self):
        """No load-timestamp column → no time axis for windowed coverage:
        detection is skipped, no query runs, and the result carries the
        warning the validate/CLI flows surface."""
        source = _make_source(resources={"order_items": OrderItemModel})
        runner = FakeQueryRunner()

        result = self._detect(source, runner, project_config=_project_config(load_timestamp_column=None))

        assert result.error is None
        assert result.findings == ()
        assert result.warnings == (removal_mod.LOAD_TIMESTAMP_UNSET_WARNING,)
        assert runner.queries == []


# ---------------------------------------------------------------------------
# Emission seam
# ---------------------------------------------------------------------------


def _make_finding(**overrides: Any) -> DriftFinding:
    base: dict[str, Any] = dict(
        kind=DriftKind.ADDITIVE,
        pipeline_name="orders",
        source_name="orders_api",
        resource_name="order_items",
        columns=("surprise_column",),
        inferred_types=("VARCHAR",),
        sample_values={"surprise_column": ["hello"]},
        first_seen_at=datetime(2026, 7, 3, 12, 0, 0, tzinfo=UTC),
        reproduce_sql='SELECT "surprise_column" FROM "raw"."order_items" LIMIT 5',
    )
    base.update(overrides)
    return DriftFinding(**base)


class TestEmissionSeam:
    def test_logging_sink_emits_structured_lines(self, caplog):
        sink = emission_mod.LoggingAlertSink()
        with caplog.at_level("WARNING", logger=emission_mod.logger.name):
            sink.emit_drift(_make_finding())
        assert any(
            "orders.orders_api.order_items" in record.getMessage() and "surprise_column" in record.getMessage()
            for record in caplog.records
        )

        with caplog.at_level("ERROR", logger=emission_mod.logger.name):
            sink.emit_error(RuntimeError("boom"), source_name="orders_api", context="probe")
        assert any("context=probe" in record.getMessage() for record in caplog.records)

    def test_resolve_sink_dry_run_outranks_injected_sink(self):
        recording = RecordingSink()
        resolved = emission_mod.resolve_sink(recording, dry_run=True)
        assert isinstance(resolved, emission_mod.NullAlertSink)
        assert isinstance(emission_mod.resolve_sink(recording, dry_run=False), RecordingSink)
        # No injected sink: config-driven resolution — the zero-config default
        # is a fan-out wrapper over the core logging sink.
        default = emission_mod.resolve_sink(None, dry_run=False, project_config=ProjectConfig())
        assert isinstance(default, emission_mod.MultiAlertSink)
        assert [(name, type(sink)) for name, sink in default.sinks] == [("logging", emission_mod.LoggingAlertSink)]

    def test_emit_findings_isolates_a_failing_sink(self):
        """One emit_drift failure never crashes the sweep; it is reported
        through the same sink's error path under a detector-specific
        context."""

        class FlakySink(RecordingSink):
            def emit_drift(self, finding: DriftFinding) -> None:
                if finding.kind is DriftKind.REMOVAL:
                    raise RuntimeError("transport down")
                super().emit_drift(finding)

        sink = FlakySink()
        additive = _make_finding()
        removal = _make_finding(kind=DriftKind.REMOVAL, resource_name="other_items")

        emission_mod.emit_findings(sink, [additive, removal])

        assert sink.drifts == [additive]
        assert sink.errors == [("orders_api", "other_items", "emit_drift_removal")]

    def test_dry_run_paths_do_not_emit(self):
        """`dry_run=True` on reconcile_source + detect_removal must suppress
        every sink call — drift, error, and flush all stay silent."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE + _cols("leaked_col")})
        runner = FakeQueryRunner(sample_rows=[("v",)])
        sink = RecordingSink()

        result = additive_mod.reconcile_source(
            "orders_api",
            dry_run=True,
            fetcher=fetcher,
            runner=runner,
            dataset="raw",
            sources={"orders_api": source},
            project_config=_project_config(),
            sink=sink,
        )

        assert len(result.findings) == 1
        assert sink.drifts == []
        assert sink.errors == []
        assert sink.flushes == 0

    def test_findings_flow_through_sink_and_flush_on_exit(self):
        """dry_run=False: findings emitted through the sink; the public entry
        point flushes exactly once on the way out."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE + _cols("surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("v",)])
        sink = RecordingSink()

        result = additive_mod.reconcile_source(
            "orders_api",
            dry_run=False,
            fetcher=fetcher,
            runner=runner,
            dataset="raw",
            sources={"orders_api": source},
            project_config=_project_config(),
            sink=sink,
        )

        assert [f.columns for f in sink.drifts] == [("surprise_column",)]
        assert sink.drifts == list(result.findings)
        assert sink.flushes == 1

    def test_detect_removal_flushes_on_exit(self):
        source = _make_source(resources={"order_items": OrderItemModel})
        runner = FakeQueryRunner(coverage={"discount_code": (0.0, 0.9)})
        sink = RecordingSink()

        result = removal_mod.detect_removal(
            "orders_api",
            dry_run=False,
            runner=runner,
            dataset="raw",
            sources={"orders_api": source},
            project_config=_project_config(),
            sink=sink,
        )

        assert [f.kind for f in sink.drifts] == [DriftKind.REMOVAL]
        assert sink.drifts == list(result.findings)
        assert sink.flushes == 1


# ---------------------------------------------------------------------------
# DuckDB end-to-end (real DestinationAdapter boundary)
# ---------------------------------------------------------------------------


@pytest.fixture
def duckdb_home(tmp_path, monkeypatch):
    """Isolated dlt home + cwd so per-pipeline DuckDB files land under tmp_path.

    The named ``duckdb`` destination materialises as ``<cwd>/<pipeline_name>.duckdb``
    — one file per source pipeline, which is exactly the per-source
    destination-locality the reconciler must honour.
    """
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-home"))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _seed(pipeline_name: str, dataset: str, table: str, rows: list[dict[str, Any]]) -> None:
    import dlt

    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb", dataset_name=dataset)
    info = pipeline.run(rows, table_name=table)
    assert not info.has_failed_jobs


@pytest.mark.integration
class TestReconcilerEndToEndDuckDB:
    """Default adapter-backed path against seeded DuckDB files."""

    def test_additive_finding_end_to_end(self, duckdb_home):
        """Seed a table with an extra column vs the model → additive finding,
        with samples fetched through the adapter-executed canonical SQL."""
        now = datetime.now(tz=UTC)
        _seed(
            "orders_api_pipeline",
            "raw_orders",
            "order_items",
            [
                {
                    "api_id": "a1",
                    "order_id": "o1",
                    "name": "first",
                    "discount_code": "D1",
                    "loaded_at": now,
                    "surprise_column": "hello",
                }
            ],
        )
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())

        result = additive_mod.reconcile_source(
            "orders_api",
            dry_run=True,
            sources={"orders_api": source},
            project_config=_project_config(default_destination="duckdb", default_dataset="raw_orders"),
        )

        assert result.error is None
        assert len(result.findings) == 1
        finding = result.findings[0]
        assert finding.columns == ("surprise_column",)
        assert finding.sample_values["surprise_column"] == ["hello"]
        assert finding.inferred_types[0]  # destination-native type string

    def test_removal_finding_end_to_end(self, duckdb_home):
        """Seed a coverage collapse (column non-null in the baseline window,
        null in the recent window) → removal finding when
        load_timestamp_column is set."""
        now = datetime.now(tz=UTC)
        baseline_rows = [
            {
                "api_id": f"b{i}",
                "order_id": f"o{i}",
                "name": "n",
                "discount_code": "D1",
                "loaded_at": now - timedelta(hours=24),
            }
            for i in range(5)
        ]
        recent_rows = [
            {
                "api_id": f"r{i}",
                "order_id": f"o{i}",
                "name": "n",
                "discount_code": None,
                "loaded_at": now - timedelta(minutes=10),
            }
            for i in range(5)
        ]
        _seed("orders_api_pipeline", "raw_orders", "order_items", baseline_rows + recent_rows)
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())

        result = removal_mod.detect_removal(
            "orders_api",
            dry_run=True,
            sources={"orders_api": source},
            project_config=_project_config(default_destination="duckdb", default_dataset="raw_orders"),
        )

        assert result.error is None
        assert len(result.findings) == 1
        assert result.findings[0].kind == DriftKind.REMOVAL
        assert result.findings[0].columns == ("discount_code",)

    def test_unset_load_timestamp_end_to_end(self, duckdb_home):
        """Without a configured load-timestamp column, removal detection is
        skipped with a warning and additive sampling still works unordered."""
        _seed(
            "orders_api_pipeline",
            "raw_orders",
            "order_items",
            [{"api_id": "a1", "order_id": "o1", "name": "n", "discount_code": "D", "surprise_column": "x"}],
        )
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())
        project_config = _project_config(
            load_timestamp_column=None, default_destination="duckdb", default_dataset="raw_orders"
        )

        additive_result = additive_mod.reconcile_source(
            "orders_api", dry_run=True, sources={"orders_api": source}, project_config=project_config
        )
        assert additive_result.error is None
        assert len(additive_result.findings) == 1
        assert additive_result.findings[0].columns == ("surprise_column",)
        assert additive_result.findings[0].sample_values["surprise_column"] == ["x"]

        removal_result = removal_mod.detect_removal(
            "orders_api", dry_run=True, sources={"orders_api": source}, project_config=project_config
        )
        assert removal_result.findings == ()
        assert removal_result.warnings == (removal_mod.LOAD_TIMESTAMP_UNSET_WARNING,)

    def test_multi_destination_sources_reconcile_against_own_destination(self, duckdb_home, make_project):
        """Two sources on two physical destinations (two DuckDB files, one per
        source pipeline) + per-source datasets: ``reconcile_all`` sweeps each
        source against its own destination/dataset. Drift seeded only on the
        second source must never leak into the first's result.
        """
        import dlt  # noqa: F401 - seeding path below

        root = make_project(
            config="""
            [dlt_ops]
            default_destination = "duckdb"
            default_dataset = "raw_main"
            load_timestamp_column = "loaded_at"

            [sources.alpha_api.dlt_ops]
            schedule = "@daily"
            dataset = "raw_alpha"

            [sources.beta_api.dlt_ops]
            schedule = "@daily"
            dataset = "raw_beta"
            """,
            files={
                "alpha/source/alpha_api.py": """
                    import dlt
                    import pydantic

                    class Widget(pydantic.BaseModel):
                        api_id: str
                        name: str | None = None

                    @dlt.resource(name="widgets", columns=Widget)
                    def widgets():
                        yield {"api_id": "1"}

                    @dlt.source(name="alpha_api")
                    def alpha_api_source():
                        return widgets
                    """,
                "beta/source/beta_api.py": """
                    import dlt
                    import pydantic

                    class Widget(pydantic.BaseModel):
                        api_id: str
                        name: str | None = None

                    @dlt.resource(name="widgets", columns=Widget)
                    def widgets():
                        yield {"api_id": "1"}

                    @dlt.source(name="beta_api")
                    def beta_api_source():
                        return widgets
                    """,
            },
        )
        now = datetime.now(tz=UTC)
        # Same resource + table name on both destinations; the extra column
        # exists only in beta's file. If the reconciler ever pointed both
        # sources at one destination, either alpha would report beta's drift
        # or beta's would go missing.
        _seed("alpha_api_pipeline", "raw_alpha", "widgets", [{"api_id": "1", "name": "a", "loaded_at": now}])
        _seed(
            "beta_api_pipeline",
            "raw_beta",
            "widgets",
            [{"api_id": "1", "name": "b", "loaded_at": now, "beta_extra": "boom"}],
        )
        assert (duckdb_home / "alpha_api_pipeline.duckdb").exists()
        assert (duckdb_home / "beta_api_pipeline.duckdb").exists()

        results = additive_mod.reconcile_all(dry_run=True, project_root=root)

        by_source = {r.source_name: r for r in results}
        assert set(by_source) == {"alpha_api", "beta_api"}
        assert by_source["alpha_api"].error is None
        assert by_source["alpha_api"].findings == ()
        assert by_source["beta_api"].error is None
        assert [f.columns for f in by_source["beta_api"].findings] == [("beta_extra",)]
