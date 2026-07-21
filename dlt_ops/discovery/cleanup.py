"""Pipeline cleanup — dlt's own drop for the remote half, dlt-ops code for the rest.

The destination-side work (data tables, dlt's schema record, dlt resource
state) goes through :class:`dlt.pipeline.helpers.pipeline_drop`. Constructing
it prepares the drop and collects the plan in ``.info`` — that is the dry run;
calling the instance executes it as a transactional, append-only pipeline run.
dlt owns its storage layout, so nothing here decodes or writes dlt state.

The drop runs on a pipeline whose working dir is a throwaway temp dir synced
from the destination. Two properties fall out of that: ``--remote-only`` never
mutates local state (the drop *is* a pipeline run, so a real working dir would
be rewritten), and a pending local load package can never block it.

What stays dlt-ops code, because dlt knows nothing about it:

- the local pipeline working dir under ``~/.dlt/pipelines`` (``--local-only``)
- rows in ``_dlt_custom_checkpoints``
- on a full clean, the pipeline's leftover rows in dlt's shared system tables,
  so ``clean`` keeps meaning "this source left no footprint in this dataset"
- source-level (multi-resource) scope, from the dlt-ops project layout

All dlt-ops SQL is canonical (DuckDB dialect, ``?`` placeholders) and goes
through the resolved ``DestinationAdapter``; identifier validation/quoting is
the adapter's ``render_identifier`` / ``render_table_ref``.
"""

import json
import logging
import shutil
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dlt
from dlt.common.pipeline import get_dlt_pipelines_dir

from dlt_ops.checkpoints import DEFAULT_CHECKPOINT_TABLE
from dlt_ops.destinations import (
    DestinationAdapter,
    UnregisteredDestinationError,
    open_destination_boundary,
)
from dlt_ops.discovery.models import SourceInfo
from dlt_ops.runs import pipeline_name_for_source

logger = logging.getLogger(__name__)

# dlt system tables (shared across pipelines in a dataset), under dlt's default
# naming convention. Only for display and for the case where the destination
# holds no schema to ask; the live names come from _DltStateTables.
DLT_SYSTEM_TABLES = ("_dlt_pipeline_state", "_dlt_loads", "_dlt_version")

_SIMPLE_REGEX_PREFIX = "re:"
"""dlt reads a resource selector with this prefix as a regex; dlt-ops means exact names only."""

_STATE_ENGINE_VERSION = 4
"""The local ``state.json`` layout :func:`_clean_local_state_selective` edits by hand.

dlt stamps the engine version it wrote and migrates older state forward, so this
is the one thing worth checking before hand-editing the file: a bump means the
shape this code assumes is no longer the shape on disk. It keys on what dlt
records, not on a list of dlt releases — a new dlt minor that keeps engine 4
needs no change here.
"""


@dataclass(frozen=True)
class _DltStateTables:
    """Physical names of dlt's shared bookkeeping tables, as one schema renders them.

    dlt normalizes its own table and column identifiers through the schema's
    naming convention before it writes them (``_norm_and_escape_columns``), so a
    hardcoded ``pipeline_name`` literal is only correct for the conventions that
    happen to leave it alone. Asking the live schema removes the guess — and the
    guess is what a dlt-minor allowlist could never have caught anyway, since the
    naming convention is a per-destination choice, not a dlt version.
    """

    state: str
    loads: str
    version: str
    pipeline_name_column: str
    schema_name_column: str

    @classmethod
    def from_schema(cls, schema: Any) -> "_DltStateTables":
        normalize = schema.naming.normalize_path
        return cls(
            state=schema.state_table_name,
            loads=schema.loads_table_name,
            version=schema.version_table_name,
            pipeline_name_column=normalize("pipeline_name"),
            schema_name_column=normalize("schema_name"),
        )


_DEFAULT_STATE_TABLES = _DltStateTables(
    state="_dlt_pipeline_state",
    loads="_dlt_loads",
    version="_dlt_version",
    pipeline_name_column="pipeline_name",
    schema_name_column="schema_name",
)
"""Used only when the destination holds no schema to derive from — in which case
it also holds no rows for this pipeline, so the names never have to be right."""


def _pipeline_working_dir(pipeline_name: str) -> Path:
    """The pipeline's local working dir, resolved the way dlt resolves it (DLT_DATA_DIR aware)."""
    return Path(get_dlt_pipelines_dir()) / pipeline_name


def _validate_resources(source: SourceInfo, resources: Sequence[str] | None) -> None:
    """Resource selectors must name resources this source actually declares.

    dlt reads a ``re:``-prefixed selector as a pattern, which would silently
    widen a targeted clean into a matching one; ``clean`` promises exact names,
    so those are refused rather than forwarded.
    """
    if not resources:
        return
    available = set(source.resources)
    missing = set(resources) - available
    if missing:
        raise ValueError(f"Unknown resources: {missing}. Available: {sorted(available)}")
    patterns = sorted(r for r in resources if r.startswith(_SIMPLE_REGEX_PREFIX))
    if patterns:
        raise ValueError(
            f"Resource selectors must be exact names, not patterns: {patterns}. "
            f"A {_SIMPLE_REGEX_PREFIX!r} prefix is a dlt regex and would widen the clean."
        )


@contextmanager
def _synced_drop_pipeline(pipeline_name: str, destination: Any, dataset_name: str) -> Iterator[Any]:
    """A destination-synced dlt pipeline on a throwaway working dir.

    ``sync_destination`` pulls the schemas and pipeline state the destination
    holds, so the drop works whether or not the user has local state. The
    working dir is temporary because ``pipeline_drop`` executes a real pipeline
    run: pointing it at the user's ``~/.dlt/pipelines`` entry would make
    ``--remote-only`` rewrite local state as a side effect.
    """
    with tempfile.TemporaryDirectory() as tmp:
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            pipelines_dir=tmp,
        )
        pipeline.sync_destination()
        yield pipeline


def _resolve_schema(pipeline: Any, schema_name: str) -> Any | None:
    """The dlt Schema cleanup acts on, or None when the destination holds nothing.

    dlt-ops names a source's schema after the source, but the dlt source may
    declare its own ``name=``. Falling back to the pipeline's default schema
    keeps cleanup working for that layout instead of raising on a lookup.
    """
    if not pipeline.default_schema_name:
        logger.info(
            f"Pipeline '{pipeline.pipeline_name}' has no state in dataset "
            f"'{pipeline.dataset_name}' — nothing for dlt to drop"
        )
        return None
    if schema_name not in pipeline.schemas:
        logger.warning(
            f"Schema '{schema_name}' is not in pipeline '{pipeline.pipeline_name}' "
            f"(has: {sorted(pipeline.schemas)}); using default schema '{pipeline.default_schema_name}'"
        )
        schema_name = pipeline.default_schema_name
    return pipeline.schemas[schema_name]


def _prepare_drop(pipeline: Any, schema: Any, resources: Sequence[str] | None) -> Any | None:
    """dlt's prepared-but-unexecuted drop, or None when there is nothing to drop.

    ``resources is None`` means the whole source (dlt's ``drop_all``); a list
    selects those resources. Resource names travel as plain strings, which dlt
    compiles to ``^re.escape(name)$`` — exact literal matches, so a hostile name
    can neither widen the selection nor reach a pattern.

    dlt's ``PipelineNeverRan`` is answered here rather than escaping to the
    caller: nothing ever landed under this pipeline name is a no-op for cleanup,
    not an error.
    """
    from dlt.pipeline.exceptions import PipelineNeverRan
    from dlt.pipeline.helpers import pipeline_drop

    try:
        return pipeline_drop(
            pipeline,
            resources=list(resources or ()),
            schema_name=schema.name,
            drop_all=resources is None,
        )
    except PipelineNeverRan:
        logger.info(f"Pipeline '{pipeline.pipeline_name}' was never run — nothing for dlt to drop")
        return None


def _delete_rows_where(
    adapter: DestinationAdapter, client: Any, dataset: str, table: str, column: str, value: str
) -> bool:
    """DELETE matching rows from a shared system table (never DROP); False when the table is absent."""
    if not adapter.table_exists(client, dataset, table):
        return False
    adapter.execute_sql(
        client,
        f"DELETE FROM {adapter.render_table_ref(dataset, table)} WHERE {adapter.render_identifier(column)} = ?",
        value,
    )
    return True


def _clean_remote_bookkeeping(
    adapter: DestinationAdapter,
    client: Any,
    dataset: str,
    pipeline_name: str,
    schema_name: str,
    resources: Sequence[str] | None,
    state_tables: _DltStateTables,
) -> list[str]:
    """The destination rows dlt's drop leaves behind, because they are dlt-ops's.

    Full clean purges this pipeline's rows from dlt's shared system tables —
    dlt's drop is append-only, so without this a "cleaned" source still shows up
    in the dataset's load history. Selective clean leaves them alone (the
    surviving resources still own that history) and only drops the checkpoints
    of the resources it targeted.

    ``state_tables`` carries dlt's own names for its tables and columns; the
    checkpoint table is dlt-ops's, so its column names are literals by ownership.
    """
    cleaned: list[str] = []

    if resources is None:
        # Pipeline-scoped tables filter by pipeline_name; schema-scoped by schema_name.
        for table, column, value in (
            (state_tables.state, state_tables.pipeline_name_column, pipeline_name),
            (state_tables.version, state_tables.schema_name_column, schema_name),
            (state_tables.loads, state_tables.schema_name_column, schema_name),
            (DEFAULT_CHECKPOINT_TABLE, "pipeline_name", pipeline_name),
        ):
            try:
                if _delete_rows_where(adapter, client, dataset, table, column, value):
                    cleaned.append(f"state: {table} (rows deleted)")
                    logger.info(f"Deleted {table} rows where {column} = {value}")
            except Exception as e:
                logger.warning(f"Failed to clean {table}: {e}")
        return cleaned

    if adapter.table_exists(client, dataset, DEFAULT_CHECKPOINT_TABLE):
        checkpoint_ref = adapter.render_table_ref(dataset, DEFAULT_CHECKPOINT_TABLE)
        for resource_name in resources:
            try:
                adapter.execute_sql(
                    client,
                    f"DELETE FROM {checkpoint_ref} WHERE pipeline_name = ? AND resource_name = ?",
                    pipeline_name,
                    resource_name,
                )
                cleaned.append(f"checkpoint: {resource_name}")
                logger.info(f"Deleted checkpoints for resource: {resource_name}")
            except Exception as e:
                logger.warning(f"Failed to clean checkpoints for {resource_name}: {e}")
    return cleaned


def _require_adapter(pipeline_name: str, destination: Any, dataset: str) -> None:
    """Refuse a core-mode destination before anything has been destroyed.

    Remote cleanup needs a ``DestinationAdapter`` for the dlt-ops-owned rows, so
    an adapterless destination has to fail ahead of dlt's drop rather than after
    it — the drop is not reversible.
    """
    with open_destination_boundary(pipeline_name, destination, dataset):
        pass


def _assert_editable_local_state(working_dir: Path) -> None:
    """Refuse an unknown local ``state.json`` layout before any destructive step runs.

    Called up front by :func:`clean_pipeline` as well as from the edit itself, so
    a layout dlt changed under us aborts the whole verb instead of stopping half
    way with the destination already dropped.
    """
    state_path = working_dir / "state.json"
    if not state_path.exists():
        return
    try:
        engine = json.loads(state_path.read_text(encoding="utf-8")).get("_state_engine_version")
    except Exception as e:
        logger.warning(f"Failed to read local state: {e}")
        return
    if engine != _STATE_ENGINE_VERSION:
        raise RuntimeError(
            f"{state_path} is state engine version {engine!r}; this build edits version "
            f"{_STATE_ENGINE_VERSION}. Refusing to hand-edit an unknown layout — clean the whole "
            "source instead (which drops the working dir outright), or use --remote-only."
        )


def _clean_local_state_selective(working_dir: Path, schema_name: str, resources: list[str]) -> list[str]:
    """Remove specific resource entries from local state.json and schema.

    This is the one place cleanup still edits a dlt-owned file by hand: dlt has
    no public API for "reset these resources in the local working dir" short of
    dropping all of it. The engine version dlt stamped is checked first, so a
    layout change is a refusal rather than a corrupted state file.
    """
    cleaned = []
    state_path = working_dir / "state.json"

    state = None
    if state_path.exists():
        try:
            with open(state_path, encoding="utf-8") as f:
                state = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read local state: {e}")

    if state is not None:
        # Deliberately outside the try below: an unknown layout is a refusal, not
        # a warning that leaves the caller thinking the resource state was reset.
        _assert_editable_local_state(working_dir)
        try:
            modified = False
            for source_data in state.get("sources", {}).values():
                resources_dict = source_data.get("resources", {})
                for res in resources:
                    if res in resources_dict:
                        del resources_dict[res]
                        modified = True

            if modified:
                state["_state_version"] = state.get("_state_version", 0) + 1
                with open(state_path, "w", encoding="utf-8") as f:
                    json.dump(state, f, indent=2, default=str)
                cleaned.append("state.json (resource entries removed)")
                logger.info("Updated local state.json")
        except Exception as e:
            logger.warning(f"Failed to update local state: {e}")

    # Remove local schema file entirely — dlt re-derives it from source on next run.
    # Surgical edits (removing tables) break dlt's schema hash verification
    # (InStorageSchemaModified) because the stored hash no longer matches content.
    schema_path = working_dir / "schemas" / f"{schema_name}.schema.json"
    if schema_path.exists():
        try:
            schema_path.unlink()
            cleaned.append(f"schema (deleted {schema_name}.schema.json)")
            logger.info(f"Deleted local schema: {schema_path}")
        except Exception as e:
            logger.warning(f"Failed to delete local schema: {e}")

    return cleaned


def get_cleanup_plan(
    source: SourceInfo,
    resources: list[str] | None,
    local: bool,
    remote: bool,
    dataset_name: str | None,
    destination: Any = None,
) -> dict:
    """Build a cleanup plan for display (dry-run info).

    The remote half is dlt's own dry run: ``pipeline_drop`` is constructed and
    its ``.info`` read, never called. ``data_tables`` therefore lists what dlt
    would actually drop — including nested child tables — rather than a guess
    from the naming convention.

    A destination that cannot be reached degrades the plan to its local half
    plus a warning; a destination with no adapter is refused outright, because
    remote cleanup could not run anyway.

    Returns dict with: pipeline_name, schema_name, working_dir, local_exists,
    is_full, target_resources, data_tables, resource_states, system_tables,
    warnings.
    """
    pipeline_name = pipeline_name_for_source(source.name)
    schema_name = source.name
    working_dir = _pipeline_working_dir(pipeline_name)
    is_full = resources is None
    target_resources = resources or list(source.resources)

    data_tables: list[str] = []
    resource_states: list[str] = []
    warnings: list[str] = []

    if remote and dataset_name and destination is not None:
        try:
            _require_adapter(pipeline_name, destination, dataset_name)
            with _synced_drop_pipeline(pipeline_name, destination, dataset_name) as pipeline:
                schema = _resolve_schema(pipeline, schema_name)
                drop = None if schema is None else _prepare_drop(pipeline, schema, resources)
                if drop is None:
                    warnings.append(
                        f"pipeline '{pipeline_name}' has no state in dataset '{dataset_name}'; "
                        "dlt has nothing to drop there"
                    )
                else:
                    data_tables = list(drop.info.get("tables", ()))
                    resource_states = list(drop.info.get("resource_states", ()))
                    warnings.extend(drop.info.get("warnings", ()))
        except UnregisteredDestinationError:
            # A capability refusal, not a transient failure: remote cleanup
            # cannot run at all, so the dry run says so instead of drawing a
            # plan the user could approve.
            raise
        except Exception as e:
            logger.warning(f"Failed to open destination for cleanup plan: {e}")
            warnings.append(f"destination unreachable ({e}); the remote table list is unknown")

    return {
        "pipeline_name": pipeline_name,
        "schema_name": schema_name,
        "working_dir": working_dir,
        "local_exists": working_dir.exists(),
        "is_full": is_full,
        "target_resources": target_resources,
        "data_tables": data_tables,
        "resource_states": resource_states,
        "system_tables": list(DLT_SYSTEM_TABLES) if is_full else [],
        "warnings": warnings,
    }


def clean_pipeline(
    source: SourceInfo,
    resources: list[str] | None,
    local: bool,
    remote: bool,
    dataset_name: str | None,
    destination: Any = None,
) -> dict[str, list[str]]:
    """Clean pipeline state and data.

    Cleanup modes:

    - Full (``resources=None``): dlt drops every data table and resets every
      resource state; dlt-ops then purges this pipeline's rows from dlt's shared
      system tables and its checkpoints, and removes the local working dir.
    - Selective (``resources=[...]``): dlt drops those resources' tables (nested
      child tables included) and resets their state; dlt-ops removes their
      checkpoint rows and edits the local state.json, keeping the working dir.

    Args:
        source: SourceInfo from discovery
        resources: Specific resources to clean (None = all)
        local: Clean local cache
        remote: Clean remote destination tables
        dataset_name: Destination dataset/schema; required when remote=True
        destination: dlt destination — name (e.g. "duckdb") or factory
            instance; required when remote=True

    Returns:
        {"local": [...], "remote": [...]} with cleaned items
    """
    result: dict[str, list[str]] = {"local": [], "remote": []}

    _validate_resources(source, resources)

    pipeline_name = pipeline_name_for_source(source.name)
    schema_name = source.name
    working_dir = _pipeline_working_dir(pipeline_name)
    is_full = resources is None

    # Everything that can refuse, refuses here — before the first irreversible
    # step. A full clean deletes the working dir outright and never reads it.
    if local and not is_full:
        _assert_editable_local_state(working_dir)

    # Remote cleanup
    if remote:
        if dataset_name is None:
            raise ValueError(
                "dataset_name is required for remote cleanup: resolve it via .dlt/config.toml or pass it explicitly"
            )
        if destination is None:
            raise ValueError(
                "destination is required for remote cleanup: resolve it via .dlt/config.toml or pass it explicitly"
            )

        _require_adapter(pipeline_name, destination, dataset_name)

        # dlt's own names for its bookkeeping tables and columns, read off the
        # schema the destination holds; the defaults only apply when it holds
        # none, in which case there are no rows for this pipeline anyway.
        state_tables = _DEFAULT_STATE_TABLES

        with _synced_drop_pipeline(pipeline_name, destination, dataset_name) as pipeline:
            schema = _resolve_schema(pipeline, schema_name)
            if schema is not None:
                state_tables = _DltStateTables.from_schema(schema)
                drop = _prepare_drop(pipeline, schema, resources)
            else:
                drop = None
            if drop is not None:
                for warning in drop.info.get("warnings", ()):
                    logger.warning(warning)
                if drop.is_empty:
                    logger.info("dlt's drop selected no tables and no resource state to reset")
                else:
                    drop()
                    # tables_with_data is what physically existed; the rest of
                    # `tables` only ever lived in the schema.
                    result["remote"].extend(f"table: {t}" for t in drop.info.get("tables_with_data", ()))
                    result["remote"].extend(f"state: reset {r}" for r in drop.info.get("resource_states", ()))
                    logger.info(f"dlt drop complete for schema '{drop.info.get('schema_name', schema_name)}'")

        with open_destination_boundary(pipeline_name, destination, dataset_name) as (adapter, client):
            result["remote"].extend(
                _clean_remote_bookkeeping(
                    adapter, client, dataset_name, pipeline_name, schema_name, resources, state_tables
                )
            )

    # Local cleanup AFTER remote, so a failed remote half leaves local state to retry from.
    if local:
        if is_full:
            if working_dir.exists():
                shutil.rmtree(working_dir)
                result["local"].append(str(working_dir))
                logger.info(f"Removed local pipeline directory: {working_dir}")
            else:
                logger.info(f"Local pipeline directory does not exist: {working_dir}")
        else:
            # Selective: update local state + schema, keep working dir
            assert resources is not None  # narrowed by is_full check
            if working_dir.exists():
                result["local"].extend(_clean_local_state_selective(working_dir, schema_name, resources))
            else:
                logger.info("No local state to clean")

    return result
