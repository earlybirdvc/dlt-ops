"""Pipeline cleanup — direct destination/local operations, no dlt API dependency.

Reverse-engineered dlt state storage (verified across DuckDB/Postgres/BigQuery
for the supported dlt range; re-verified per minor via ci/dump_state_schema.py):
- Local: <pipelines-dir>/{name}/state.json (plain JSON)
- Remote: _dlt_pipeline_state table (zlib+b64 compressed JSON blob, append-only)
- Schema: _dlt_version table (raw JSON, contains table→resource mapping)
- System: _dlt_loads (load history), _dlt_pipeline_state (state), _dlt_version (schema)

System tables are SHARED across pipelines in the same dataset — DELETE rows, don't DROP tables.

All destination SQL is canonical (DuckDB dialect, ``?`` placeholders) and goes
through the resolved ``DestinationAdapter``; identifier validation/quoting is
the adapter's ``render_identifier`` / ``render_table_ref``.
"""

import base64
import json
import logging
import os
import shutil
import time
import zlib
from pathlib import Path
from typing import Any

from dlt.common.pipeline import get_dlt_pipelines_dir

from dlt_ops import _compat
from dlt_ops.checkpoints import DEFAULT_CHECKPOINT_TABLE
from dlt_ops.destinations import DestinationAdapter, open_destination_boundary
from dlt_ops.discovery.models import SourceInfo
from dlt_ops.discovery.phase2 import introspect
from dlt_ops.runs import pipeline_name_for_source

logger = logging.getLogger(__name__)

# dlt system tables (shared across pipelines in a dataset)
DLT_SYSTEM_TABLES = ("_dlt_pipeline_state", "_dlt_loads", "_dlt_version")


class CleanupUnsupportedError(RuntimeError):
    """Installed dlt is outside the range cleanup's state handling is verified for."""


def _check_dlt_version() -> None:
    """Hard-fail outside the verified dlt range.

    Cleanup rewrites dlt-internal state tables; guessing against an unverified
    layout is how the wrong thing gets deleted, so there is no silent degrade.
    """
    installed = _compat.installed_dlt_version()
    if _compat.is_dlt_version_supported(installed):
        return
    raise CleanupUnsupportedError(
        f"cleanup is verified for dlt {_compat.supported_dlt_range()}; installed: {installed}. "
        "Cleanup rewrites dlt-internal state tables and refuses to guess against an unverified "
        "layout. For whole-pipeline removal on any dlt version, use dlt's own pipeline.drop()."
    )


def _pipeline_working_dir(pipeline_name: str) -> Path:
    """The pipeline's local working dir, resolved the way dlt resolves it (DLT_DATA_DIR aware)."""
    return Path(get_dlt_pipelines_dir()) / pipeline_name


def _decompress_dlt_state(compressed: str) -> dict:
    """Decode the _dlt_pipeline_state.state column.

    Primary format: zlib.compress(json_bytes, level=9) → base64 (verified on
    every supported destination/version). Raw JSON accepted as fallback.
    """
    try:
        state_bytes = base64.b64decode(compressed, validate=True)
        return json.loads(zlib.decompress(state_bytes))
    except Exception:
        return json.loads(compressed)


def _compress_dlt_state(state: dict) -> str:
    """Compress state dict to dlt format for _dlt_pipeline_state.state column."""
    json_bytes = json.dumps(state, default=str).encode("utf-8")
    return base64.b64encode(zlib.compress(json_bytes, level=9)).decode("ascii")


def _decode_dlt_schema(stored: str) -> dict:
    """Decode the _dlt_version.schema column.

    Primary format: raw JSON (verified on every supported destination/version —
    unlike the state blob, dlt stores schemas uncompressed). zlib+b64 accepted
    as fallback so the decode never tightens.
    """
    try:
        return json.loads(stored)
    except (TypeError, ValueError):
        return json.loads(zlib.decompress(base64.b64decode(stored, validate=True)))


def _mapping_from_schema(schema: dict) -> dict[str, str]:
    """{resource_name: table_name} from a decoded dlt schema, system tables excluded."""
    mapping = {}
    for table_name, table_data in schema.get("tables", {}).items():
        if table_name.startswith("_dlt_"):
            continue
        resource = table_data.get("resource", table_name)
        mapping[resource] = table_name
    return mapping


def _get_table_mapping_local(pipeline_name: str, schema_name: str) -> dict[str, str] | None:
    """Get resource→table mapping from local schema file.

    Returns: {resource_name: table_name} or None if not available.
    """
    schema_path = _pipeline_working_dir(pipeline_name) / "schemas" / f"{schema_name}.schema.json"
    if not schema_path.exists():
        return None

    try:
        with open(schema_path) as f:
            schema = json.load(f)
        return _mapping_from_schema(schema)
    except Exception as e:
        logger.warning(f"Failed to read local schema: {e}")
        return None


def _get_table_mapping_remote(
    adapter: DestinationAdapter, client: Any, dataset: str, schema_name: str
) -> dict[str, str] | None:
    """Get resource→table mapping from the destination's _dlt_version table.

    Decodes the schema stored in the latest _dlt_version row.
    Returns: {resource_name: table_name} or None if not available.
    """
    try:
        if not adapter.table_exists(client, dataset, "_dlt_version"):
            return None
        cursor = adapter.execute_query(
            client,
            f'SELECT "schema" FROM {adapter.render_table_ref(dataset, "_dlt_version")} '
            "WHERE schema_name = ? ORDER BY inserted_at DESC LIMIT 1",
            schema_name,
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return _mapping_from_schema(_decode_dlt_schema(row[0]))
    except Exception as e:
        logger.warning(f"Failed to read remote schema: {e}")
        return None


def _introspect_source(source: SourceInfo) -> SourceInfo:
    """Attach ``source_fn`` to a Phase-1-only record via the Phase-2 sandboxed import.

    Returns the record unchanged (no ``source_fn``) when introspection fails;
    the caller degrades to the naming convention.
    """
    if source.module_path is None:
        return source
    try:
        return introspect(source.path.parent, {source.name: source}).get(source.name, source)
    except Exception as e:
        logger.warning(f"Phase-2 introspection failed for source '{source.name}': {e}")
        return source


def _get_table_mapping_from_source(source: SourceInfo) -> dict[str, str]:
    """Get resource→table mapping by instantiating the source and reading table_name.

    Last resort. A Phase-1-only record is first enriched through the Phase-2
    sandboxed import; a source that cannot be imported — or whose
    instantiation raises — degrades to the resource_name == table_name
    convention with a warning.
    """
    if not source.is_introspected:
        source = _introspect_source(source)
    if not source.is_introspected:
        logger.warning(
            f"Source '{source.name}' is not importable ({source.import_error or 'no source_fn attached'}); "
            "falling back to the resource_name == table_name convention"
        )
        return {r: r for r in source.resources}

    try:
        src_instance = source.source_fn()
        mapping = {}
        for res_name in source.resources:
            if res_name in src_instance.resources:
                res = src_instance.resources[res_name]
                table_name = getattr(res, "table_name", res_name) or res_name
                mapping[res_name] = table_name
            else:
                mapping[res_name] = res_name
        return mapping
    except Exception as e:
        logger.warning(f"Failed to instantiate source for table mapping: {e}")
        # Ultimate fallback: resource name == table name
        return {r: r for r in source.resources}


def get_table_mapping(
    source: SourceInfo,
    pipeline_name: str,
    schema_name: str,
    dataset_name: str | None,
    adapter: DestinationAdapter | None = None,
    client: Any = None,
) -> dict[str, str]:
    """Get resource→table mapping with 3-tier fallback.

    1. Local schema file (fastest, no network)
    2. Destination _dlt_version table (works without local state)
    3. Source instantiation via Phase-2 import / naming convention (last resort)
    """
    # Tier 1: local schema
    mapping = _get_table_mapping_local(pipeline_name, schema_name)
    if mapping:
        logger.info(f"Table mapping from local schema: {len(mapping)} tables")
        return mapping

    # Tier 2: remote schema
    if dataset_name and adapter is not None and client is not None:
        mapping = _get_table_mapping_remote(adapter, client, dataset_name, schema_name)
        if mapping:
            logger.info(f"Table mapping from remote schema: {len(mapping)} tables")
            return mapping

    # Tier 3: source instantiation / convention
    mapping = _get_table_mapping_from_source(source)
    logger.info(f"Table mapping from source: {len(mapping)} tables")
    return mapping


def _get_remote_state(adapter: DestinationAdapter, client: Any, dataset: str, pipeline_name: str) -> dict | None:
    """Get latest pipeline state from the destination's _dlt_pipeline_state.

    Mirrors dlt's query: join with _dlt_loads (status=0), order by load_id DESC.
    Absent state tables mean no state — not an error.
    """
    if not (
        adapter.table_exists(client, dataset, "_dlt_pipeline_state")
        and adapter.table_exists(client, dataset, "_dlt_loads")
    ):
        return None
    cursor = adapter.execute_query(
        client,
        f"SELECT ps.state, ps.version, ps.version_hash "
        f"FROM {adapter.render_table_ref(dataset, '_dlt_pipeline_state')} AS ps "
        f"JOIN {adapter.render_table_ref(dataset, '_dlt_loads')} AS l ON l.load_id = ps._dlt_load_id "
        "WHERE ps.pipeline_name = ? AND l.status = 0 "
        "ORDER BY l.load_id DESC LIMIT 1",
        pipeline_name,
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return _decompress_dlt_state(row[0])


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


def _clean_remote_full(
    adapter: DestinationAdapter,
    client: Any,
    dataset: str,
    pipeline_name: str,
    schema_name: str,
    data_tables: list[str],
) -> list[str]:
    """Full remote cleanup: drop data tables + delete from system tables."""
    cleaned = []

    # Drop data tables
    for table in data_tables:
        try:
            adapter.drop_table_if_exists(client, dataset, table)
            cleaned.append(f"table: {table}")
            logger.info(f"Dropped table: {dataset}.{table}")
        except Exception as e:
            logger.warning(f"Failed to drop table {table}: {e}")

    # Delete from system tables (shared — DELETE rows, not DROP table).
    # Pipeline-scoped tables filter by pipeline_name; schema-scoped by schema_name.
    system_deletes = (
        ("_dlt_pipeline_state", "pipeline_name", pipeline_name),
        ("_dlt_version", "schema_name", schema_name),
        ("_dlt_loads", "schema_name", schema_name),
        (DEFAULT_CHECKPOINT_TABLE, "pipeline_name", pipeline_name),
    )
    for table, column, value in system_deletes:
        try:
            if _delete_rows_where(adapter, client, dataset, table, column, value):
                cleaned.append(f"state: {table} (rows deleted)")
                logger.info(f"Deleted {table} rows where {column} = {value}")
        except Exception as e:
            logger.warning(f"Failed to clean {table}: {e}")

    return cleaned


def _clean_remote_selective(
    adapter: DestinationAdapter,
    client: Any,
    dataset: str,
    pipeline_name: str,
    schema_name: str,
    resources: list[str],
    resource_tables: dict[str, str],
) -> list[str]:
    """Selective remote cleanup: drop specific tables + update pipeline state.

    resources: list of resource names to clean (for state/checkpoint removal).
    resource_tables: {resource_name: table_name} for resources with known tables.
    """
    cleaned = []

    # Drop specific data tables (only those with known mapping)
    for table_name in resource_tables.values():
        try:
            adapter.drop_table_if_exists(client, dataset, table_name)
            cleaned.append(f"table: {table_name}")
            logger.info(f"Dropped table: {dataset}.{table_name}")
        except Exception as e:
            logger.warning(f"Failed to drop table {table_name}: {e}")

    # Update pipeline state: remove ALL requested resource entries (not just those with tables)
    state = _get_remote_state(adapter, client, dataset, pipeline_name)
    if state:
        modified = False
        for source_name, source_data in state.get("sources", {}).items():
            resources_dict = source_data.get("resources", {})
            for resource_name in resources:
                if resource_name in resources_dict:
                    del resources_dict[resource_name]
                    modified = True
                    logger.info(f"Removed resource state: {source_name}.{resource_name}")

        if modified:
            # Bump version and re-encode the FULL decoded dict — the blob carries
            # destination-naming values (destination_name/type, dataset_name), so
            # surgery must roundtrip what was read, never template a fresh blob.
            state["_state_version"] = state.get("_state_version", 0) + 1
            compressed = _compress_dlt_state(state)

            # Insert updated state as new row (append-only table). A numeric
            # time_ns load_id sorts after every earlier dlt epoch-seconds id.
            load_id = str(time.time_ns())
            now = adapter.timestamp_now_sql

            adapter.execute_sql(
                client,
                f"INSERT INTO {adapter.render_table_ref(dataset, '_dlt_loads')} "
                f"(load_id, schema_name, status, inserted_at, schema_version_hash) VALUES (?, ?, 0, {now}, ?)",
                load_id,
                schema_name,
                state.get("_version_hash", ""),
            )
            adapter.execute_sql(
                client,
                f"INSERT INTO {adapter.render_table_ref(dataset, '_dlt_pipeline_state')} "
                "(version, engine_version, pipeline_name, state, created_at, version_hash, _dlt_load_id, _dlt_id) "
                f"VALUES (?, ?, ?, ?, {now}, ?, ?, ?)",
                state["_state_version"],
                state.get("_state_engine_version", 4),
                pipeline_name,
                compressed,
                state.get("_version_hash", ""),
                load_id,
                base64.b64encode(os.urandom(10)).decode("ascii"),
            )
            cleaned.append(f"state: updated (removed {len(resources)} resource(s))")
            logger.info("Updated pipeline state in destination")
    else:
        logger.info("No remote state found — nothing to update")

    # Clean checkpoints for ALL requested resources (not just those with tables)
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


def _clean_local_state_selective(working_dir: Path, schema_name: str, resources: list[str]) -> list[str]:
    """Remove specific resource entries from local state.json and schema."""
    cleaned = []
    state_path = working_dir / "state.json"

    if state_path.exists():
        try:
            with open(state_path) as f:
                state = json.load(f)

            modified = False
            for source_data in state.get("sources", {}).values():
                resources_dict = source_data.get("resources", {})
                for res in resources:
                    if res in resources_dict:
                        del resources_dict[res]
                        modified = True

            if modified:
                state["_state_version"] = state.get("_state_version", 0) + 1
                with open(state_path, "w") as f:
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

    ``destination`` is the dlt destination (name or factory) used to open the
    remote boundary for tier-2 mapping; without it the plan builds from
    local/source tiers only.

    Returns dict with: pipeline_name, schema_name, data_tables, resources, scope.
    """
    _check_dlt_version()

    pipeline_name = pipeline_name_for_source(source.name)
    schema_name = source.name
    working_dir = _pipeline_working_dir(pipeline_name)
    is_full = resources is None
    target_resources = resources or list(source.resources)

    table_mapping: dict[str, str] | None = None
    if remote and dataset_name and destination is not None:
        try:
            with open_destination_boundary(pipeline_name, destination, dataset_name) as (adapter, client):
                table_mapping = get_table_mapping(source, pipeline_name, schema_name, dataset_name, adapter, client)
        except Exception as e:
            logger.warning(f"Failed to open destination for cleanup plan: {e}")
    if table_mapping is None:
        table_mapping = get_table_mapping(source, pipeline_name, schema_name, None)

    # For full cleanup: show ALL tables in mapping (not just current resources)
    # For selective: show only requested resource tables
    if is_full:
        target_tables = list(table_mapping.values())
    else:
        target_tables = [table_mapping[r] for r in target_resources if r in table_mapping]

    return {
        "pipeline_name": pipeline_name,
        "schema_name": schema_name,
        "working_dir": working_dir,
        "local_exists": working_dir.exists(),
        "is_full": is_full,
        "target_resources": target_resources,
        "data_tables": target_tables,
        "table_mapping": table_mapping,
        "system_tables": list(DLT_SYSTEM_TABLES) if is_full else [],
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

    No dependency on dlt pipeline APIs — uses direct destination queries (via
    the DestinationAdapter boundary) and local file manipulation.

    Cleanup modes:
    - Full (resources=None): drop all data tables, delete system table rows, remove local dir
    - Selective (resources=[...]): drop specific tables, update state to remove resource entries

    Table identification uses 3-tier fallback:
    1. Local schema file (fastest)
    2. Destination _dlt_version table (works without local state)
    3. Source discovery (last resort)

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
    _check_dlt_version()

    result: dict[str, list[str]] = {"local": [], "remote": []}

    # Validate resources if specified
    if resources:
        available = set(source.resources)
        missing = set(resources) - available
        if missing:
            raise ValueError(f"Unknown resources: {missing}. Available: {sorted(available)}")

    pipeline_name = pipeline_name_for_source(source.name)
    schema_name = source.name
    working_dir = _pipeline_working_dir(pipeline_name)
    is_full = resources is None

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

        with open_destination_boundary(pipeline_name, destination, dataset_name) as (adapter, client):
            table_mapping = get_table_mapping(source, pipeline_name, schema_name, dataset_name, adapter, client)

            if is_full:
                all_tables = list(table_mapping.values())
                cleaned = _clean_remote_full(adapter, client, dataset_name, pipeline_name, schema_name, all_tables)
                result["remote"].extend(cleaned)
            else:
                assert resources is not None  # narrowed by is_full check
                resource_tables = {r: table_mapping[r] for r in resources if r in table_mapping}
                missing_tables = [r for r in resources if r not in table_mapping]
                if missing_tables:
                    logger.warning(f"No table mapping found for resources: {missing_tables}")
                cleaned = _clean_remote_selective(
                    adapter, client, dataset_name, pipeline_name, schema_name, resources, resource_tables
                )
                result["remote"].extend(cleaned)

    # Local cleanup AFTER remote (local schema needed for table mapping)
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
                cleaned = _clean_local_state_selective(working_dir, schema_name, resources)
                result["local"].extend(cleaned)
            else:
                logger.info("No local state to clean")

    return result
