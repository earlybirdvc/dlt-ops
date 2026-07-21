"""Dump dlt state-table schemas + semantics for one destination.

Run inside a throwaway venv that has the dlt version under test:

    uv venv v129 && uv pip install -p v129/bin/python "dlt[duckdb,postgres,bigquery]~=1.29.0"
    v129/bin/python ci/dump_state_schema.py --dest duckdb  --out ci/state-schema-dumps/duckdb_129.json
    v129/bin/python ci/dump_state_schema.py --dest postgres --pg-dsn postgresql://postgres:spike@localhost:55432/postgres --out ci/state-schema-dumps/postgres_129.json
    v129/bin/python ci/dump_state_schema.py --dest bigquery --dataset dltx_spike_<ts> --out ci/state-schema-dumps/bigquery_129.json

What it does:
1. Runs a test pipeline TWICE (one incremental resource + one replace resource)
   so `_dlt_pipeline_state` has multiple versions and `_dlt_loads` has history.
2. Dumps information_schema columns for `_dlt_pipeline_state`, `_dlt_loads`,
   `_dlt_version` + all rows (state/schema blobs truncated in the dump).
3. Verifies the zlib+b64 state codec (copied from
   dlt_ops/discovery/cleanup.py::_decompress_dlt_state — do not import
   package internals here; the script must run on bare `dlt[...]` venvs),
   records `engine_version` values, `load_id` ordering semantics and the
   `status` domain of `_dlt_loads`.

Output JSON is stable-ordered so two dumps diff mechanically — re-run on every
dlt pin bump and diff against the committed dumps in ci/state-schema-dumps/.
"""

from __future__ import annotations

import argparse
import base64
import datetime
import json
import sys
import zlib
from pathlib import Path

import dlt

STATE_TABLES = ("_dlt_loads", "_dlt_pipeline_state", "_dlt_version")
BLOB_COLUMNS = {"state", "schema"}  # truncated in row dumps
TRUNCATE_AT = 48


# --- codec copied from dlt_ops/discovery/cleanup.py (_decompress_dlt_state) ---
def decode_state_blob(compressed: str) -> tuple[dict, str]:
    """Returns (decoded_json, format) where format is 'zlib+b64' or 'raw-json'."""
    try:
        state_bytes = base64.b64decode(compressed, validate=True)
        decompressed = zlib.decompress(state_bytes)
        return json.loads(decompressed), "zlib+b64"
    except Exception:
        return json.loads(compressed), "raw-json"


@dlt.source(name="spike_src")
def spike_src(run_no: int):
    @dlt.resource(write_disposition="append", primary_key="id")
    def events(cursor=dlt.sources.incremental("id", initial_value=0)):
        hi = 5 if run_no == 1 else 10  # run 2 re-yields 1..10; incremental keeps 6..10
        for i in range(1, hi + 1):
            yield {"id": i, "payload": f"evt-{i}"}

    @dlt.resource(write_disposition="replace")
    def dims():
        n = 3 if run_no == 1 else 4
        for i in range(n):
            yield {"code": chr(97 + i), "run_no": run_no}

    return events, dims


def build_destination(args):
    if args.dest == "duckdb":
        return dlt.destinations.duckdb(str(Path(args.workdir) / f"spike_{args.label}.duckdb"))
    if args.dest == "postgres":
        return dlt.destinations.postgres(args.pg_dsn)
    if args.dest == "bigquery":
        kwargs = {}
        if args.bq_location:
            kwargs["location"] = args.bq_location
        return dlt.destinations.bigquery(**kwargs)  # Application Default Credentials
    raise SystemExit(f"unknown dest {args.dest}")


def information_schema_sql(dest: str, dataset: str, table: str) -> str:
    cols = "column_name, data_type, is_nullable"
    if dest == "bigquery":
        return (
            f"SELECT {cols} FROM {dataset}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' ORDER BY ordinal_position"
        )
    return (
        f"SELECT {cols} FROM information_schema.columns "
        f"WHERE table_schema = '{dataset}' AND table_name = '{table}' ORDER BY ordinal_position"
    )


def dump_rows(client, table: str, order_by: str) -> dict:
    qualified = client.make_qualified_table_name(table)
    with client.execute_query(f"SELECT * FROM {qualified} ORDER BY {order_by}") as cur:
        columns = [d[0] for d in cur.description]
        raw_rows = cur.fetchall()
    rows = []
    for raw in raw_rows:
        row = {}
        for col, val in zip(columns, raw):
            if col in BLOB_COLUMNS and isinstance(val, str):
                row[col] = f"<{len(val)} chars> {val[:TRUNCATE_AT]}..."
            else:
                row[col] = str(val) if not isinstance(val, (int, float, type(None))) else val
        rows.append(row)
    return {
        "columns": columns,
        "rows": rows,
        "raw_blobs": [r[columns.index("state")] for r in raw_rows] if "state" in columns else None,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dest", required=True, choices=["duckdb", "postgres", "bigquery"])
    p.add_argument("--label", default="spike", help="suffix for duckdb file / pipelines dir")
    p.add_argument("--dataset", default="dltx_spike")
    p.add_argument("--workdir", default="/tmp/dltx-spike")
    p.add_argument("--pg-dsn", default="postgresql://postgres:spike@localhost:55432/postgres")
    p.add_argument("--bq-location", default=None)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name="dltx_spike",
        destination=build_destination(args),
        dataset_name=args.dataset,
        pipelines_dir=str(Path(args.workdir) / "pipelines" / f"{args.dest}_{args.label}"),
        dev_mode=False,
    )
    for run_no in (1, 2):
        info = pipeline.run(spike_src(run_no))
        print(f"run {run_no}: loads={info.loads_ids}", file=sys.stderr)

    result: dict = {
        "dlt_version": dlt.__version__,
        "destination": args.dest,
        "dataset": args.dataset,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "columns": {},
        "rows": {},
        "checks": {},
    }

    with pipeline.sql_client() as client:
        for table in STATE_TABLES:
            with client.execute_query(information_schema_sql(args.dest, args.dataset, table)) as cur:
                result["columns"][table] = [
                    {"name": r[0], "type": str(r[1]), "nullable": str(r[2])} for r in cur.fetchall()
                ]
        loads = dump_rows(client, "_dlt_loads", "load_id")
        state = dump_rows(client, "_dlt_pipeline_state", "version")
        version = dump_rows(client, "_dlt_version", "version")
        result["rows"] = {"_dlt_loads": loads, "_dlt_pipeline_state": state, "_dlt_version": version}

        # --- semantics checks -------------------------------------------------
        # state codec: decode every state blob; record engine versions
        state_checks = []
        for blob in state.pop("raw_blobs") or []:
            decoded, fmt = decode_state_blob(blob)
            state_checks.append(
                {
                    "format": fmt,
                    "_state_version": decoded.get("_state_version"),
                    "_state_engine_version": decoded.get("_state_engine_version"),
                    "top_level_keys": sorted(decoded.keys()),
                    "sources_resources": {
                        s: sorted(d.get("resources", {}).keys()) for s, d in decoded.get("sources", {}).items()
                    },
                }
            )
        result["checks"]["state_codec"] = state_checks
        loads.pop("raw_blobs", None)
        version.pop("raw_blobs", None)

        # _dlt_version.schema codec (cleanup tier-2 mapping decodes this too)
        qualified = client.make_qualified_table_name("_dlt_version")
        with client.execute_query(f"SELECT * FROM {qualified} ORDER BY version") as cur:
            cols = [d[0] for d in cur.description]
            schema_idx = cols.index("schema")
            schema_checks = []
            for raw in cur.fetchall():
                decoded, fmt = decode_state_blob(raw[schema_idx])
                schema_checks.append(
                    {
                        "format": fmt,
                        "schema_engine_version": decoded.get("engine_version"),
                        "tables_with_resource": {
                            t: d.get("resource")
                            for t, d in decoded.get("tables", {}).items()
                            if not t.startswith("_dlt_")
                        },
                    }
                )
        result["checks"]["version_schema_codec"] = schema_checks

        # load_id ordering: lexicographic == numeric == inserted_at order?
        load_ids = [r["load_id"] for r in loads["rows"]]
        result["checks"]["load_id_ordering"] = {
            "load_ids": load_ids,
            "lexicographic_equals_numeric": sorted(load_ids) == sorted(load_ids, key=float),
            "inserted_at_monotonic_with_load_id": (
                [r["inserted_at"] for r in sorted(loads["rows"], key=lambda r: r["load_id"])]
                == sorted(r["inserted_at"] for r in loads["rows"])
            ),
        }
        result["checks"]["status_domain"] = sorted({r["status"] for r in loads["rows"]})
        # state row engine_version column vs in-blob _state_engine_version
        result["checks"]["engine_version_column"] = sorted({r["engine_version"] for r in state["rows"]})

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
    print(f"wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
