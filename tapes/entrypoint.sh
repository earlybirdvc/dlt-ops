#!/bin/bash
# Sync the mounted repo into the prebaked venv, then hand off to vhs.
set -euo pipefail
uv sync --frozen --no-dev --extra duckdb --inexact --project /repo --quiet
mkdir -p "${DLT_DATA_DIR:-/tmp/dlt-scratch}"
exec /usr/bin/vhs "$@"
