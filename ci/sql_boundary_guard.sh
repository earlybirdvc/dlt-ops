#!/usr/bin/env bash
# SQL-boundary guard — fails when package code speaks raw destination SQL
# instead of going through the DestinationAdapter boundary
# (dlt_ops/destinations/).
#
# Patterns:  raw dlt sql_client acquisition (`sql_client(`), raw client
#            execution (`.execute_sql(` / `.execute_query(`), and the EE
#            BigQuery helper calls (`bq_client(` / `bq_execute(` / `bq_query(`).
#            Calls whose receiver names an adapter (`adapter.execute_sql(...)`,
#            `self._adapter.execute_query(...)`) are the boundary itself and
#            are exempt.
# Scanned:   dlt_ops/ minus dlt_ops/destinations/ (adapters are the
#            one place allowed to touch the raw client). Tests are out of
#            scope: the test harness legitimately builds clients to hand to
#            adapters and to verify destination state directly.
# Allowlist: ci/sql-boundary-allow.txt — `<path>:<ticket>` entries. Hits in
#            allowlisted files are tolerated until their port ticket lands; an
#            allowlisted file with NO remaining hits fails the run (stale
#            entry), so the list only shrinks.
#
# Usage: bash ci/sql_boundary_guard.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

allow_file="ci/sql-boundary-allow.txt"

raw_pattern='sql_client\(|\.execute_sql\(|\.execute_query\(|bq_client\(|bq_execute\(|bq_query\('
adapter_call_pattern='[A-Za-z0-9_.]*adapter[A-Za-z0-9_]*\.execute_(sql|query)\('

# Hit lines are `path:lineno:content`; paths are repo-relative because we
# scan relative paths from the repo root.
hits="$(grep -rnE "$raw_pattern" \
  --include='*.py' \
  --exclude-dir=__pycache__ --exclude-dir=destinations \
  dlt_ops | { grep -viE "$adapter_call_pattern" || true; })"

allow_entries="$(grep -vE '^[[:space:]]*(#|$)' "$allow_file" || true)"

bad_entries="$(printf '%s\n' "$allow_entries" | { grep -vE '^[^:]+:OSS-[0-9]{3}$' || true; } | sed '/^$/d')"
if [ -n "$bad_entries" ]; then
  echo "MALFORMED allowlist entries in $allow_file (want <path>:OSS-NNN):" >&2
  printf '%s\n' "$bad_entries" >&2
  exit 1
fi

hit_files="$(printf '%s\n' "$hits" | cut -d: -f1 | sed '/^$/d' | sort -u)"
allowed_files="$(printf '%s\n' "$allow_entries" | cut -d: -f1 | sed '/^$/d' | sort -u)"

violation_files="$(comm -23 <(printf '%s\n' "$hit_files") <(printf '%s\n' "$allowed_files"))"
stale_files="$(comm -13 <(printf '%s\n' "$hit_files") <(printf '%s\n' "$allowed_files"))"

fail=0

if [ -n "$violation_files" ]; then
  echo "Raw destination SQL outside dlt_ops/destinations/ (route through DestinationAdapter):" >&2
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    if printf '%s\n' "$violation_files" | grep -qxF "${line%%:*}"; then
      printf '%s\n' "$line" >&2
    fi
  done <<< "$hits"
  fail=1
fi

if [ -n "$stale_files" ]; then
  while IFS= read -r file; do
    [ -z "$file" ] && continue
    echo "STALE allowlist entry (no hits left) — remove '$file' from $allow_file" >&2
  done <<< "$stale_files"
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "SQL-boundary guard: FAIL" >&2
  exit 1
fi

echo "SQL-boundary guard: OK ($(printf '%s\n' "$allowed_files" | sed '/^$/d' | wc -l | tr -d ' ') allowlisted files still pending port)"
