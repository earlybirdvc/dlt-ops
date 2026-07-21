#!/bin/bash
# Off-screen setup for backfill.tape: builds the project state that
# docs/guides/backfill.md step 1 walks through (scaffold + the page's
# web_events source + its config block), so the recording starts at validate.
set -euo pipefail
dlt-ops init demo --example
cd demo
mkdir -p web/source
cp /repo/tapes/backfill_web_events.py web/source/web_events.py
cat >> .dlt/config.toml <<'EOF'

[sources.web_events]

[sources.web_events.dlt_ops]
schedule = "@manual"
dataset = "web_raw"
EOF
