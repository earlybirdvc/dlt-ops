# Terminal recordings for the docs

The GIFs under `docs/assets/terminal/` are real [VHS](https://github.com/charmbracelet/vhs) recordings of the
CLI running the repo's example projects against local DuckDB, fully offline, inside Docker
(`ghcr.io/charmbracelet/vhs:v0.11.0` + uv-managed venv; the host needs Docker only).

Regenerate all three (or pass tape names): `tapes/render.sh [quickstart checkpoint-resume backfill]`
