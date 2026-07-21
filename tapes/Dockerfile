# VHS runner for the docs terminal recordings (see tapes/README.md).
# Build from the repo root:  docker build -f tapes/Dockerfile -t dlt-ops-vhs .
FROM ghcr.io/charmbracelet/vhs:v0.11.0

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# gifsicle: post-render GIF optimization (tapes/render.sh).
RUN apt-get update \
    && apt-get install -y --no-install-recommends gifsicle \
    && rm -rf /var/lib/apt/lists/*

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    DLT_DATA_DIR=/tmp/dlt-scratch \
    PYTHONPYCACHEPREFIX=/tmp/pycache \
    PATH=/opt/venv/bin:$PATH

# The repo pins its interpreter in .python-version — install that managed build.
COPY .python-version /prebake/.python-version
RUN uv python install "$(cat /prebake/.python-version)"

# Prebake the locked dependency set into /opt/venv so the runtime sync against
# the mounted repo only has to (re)install the editable dlt-ops itself.
COPY pyproject.toml uv.lock LICENSE /prebake/
COPY dlt_ops /prebake/dlt_ops
# psutil quiets dlt's progress-logger warning; the runtime sync is --inexact
# so this extra package survives.
RUN cd /prebake && uv sync --frozen --no-dev --extra duckdb \
    && uv pip install --python /opt/venv/bin/python psutil

COPY tapes/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
