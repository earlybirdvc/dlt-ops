import importlib.util
import os
import subprocess
import time
from pathlib import Path
from textwrap import dedent

import pytest


@pytest.fixture(autouse=True)
def _isolate_dlt_project_dir():
    """discover_sources setdefaults DLT_PROJECT_DIR; don't leak it across tests."""
    before = os.environ.get("DLT_PROJECT_DIR")
    os.environ.pop("DLT_PROJECT_DIR", None)
    yield
    if before is None:
        os.environ.pop("DLT_PROJECT_DIR", None)
    else:
        os.environ["DLT_PROJECT_DIR"] = before


@pytest.fixture(scope="session")
def postgres_url():
    """Connection URL for a live Postgres, for integration-marked tests.

    Resolution order: the POSTGRES_URL env var (CI service container / local
    override), else a throwaway ``postgres:16`` docker container torn down
    after the session, else skip. psycopg2 (the ``[postgres]`` extra) gates
    everything so the credential-free lane never touches docker.
    """
    if importlib.util.find_spec("psycopg2") is None:
        pytest.skip("psycopg2 not installed — sync with the [postgres] extra to run Postgres tests")

    env_url = os.environ.get("POSTGRES_URL")
    if env_url:
        yield env_url
        return

    container = f"dltx-test-pg-{os.getpid()}"
    command = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container,
        "-e",
        "POSTGRES_PASSWORD=test",
        "-p",
        "55433:5432",
        "postgres:16",
    ]
    try:
        started = subprocess.run(command, capture_output=True, text=True)
    except FileNotFoundError:
        pytest.skip("no POSTGRES_URL and no docker CLI — skipping Postgres integration tests")
    if started.returncode != 0:
        pytest.skip(f"no POSTGRES_URL and docker unavailable: {started.stderr.strip()}")

    url = "postgresql://postgres:test@localhost:55433/postgres"
    try:
        import psycopg2

        deadline = time.monotonic() + 90
        while True:
            try:
                psycopg2.connect(url).close()
                break
            except psycopg2.OperationalError:
                if time.monotonic() > deadline:
                    pytest.fail(f"postgres container {container} not ready within 90s")
                time.sleep(0.5)
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)


@pytest.fixture
def make_project(tmp_path):
    """Factory for neutral dlt-ops project trees under tmp_path.

    `config` is the .dlt/config.toml body; `files` maps project-relative
    paths to file bodies (both dedented before writing), e.g.
    {"web_events/source/page_views.py": "..."}.
    """

    def _make(
        config: str = "[dlt_ops]\n",
        files: dict[str, str] | None = None,
        name: str = "project",
    ) -> Path:
        root = tmp_path / name
        (root / ".dlt").mkdir(parents=True)
        (root / ".dlt" / "config.toml").write_text(dedent(config))
        for relpath, body in (files or {}).items():
            path = root / relpath
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(dedent(body))
        return root

    return _make
