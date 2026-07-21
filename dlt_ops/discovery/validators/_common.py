"""Shared AST-scan plumbing for rule modules (core and destination plugins)."""

import ast
import logging
from collections.abc import Callable, Iterator
from pathlib import Path

from dlt_ops.discovery.models import ValidationContext

logger = logging.getLogger(__name__)


def unique_pipeline_dirs(ctx: ValidationContext) -> dict[str, Path]:
    """Map pipeline name -> directory, deduped across multi-source pipelines."""
    return {source.pipeline_name: source.path for source in ctx.sources.values()}


def iter_py_files(pipeline_dir: Path) -> Iterator[Path]:
    for py_file in sorted(pipeline_dir.rglob("*.py")):
        if any(part in ("tests", "__pycache__") for part in py_file.parts):
            continue
        yield py_file


def parse_file(py_file: Path) -> ast.Module | None:
    try:
        return ast.parse(py_file.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as e:
        logger.debug(f"Failed to parse {py_file}: {e}")
        return None


def find_calls(tree: ast.Module, predicate: Callable[[ast.expr], bool]) -> list[ast.Call]:
    return [node for node in ast.walk(tree) if isinstance(node, ast.Call) and predicate(node.func)]


def get_keyword(call: ast.Call, name: str) -> ast.keyword | None:
    return next((kw for kw in call.keywords if kw.arg == name), None)


def rel(py_file: Path, ctx: ValidationContext) -> str:
    try:
        return str(py_file.relative_to(ctx.project_root))
    except ValueError:
        return py_file.name
