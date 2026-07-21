"""Shared plumbing for the additive + removal detectors.

Both detectors need the same handful of primitives: the source's ignored
(injected) column set, a lookup from ``SourceInfo`` + resource name to the
Pydantic model dlt attached via ``columns=<Model>``, canonical-dialect
identifier quoting, and the ``reproduce_sql`` alert sinks attach to every
drift event.

All SQL fragments produced here are CANONICAL (DuckDB dialect, double-quoted
identifiers) per the DestinationAdapter boundary contract — the adapter owns
transpilation to the destination-native dialect.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Protocol

from dlt.common.normalizers.naming.snake_case import NamingConvention

from dlt_ops.discovery.models import SourceInfo
from dlt_ops.pydantic_fields import extract_model_column_names

if TYPE_CHECKING:
    from datetime import datetime

    import pydantic

    from dlt_ops.config import ProjectConfig


logger = logging.getLogger(__name__)


class _Normalizer(Protocol):
    """Structural view of the identifier normalizer contract.

    Parameter name mirrors ``dlt.common.normalizers.naming.NamingConvention``
    (``identifier``, not ``name``) so a real dlt NamingConvention structurally
    satisfies this Protocol under strict type checkers that enforce
    positional-or-keyword parameter-name compatibility.
    """

    def normalize_identifier(self, identifier: str) -> str: ...


# Fallback when a source instance cannot be built (test fakes, orphan sources,
# a source_fn that raises before the schema is attached). dlt's default is
# snake_case, so this fallback matches the real write path for any pipeline
# without a custom convention — but the primary path derives the exact
# NamingConvention from the source's own dlt Schema so a custom convention
# cannot silently diverge from what the destination persists.
_DEFAULT_NAMING: _Normalizer = NamingConvention()

# Conservative identifier grammar shared with the first-party destination
# adapters' render_identifier (see destinations/_base.py): dlt-normalized
# datasets/tables/columns are snake_case, and anything outside it would not
# be portable across destinations anyway.
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_]+")


def resolve_source_naming(source: SourceInfo) -> _Normalizer:
    """Return the NamingConvention the source's dlt Schema uses on writes.

    A ``DltSource`` carries its own ``dlt.common.schema.Schema`` whose
    ``.naming`` reflects the destination-side normalizer dlt will apply to
    every column on the write. Reading it here means the reconciler stays
    correct even if a source ever opts into a custom convention
    (``[schema] naming = "..."`` or a schema built with an override in
    Python) — a hardcoded constant in a generic package is a smell.

    Falls back to the default snake_case convention when the source cannot
    be instantiated or exposes no schema (test fakes, orphan sources). The
    fallback matches the write path of any pipeline without a naming
    override; it would only diverge in the exact scenario the guard is
    designed for — a custom naming convention — and even then it surfaces
    as loud false-positive drift (the same class this module already
    emits), not silent data corruption.
    """
    try:
        src_instance: Any = source.source_fn()
    except Exception:
        logger.debug("source_fn() failed while resolving naming for source=%s; using default", source.name)
        return _DEFAULT_NAMING
    schema = getattr(src_instance, "schema", None)
    naming = getattr(schema, "naming", None)
    if naming is None:
        return _DEFAULT_NAMING
    return naming


def destination_column_names(
    model: "type[pydantic.BaseModel]",
    naming: "_Normalizer | None" = None,
) -> set[str]:
    """Return the set of destination column names a model's fields will produce.

    A Pydantic attribute `startTime` lands at the destination as `start_time`;
    a `Field(alias="FROM")` alias lands as `from`. Callers (the additive and
    removal detectors) diff against live destination columns, which are
    already destination-side — so we mirror the write path here.

    Every candidate raw-payload key (attribute name + alias +
    string-valued validation_alias, via ``extract_model_column_names``) is run
    through the same normalizer dlt uses on the write. Set semantics dedupe
    collisions (e.g. a snake_case attribute plus an uppercased alias that
    normalizes to the same name).

    ``naming`` is injected by the caller from ``resolve_source_naming(source)``
    so every source uses its own dlt Schema's convention, not a hardcoded one.
    When omitted (fast-path callers, tests), falls back to the default
    snake_case convention.

    Reconciler-scoped on purpose. ``pydantic_fields.extract_model_column_names``
    stays the source-side view because ``drop_unknown_nulls`` runs at ingest
    time, where the raw payload speaks source-side keys.
    """
    normalizer = naming if naming is not None else _DEFAULT_NAMING
    return {normalizer.normalize_identifier(name) for name in extract_model_column_names(model)}


def configured_load_timestamp_column(project_config: "ProjectConfig") -> str | None:
    """Non-empty project-level ``[dlt_ops] load_timestamp_column``, else None.

    When set, the runner stamps it on every row, the removal
    detector windows on it, additive sampling orders by it, and the ignored
    set auto-registers it. Unset / empty / non-string = feature off.
    """
    column = project_config.raw.get("load_timestamp_column")
    if isinstance(column, str) and column.strip():
        return column.strip()
    return None


def ignored_columns_for(
    source: SourceInfo,
    naming: "_Normalizer",
    project_config: "ProjectConfig",
) -> frozenset[str]:
    """The live destination columns the additive diff must NOT treat as drift.

    Union of:

    - project-level ``[dlt_ops] injected_columns`` — keys the project
      stamps on every row of every source;
    - per-source ``[sources.<X>.dlt_ops] injected_columns`` — keys one
      source's aggregator or resource stamps on its rows;
    - the configured ``load_timestamp_column`` — auto-registered because the
      runner stamps it (never part of the Pydantic model by design).

    ``source.config`` may be ``None`` for orphan/misconfigured sources; the
    union then degenerates to the project-level pieces alone, and drift
    detection still runs.

    Each key is run through the source's own destination-side ``naming``
    convention so it matches the persisted column shape — a camelCase
    injected key ``sessionId`` declared in TOML would otherwise mismatch the
    ``session_id`` column dlt writes and surface as false-positive drift.
    """
    raw_project = project_config.raw.get("injected_columns")
    project_level = tuple(c for c in raw_project if isinstance(c, str)) if isinstance(raw_project, list) else ()
    per_source: tuple[str, ...] = source.config.injected_columns if source.config else ()
    load_ts = configured_load_timestamp_column(project_config)
    raw = frozenset(project_level) | frozenset(per_source) | (frozenset({load_ts}) if load_ts else frozenset())
    return frozenset(naming.normalize_identifier(name) for name in raw)


def canonical_ident(ident: str) -> str:
    """Validate ``ident`` against the shared identifier grammar and quote it.

    Canonical (DuckDB) double-quoting — the adapter's transpile step converts
    it to the destination-native quoting. Quoting defends against a drifted
    column name colliding with a reserved SQL keyword; the grammar check
    rejects anything that could break out of the quotes. A rejected name
    raises ``ValueError`` and lands in the caller's per-resource error
    isolation instead of reaching SQL text.
    """
    if not isinstance(ident, str) or not _IDENTIFIER_RE.fullmatch(ident):
        raise ValueError(f"invalid identifier {ident!r}: must match {_IDENTIFIER_RE.pattern}")
    return f'"{ident}"'


def canonical_table_ref(dataset: str, table: str) -> str:
    """``dataset.table`` reference in canonical form; both parts validated."""
    return f"{canonical_ident(dataset)}.{canonical_ident(table)}"


def resource_pydantic_model(source: SourceInfo, resource_name: str) -> "type[pydantic.BaseModel] | None":
    """Return the Pydantic model attached to ``columns=`` on a dlt resource.

    dlt wraps the user's model in a ``dlt.common.libs.pydantic.<Name>ExtraAllow``
    subclass (extra="allow") for its own validation. That subclass inherits
    ``model_fields`` from the original, so ``extract_model_column_names``
    still works against it — we return whatever dlt exposes.

    Returns ``None`` when the resource has no Pydantic model attached
    (e.g. ``columns=`` given as a dict, or omitted entirely). Both detectors
    treat ``None`` as "nothing to diff against" and skip the resource.
    """
    try:
        src_instance = source.source_fn()
    except Exception:
        logger.exception("source_fn() failed for source=%s", source.name)
        raise

    resource = src_instance.resources.get(resource_name)
    if resource is None:
        return None

    validator = getattr(resource, "validator", None)
    if validator is None:
        return None
    return getattr(validator, "model", None)


def build_reproduce_sql(
    dataset: str,
    table: str,
    columns: tuple[str, ...],
    *,
    first_seen_at: "datetime",
    load_timestamp_column: str | None,
) -> str:
    """Build the copy-pasteable canonical-dialect SELECT attached to a finding.

    Owned by the reconciler (not the emitter) so alert sinks stay pure
    serializers with no SQL knowledge — the result rides on
    ``DriftFinding.reproduce_sql``. When no load-timestamp column is
    configured there is no time axis to anchor on, so the time predicate is
    dropped and the SELECT degrades to a plain ``LIMIT 5`` preview.
    """
    preview_columns = list(columns[:5])
    projection = ", ".join(canonical_ident(col) for col in preview_columns) if preview_columns else "*"
    time_predicate = ""
    if load_timestamp_column:
        time_predicate = f"WHERE {canonical_ident(load_timestamp_column)} >= TIMESTAMP '{first_seen_at.isoformat()}' "
    return f"SELECT {projection} FROM {canonical_table_ref(dataset, table)} {time_predicate}LIMIT 5"
