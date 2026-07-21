"""Reconciler data models.

Cross-module contracts for the reconciler. Kept as frozen ``attrs`` classes to
match the ``dlt_ops`` convention (see ``discovery/models.py``).

``DriftFinding`` is what the additive + removal detectors emit and alert sinks
consume. ``ReconcileResult`` is what the public API returns per source run —
the caller (CLI, orchestrator task) can log the counts, and pytest fixtures
can assert on the finding tuples.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Any

import attrs


class DriftKind(StrEnum):
    """Closed set of drift categories the reconciler emits.

    Serialised as its string value on the alert event's ``drift_type`` tag
    (StrEnum stringifies to the value), which keeps the wire format identical
    to the old ``Literal["additive", "removal"]`` while making the closed set
    self-documenting inside Python. Downstream consumers compare against
    ``DriftKind.ADDITIVE`` / ``DriftKind.REMOVAL``.
    """

    ADDITIVE = "additive"
    REMOVAL = "removal"


@attrs.frozen
class DriftFinding:
    """One drifted resource surfaced by the reconciler.

    ``kind`` disambiguates additive (the destination has a column the model
    doesn't) from removal (a model column's non-null coverage collapsed to
    ~0). ``columns`` is the full drifted set — additive detection emits one
    finding per drifted RESOURCE with all its drifted columns, not one
    finding per column, so downstream alert issues collapse cleanly.

    ``sample_values`` is a mapping keyed by column name → up to 5 recent
    values from the resource's destination table. Empty for removal findings
    (there are no recent non-null values to sample by definition).
    ``inferred_types`` is the destination-reported ``data_type`` string per
    column, positionally aligned to ``columns``.

    ``reproduce_sql`` is a copy-pasteable canonical-dialect SELECT computed at
    detection time (where the resolved dataset and load-timestamp column are
    known) so alert sinks stay pure serializers with no SQL knowledge.
    """

    kind: DriftKind
    pipeline_name: str
    source_name: str
    resource_name: str
    columns: tuple[str, ...]
    inferred_types: tuple[str, ...]
    sample_values: Mapping[str, list[Any]]
    first_seen_at: datetime
    reproduce_sql: str | None = None


@attrs.frozen
class ReconcileResult:
    """Outcome of one ``reconcile_source`` / ``detect_removal`` call.

    ``findings`` is the tuple of per-resource findings surfaced during the
    run. ``duration_ms`` is wall-clock time including destination round-trips
    and alert emission (or ``0`` when the runner skipped everything on an
    early failure). ``error`` is populated only when the top-level
    source-scan itself failed — per-resource failures are wrapped and
    reported through the alert sink's error path, and do not surface here so
    an orchestrated sweep can succeed with partial coverage. ``warnings``
    carries non-fatal degradations (e.g. removal detection skipped because no
    load-timestamp column is configured).
    """

    source_name: str
    findings: tuple[DriftFinding, ...]
    duration_ms: int
    error: str | None = None
    warnings: tuple[str, ...] = ()
