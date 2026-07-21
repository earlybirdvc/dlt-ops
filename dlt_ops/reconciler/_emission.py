"""Alert emission for the reconciler: sink resolution, fan-out, isolation.

The reconciler core emits every event through the ``AlertSink`` protocol
(``protocols.AlertSink`` — the alert-sink plugin axis contract), so no
alerting SDK ever loads on the core path. This module holds:

- ``LoggingAlertSink`` — the first-party core sink (structured log lines,
  no-op flush), registered as the ``logging`` entry point on the
  ``dlt_ops.alert_sink`` axis and the default when no ``alert_sinks``
  key is configured.
- ``NullAlertSink`` — the ``--dry-run`` sink: suppresses all emission.
- ``MultiAlertSink`` — fan-out over the configured sinks with per-sink
  isolation: one sink raising never crashes another sink nor the caller.
- ``resolve_sink`` — config+registry-driven resolution, once per public
  reconcile invocation: each name in ``[dlt_ops] alert_sinks`` resolves
  through the plugin registry, gets constructed with its
  ``[dlt_ops.alert_sink.<name>]`` options as keyword arguments, and is
  wrapped in a ``MultiAlertSink``.
- ``emit_findings`` — the per-finding loop with isolation: one emission
  failure never crashes the sweep.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dlt_ops.config import (
    DEFAULT_ALERT_SINKS,
    ProjectConfig,
    find_project_root,
    load_project_config,
)
from dlt_ops.plugins import registry as _plugins

if TYPE_CHECKING:
    from pathlib import Path

    from dlt_ops.reconciler.protocols import AlertSink

logger = logging.getLogger(__name__)

# Entry-point group suffix for alert-sink plugins (see plugins.registry.AXES).
ALERT_SINK_AXIS = "alert_sink"

# Distinct error contexts for a sink-side failure on each detector's findings,
# so one detector's emission bug never dedupes with the other's.
_EMIT_DRIFT_CONTEXT_ADDITIVE = "emit_drift"
_EMIT_DRIFT_CONTEXT_REMOVAL = "emit_drift_removal"

# The sink-protocol default so sinks with real transport queues (e.g. the
# Sentry sink in dlt_ops.sentry) drain deterministically before an
# orchestrated task's teardown.
FLUSH_TIMEOUT_SECONDS = 2.0


class LoggingAlertSink:
    """Core sink (entry point ``logging``): one structured log line per event.

    Always available — it ships with the package and needs no configuration,
    so a zero-config project still surfaces drift in its logs. Nothing to
    flush.
    """

    def emit_drift(self, finding) -> None:
        logger.warning(
            "schema drift (%s): %s.%s.%s — %d column(s): %s | reproduce: %s",
            finding.kind,
            finding.pipeline_name,
            finding.source_name,
            finding.resource_name,
            len(finding.columns),
            ", ".join(finding.columns[:5]) + ("..." if len(finding.columns) > 5 else ""),
            finding.reproduce_sql,
        )

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None:
        logger.error(
            "reconciler error: source=%s resource=%s context=%s",
            source_name,
            resource_name,
            context,
            exc_info=exc,
        )

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> None:
        return None


class NullAlertSink:
    """``--dry-run`` sink: all emission suppressed (findings still returned)."""

    def emit_drift(self, finding) -> None:
        return None

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None:
        return None

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> None:
        return None


class MultiAlertSink:
    """Fan-out over the configured sinks: every sink gets every event.

    Per-sink isolation: a raising sink is logged and never prevents the
    remaining sinks from receiving the event. ``emit_error`` and ``flush``
    are terminal — they swallow per-sink failures entirely, so the
    reconciler's unguarded ``sink.emit_error(...)`` call sites can never be
    crashed by a plugin. ``emit_drift`` re-raises the first per-sink failure
    AFTER the full fan-out completes, so its one caller (``emit_findings``)
    can report the failure through the sink's own error path — preserving
    the single-sink contract where a failed drift emission becomes a
    reconciler-error event.
    """

    def __init__(self, sinks: "list[tuple[str, AlertSink]] | tuple[tuple[str, AlertSink], ...]") -> None:
        self._sinks = tuple(sinks)

    @property
    def sinks(self) -> "tuple[tuple[str, AlertSink], ...]":
        """Resolved ``(name, sink)`` pairs, in configuration order."""
        return self._sinks

    def emit_drift(self, finding) -> None:
        first_failure: Exception | None = None
        for name, sink in self._sinks:
            try:
                sink.emit_drift(finding)
            except Exception as exc:
                logger.exception("alert sink %r emit_drift raised — remaining sinks still receive the event", name)
                if first_failure is None:
                    first_failure = exc
        if first_failure is not None:
            raise first_failure

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None:
        for name, sink in self._sinks:
            try:
                sink.emit_error(exc, source_name=source_name, resource_name=resource_name, context=context)
            except Exception:
                logger.exception("alert sink %r emit_error raised — remaining sinks still receive the event", name)

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> None:
        for name, sink in self._sinks:
            try:
                sink.flush(timeout=timeout)
            except Exception:
                logger.exception("alert sink %r flush raised — some of its events may not have shipped", name)


def _config_for_sinks(project_config: ProjectConfig | None, project_root: "Path | None") -> ProjectConfig:
    """The ProjectConfig sink resolution reads; defaults on any load failure.

    A broken or missing project config must not crash sink resolution — the
    reconcile call that follows will surface the failure through its own
    (default-sink) error path.
    """
    if project_config is not None:
        return project_config
    try:
        root = project_root if project_root is not None else find_project_root()
        return load_project_config(root)
    except Exception:
        logger.warning(
            "could not load project config for alert-sink resolution — using the default sinks (%s)",
            ", ".join(DEFAULT_ALERT_SINKS),
        )
        return ProjectConfig()


def _construct_sink(name: str, options: dict[str, Any]) -> "AlertSink":
    """Load plugin ``alert_sink/<name>`` and construct it with its options.

    Entry points may register a class (constructed with the sink's
    ``[dlt_ops.alert_sink.<name>]`` options as keyword arguments) or a
    ready instance (options then have nowhere to go and are ignored with a
    warning).
    """
    plugin = _plugins.get(ALERT_SINK_AXIS, name)
    if isinstance(plugin, type):
        return plugin(**options)
    if options:
        logger.warning(
            "alert sink %r registered an instance, not a class — ignoring its [dlt_ops.alert_sink.%s] options",
            name,
            name,
        )
    return plugin


def resolve_sink(
    sink: "AlertSink | None",
    *,
    dry_run: bool,
    project_config: ProjectConfig | None = None,
    project_root: "Path | None" = None,
) -> "AlertSink":
    """The sink a public entry point will use for this invocation.

    Precedence:

    1. ``--dry-run`` outranks everything — the dry-run contract is "no
       emission anywhere", not "emission to a quieter sink".
    2. An injected ``sink`` (tests) is used as-is.
    3. Otherwise resolution is config-driven, once per invocation: every name
       in ``[dlt_ops] alert_sinks`` (default: ``["logging"]``) resolves
       via the plugin registry and the resulting sinks are wrapped in a
       fan-out :class:`MultiAlertSink`.

    Best-effort at runtime: a sink that fails to load or construct is logged
    and dropped for this invocation (Tier-1 ``validate`` and the Tier-2
    preflight are the enforcement points for unregistered/broken sinks). If
    every configured sink drops, emission falls back to the core logging
    sink so findings are never silently lost; an explicitly empty
    ``alert_sinks = []`` disables emission on purpose.
    """
    if dry_run:
        return NullAlertSink()
    if sink is not None:
        return sink

    config = _config_for_sinks(project_config, project_root)
    names = config.alert_sinks if config.alert_sinks is not None else DEFAULT_ALERT_SINKS
    resolved: list[tuple[str, AlertSink]] = []
    for name in names:
        try:
            resolved.append((name, _construct_sink(name, config.alert_sink_options.get(name, {}))))
        except Exception:
            logger.exception("alert sink %r could not be resolved — dropping it for this invocation", name)
    if not resolved and names:
        logger.error(
            "no configured alert sink could be resolved (%s) — falling back to the core logging sink",
            ", ".join(names),
        )
        resolved = [("logging", LoggingAlertSink())]
    return MultiAlertSink(resolved)


def emit_findings(sink: "AlertSink", findings: list) -> None:
    """Publish each finding; a single emission failure never crashes the caller.

    A failing ``emit_drift`` is reported through the same sink's error path
    under a detector-specific context, then swallowed — the reconciler's
    best-effort observer contract.
    """
    from dlt_ops.reconciler.models import DriftKind

    for finding in findings:
        try:
            sink.emit_drift(finding)
        except Exception as exc:
            context = _EMIT_DRIFT_CONTEXT_REMOVAL if finding.kind is DriftKind.REMOVAL else _EMIT_DRIFT_CONTEXT_ADDITIVE
            try:
                sink.emit_error(
                    exc,
                    source_name=finding.source_name,
                    resource_name=finding.resource_name,
                    context=context,
                )
            except Exception:
                logger.exception("alert sink emit_error itself failed")
            logger.exception(
                "emit_drift (%s) failed for %s.%s",
                finding.kind.value,
                finding.source_name,
                finding.resource_name,
            )


__all__ = [
    "ALERT_SINK_AXIS",
    "FLUSH_TIMEOUT_SECONDS",
    "LoggingAlertSink",
    "MultiAlertSink",
    "NullAlertSink",
    "emit_findings",
    "resolve_sink",
]
