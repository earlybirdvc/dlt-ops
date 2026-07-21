"""Sentry alert sink — the ``sentry`` plugin on the alert-sink axis.

Installed via the ``[sentry]`` extra (``pip install dlt-ops[sentry]``)
and enabled per project with ``[dlt_ops] alert_sinks = ["logging",
"sentry"]``. The module imports without ``sentry-sdk`` (so ``plugins
doctor`` stays healthy on a core install — the same pattern as the Airflow
secret backend); constructing the sink without the extra raises, which
``validate`` and the Tier-2 preflight surface for projects that actually
configured the sink.

The sink ships its own isolated ``sentry_sdk.Client`` instead of routing
through the process-global SDK, because a host process (an orchestrator
worker, a larger application) often configures its own Sentry and two global
behaviors would corrupt drift events:

1. A global ``before_send`` hook can rewrite fingerprints — all drift across
   all sources would then collapse into one Sentry Issue, useless for the
   "one alert per drifted resource" design.
2. A global ``LoggingIntegration`` scrapes warning-level log calls into
   Sentry events, double-firing everything this sink reports.

The isolated client bypasses both: it never runs the host's hooks and has no
integrations enabled. ``default_integrations=False`` also disables
``AtexitIntegration``, so the client's background transport queue is NOT
flushed automatically on process exit — a short-lived CLI or orchestrator
task could drop queued events. The reconciler's public entry points call
``flush()`` on the way out (see ``reconciler._emission``), closing that
window deterministically.

Configuration:

- DSN (secret): ``.dlt/secrets.toml`` →
  ``[alert_sinks.sentry] dsn = "https://...@....ingest.sentry.io/..."``,
  read through ``dlt.secrets`` so any dlt-native config provider can serve
  it. The bare ``SENTRY_DSN`` environment variable is deliberately NOT read
  — one source of truth; env-var-habituated users must move the value into
  the secrets file (or another dlt provider). DSN unset → the sink stays
  inert with a logged warning: findings are logged, not shipped.
- Options (non-secret): ``.dlt/config.toml`` →
  ``[dlt_ops.alert_sink.sentry]`` (``environment``, ``release``),
  passed as constructor keyword arguments by sink resolution.

sentry-sdk 2.x API pattern (``Hub`` and ``push_scope`` are deprecated): the
way to "use a specific client for this block" is to fork the current scope
via ``sentry_sdk.new_scope()``, call ``scope.set_client(client)``, then call
``sentry_sdk.capture_message(...)`` — ``Scope.get_client()`` walks current →
isolation → global, so the isolated client wins inside the ``with`` block.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

import dlt

from dlt_ops.reconciler._emission import FLUSH_TIMEOUT_SECONDS

if TYPE_CHECKING:
    import sentry_sdk

    from dlt_ops.reconciler.models import DriftFinding

__all__ = ["SentryAlertSink"]

logger = logging.getLogger(__name__)

_DSN_SECRET_KEY = "alert_sinks.sentry.dsn"

_DRIFT_FINGERPRINT_ROOT = "schema-drift"
_RECONCILER_ERROR_FINGERPRINT_ROOT = "schema-drift-reconciler-error"


def _require_sentry_sdk():
    """The ``sentry_sdk`` module, or an actionable error without the extra."""
    try:
        import sentry_sdk
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "the sentry alert sink requires the [sentry] extra: pip install 'dlt-ops[sentry]'"
        ) from exc
    return sentry_sdk


def _dsn_from_secrets() -> str | None:
    """The configured DSN, or None when no dlt provider resolves the key."""
    try:
        return dlt.secrets[_DSN_SECRET_KEY]
    except KeyError:
        return None


class SentryAlertSink:
    """AlertSink shipping drift findings + reconciler errors to Sentry.

    Fingerprints:

    - findings: ``["schema-drift", pipeline, source, resource]`` — additive
      and removal drift on the same resource collapse into one Sentry Issue
      by design (one PR closes both); the ``drift_type`` tag disambiguates
      in the UI.
    - reconciler-internal failures:
      ``["schema-drift-reconciler-error", source, resource|<source-level>,
      context]`` — kept out of the drift Issue so an operator triaging a
      real drift alert never has to filter out reconciler bugs.
    """

    def __init__(self, *, environment: str = "prod", release: str | None = None) -> None:
        # Verified at construction, not module import: `plugins doctor` can
        # load the entry point on a core install, while validate/preflight
        # (which construct configured sinks) surface the missing extra.
        self._sdk = _require_sentry_sdk()
        self._environment = environment
        self._release = release
        # One isolated Client per sink instance (sink resolution constructs
        # one instance per reconcile invocation). Lazy — constructing the
        # sink never touches the network or fails when the DSN is unset —
        # and guarded by a lock so concurrent emitters share one client.
        self._client: sentry_sdk.Client | None = None
        self._lock = threading.Lock()
        self._warned_inert = False

    def _get_client(self) -> sentry_sdk.Client | None:
        """The sink's isolated Sentry Client, or None when the DSN is unset.

        Returns None when ``[alert_sinks.sentry] dsn`` resolves through no
        dlt provider — the reconciler still runs (findings are collected,
        returned, and logged); the sink is inert.
        """
        if self._client is not None:
            return self._client

        dsn = _dsn_from_secrets()
        if not dsn:
            if not self._warned_inert:
                self._warned_inert = True
                logger.warning(
                    "no DSN under [alert_sinks.sentry] in .dlt/secrets.toml — Sentry sink inert; "
                    "findings are logged but not shipped (note: the SENTRY_DSN env var is not read)"
                )
            return None

        with self._lock:
            # Re-check under the lock — another thread may have raced past
            # the first `if self._client is not None` check.
            if self._client is not None:
                return self._client

            self._client = self._sdk.Client(
                dsn=dsn,
                environment=self._environment,
                release=self._release,
                # default_integrations=False disables LoggingIntegration +
                # the rest — critical: LoggingIntegration would scrape this
                # sink's own log lines into duplicate events. Also disables
                # AtexitIntegration — `flush` picks up its job at every
                # reconciler entry-point exit.
                default_integrations=False,
                traces_sample_rate=0.0,
            )
            return self._client

    def emit_drift(self, finding: "DriftFinding") -> None:
        """Emit one Sentry event per drifted resource.

        Inert (finding logged, nothing shipped) when the DSN is unset.
        """
        client = self._get_client()
        if client is None:
            logger.info(
                "drift finding (Sentry sink inert): %s.%s.%s [%s] %d columns",
                finding.pipeline_name,
                finding.source_name,
                finding.resource_name,
                finding.kind,
                len(finding.columns),
            )
            return

        # Fork the current scope so our tags/fingerprint don't bleed into the
        # process-global scope (a host process keeps its own tags on the
        # isolation scope — we must not touch those).
        with self._sdk.new_scope() as scope:
            # Bind our isolated client to this scope. `Scope.get_client()`
            # (called downstream by `capture_message` → `capture_event`)
            # walks current → isolation → global; the current scope's client
            # wins.
            scope.set_client(client)

            # `update_from_kwargs(fingerprint=...)` is the type-checker-
            # friendly equivalent of `scope.fingerprint = [...]`. The
            # property setter exists at runtime but its type stub is missing
            # on Scope; the kwargs helper writes to the same `_fingerprint`
            # slot with a typed signature.
            scope.update_from_kwargs(
                fingerprint=[
                    _DRIFT_FINGERPRINT_ROOT,
                    finding.pipeline_name,
                    finding.source_name,
                    finding.resource_name,
                ]
            )
            scope.set_tag("pipeline", finding.pipeline_name)
            scope.set_tag("source", finding.source_name)
            scope.set_tag("resource", finding.resource_name)
            # additive | removal — disambiguates in the Sentry UI.
            scope.set_tag("drift_type", str(finding.kind))

            preview_columns = list(finding.columns[:5])
            scope.set_context(
                "schema_drift",
                {
                    "columns": list(finding.columns),
                    "inferred_types": list(finding.inferred_types),
                    "first_seen_at": finding.first_seen_at.isoformat(),
                    "sample_values": dict(finding.sample_values),
                    "reproduce_sql": finding.reproduce_sql,
                },
            )

            preview = ", ".join(preview_columns)
            message = (
                f"Schema drift ({finding.kind}): "
                f"{finding.pipeline_name}.{finding.source_name}.{finding.resource_name} — "
                f"{len(finding.columns)} column(s): {preview}" + ("..." if len(finding.columns) > 5 else "")
            )
            self._sdk.capture_message(message, level="warning")

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None:
        """Emit a reconciler-internal failure under its own fingerprint.

        ``level="error"`` (capture_exception's default) because a reconciler
        failure means drift is no longer being observed on that resource —
        an infra problem, not a data problem. Inert when the DSN is unset.
        """
        client = self._get_client()
        if client is None:
            logger.exception(
                "reconciler error (Sentry sink inert): source=%s resource=%s context=%s",
                source_name,
                resource_name,
                context,
                exc_info=exc,
            )
            return

        with self._sdk.new_scope() as scope:
            scope.set_client(client)

            # See `emit_drift` for why `update_from_kwargs` vs the more
            # idiomatic `scope.fingerprint = [...]`.
            scope.update_from_kwargs(
                fingerprint=[
                    _RECONCILER_ERROR_FINGERPRINT_ROOT,
                    source_name,
                    resource_name or "<source-level>",
                    context,
                ]
            )
            scope.set_tag("source", source_name)
            if resource_name is not None:
                scope.set_tag("resource", resource_name)
            scope.set_tag("reconciler_context", context)
            scope.set_context(
                "reconciler_error",
                {
                    "context": context,
                    "resource": resource_name,
                    "exception_type": type(exc).__name__,
                    "exception_message": str(exc),
                },
            )
            self._sdk.capture_exception(exc)

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> None:
        """Drain the isolated client's transport queue before the process exits.

        Idempotent and a no-op when no client was ever constructed (DSN
        unset). ``timeout`` is capped low intentionally — a slow drain must
        not delay an orchestrated task's teardown; a stuck DSN loses at most
        ``timeout`` seconds' worth of queued events, matching the drift
        observer's best-effort contract.
        """
        client = self._client
        if client is None:
            return
        try:
            client.flush(timeout=timeout)
        except Exception:
            # A flush failure means events may have been dropped; log and
            # continue — raising here would defeat the best-effort design.
            logger.exception("Sentry alert sink flush raised — some drift events may not have shipped")
