"""Orchestrator-neutral secrets setup: resolve each source's backend, fetch, write ``dlt.secrets``.

The generalization of the Airflow-era ``setup_secrets_from_config`` shape
(``airflow/secrets.py`` — ported onto this axis by the Airflow plugin): the
runtime iterates discovered sources, resolves the serving backend per source,
calls ``backend.get(request.ref)`` and writes
``dlt.secrets[f"sources.{section}.{request.key}"]``. Selection is implicit per
plugin (no ``secret_backend`` config key in v0.1): every registered backend is
asked to claim the source's raw ``[sources.<X>.dlt_ops]`` table via the
optional ``secret_requests`` hook; no claim = the ``secrets_toml`` default,
which serves without fetching (dlt reads the file natively).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import attrs
import dlt

from dlt_ops.plugins import registry as plugins
from dlt_ops.plugins.registry import PluginCollisionError
from dlt_ops.secrets.default import DEFAULT_BACKEND_NAME
from dlt_ops.secrets.protocol import SecretNotFoundError, SecretRequest

if TYPE_CHECKING:
    from dlt_ops.discovery.models import SourceInfo

__all__ = ["SECRET_BACKEND_AXIS", "BackendEngagement", "resolve_backend", "setup_secrets"]

logger = logging.getLogger(__name__)

SECRET_BACKEND_AXIS = "secret_backend"


@attrs.frozen
class BackendEngagement:
    """Resolution result for one source: the serving backend plus its fetch plan.

    ``requests`` is empty exactly when the default ``secrets_toml`` backend
    serves the source (nothing to fetch — dlt reads its providers natively).
    """

    name: str
    requests: tuple[SecretRequest, ...]


def _ext_table(config: Mapping[str, Any], section: str) -> dict[str, Any]:
    """The raw ``[sources.<section>.dlt_ops]`` table; ``{}`` when absent/malformed."""
    sources = config.get("sources")
    if not isinstance(sources, dict):
        return {}
    source_table = sources.get(section)
    if not isinstance(source_table, dict):
        return {}
    ext = source_table.get("dlt_ops")
    return ext if isinstance(ext, dict) else {}


def _backend_instance(obj: Any) -> Any:
    """Entry points may register a class (the adapter pattern) or an instance."""
    return obj() if isinstance(obj, type) else obj


def resolve_backend(section: str, config: Mapping[str, Any], *, registry: Any = plugins) -> BackendEngagement:
    """Which secret backend serves the source with config section ``section``.

    Loads every registered backend and asks it to claim the source's raw
    ``[sources.<section>.dlt_ops]`` table through the optional
    ``secret_requests`` hook. Exactly one backend may claim; no claim falls
    back to the ``secrets_toml`` default with an empty fetch plan.

    Raises:
        PluginCollisionError: more than one backend claims the source.
        Exception: whatever a registered backend's load or hook raises —
            callers surface it per their tier's soft/hard-fail policy.
    """
    ext = _ext_table(config, section)
    claims: list[BackendEngagement] = []
    for name in registry.names(SECRET_BACKEND_AXIS):
        backend = _backend_instance(registry.get(SECRET_BACKEND_AXIS, name))
        hook = getattr(backend, "secret_requests", None)
        if hook is None:
            continue
        requests = tuple(hook(ext))
        if requests:
            claims.append(BackendEngagement(name=name, requests=requests))
    if len(claims) > 1:
        claimants = ", ".join(repr(claim.name) for claim in claims)
        raise PluginCollisionError(
            f"multiple secret backends claim source {section!r}: {claimants}. "
            f"A source resolves to exactly one backend — remove the extra plugin or its "
            f"trigger keys from [sources.{section}.dlt_ops]."
        )
    if claims:
        return claims[0]
    return BackendEngagement(name=DEFAULT_BACKEND_NAME, requests=())


def setup_secrets(
    sources: Mapping[str, SourceInfo] | None = None,
    project_root: Path | None = None,
    *,
    config: Mapping[str, Any] | None = None,
    fail_on_missing: bool = True,
    registry: Any = plugins,
) -> None:
    """Resolve each source's secret backend, fetch its secrets, write ``dlt.secrets``.

    Sources served by the default ``secrets_toml`` backend need no work — dlt
    reads the file natively — so only claiming backends fetch.

    Args:
        sources: Pre-discovered sources, or None to discover from project_root.
        project_root: Project root; required when ``sources`` or ``config`` is None.
        config: Parsed raw ``.dlt/config.toml``; loaded from project_root when None.
        fail_on_missing: If True, a missing secret raises ``SecretNotFoundError``;
            if False it is logged and skipped.
        registry: Plugin registry (module-level default; tests inject stubs).
    """
    if sources is None:
        if project_root is None:
            raise ValueError("Either sources or project_root must be provided")
        from dlt_ops.discovery import discover_sources

        sources = discover_sources(project_root)
    if config is None:
        if project_root is None:
            raise ValueError("Either config or project_root must be provided")
        from dlt_ops.config import load_raw_config

        config = load_raw_config(project_root)

    for name, source in sources.items():
        engagement = resolve_backend(source.config_section, config, registry=registry)
        if not engagement.requests:
            logger.debug(f"Source {name}: no secret backend claim, {engagement.name} serves natively")
            continue
        backend = _backend_instance(registry.get(SECRET_BACKEND_AXIS, engagement.name))
        for request in engagement.requests:
            try:
                secret_value = backend.get(request.ref)
            except SecretNotFoundError as exc:
                if fail_on_missing:
                    raise
                logger.warning(f"Failed to get secret {request.ref!r} for {name} from {engagement.name!r}: {exc}")
                continue
            secret_key = f"sources.{source.config_section}.{request.key}"
            dlt.secrets[secret_key] = secret_value
            logger.info(f"Set secret for {name}: {secret_key} (backend {engagement.name!r})")
