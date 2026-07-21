"""SecretBackend Protocol — the secret-backend plugin axis contract.

The backend chooses WHERE a secret comes from (``.dlt/secrets.toml``, Airflow
Variables, a vault, ...); the runtime just calls ``get`` and writes
``dlt.secrets`` (see :func:`dlt_ops.secrets.setup.setup_secrets`).
Implementations register under the ``dlt_ops.secret_backend``
entry-point group (or via ``dlt_ops.register("secret_backend", name)``)
and are resolved by name through the plugin registry.

The required surface is deliberately minimal — ``name`` plus ``get`` — so the
axis doesn't encode any one implementation's config schema.

Backend selection (v0.1) is implicit per plugin: a backend claims a source by
implementing the OPTIONAL ``secret_requests`` hook::

    def secret_requests(self, ext: Mapping[str, Any]) -> Sequence[SecretRequest]: ...

``ext`` is the source's raw ``[sources.<X>.dlt_ops]`` table. A non-empty
result means "this backend serves the source" and doubles as the fetch plan:
each :class:`SecretRequest` names the backend-native reference to ``get`` and
the ``dlt.secrets`` leaf to write. Trigger keys stay plugin-owned — the
Airflow backend claims on its ``airflow_var`` / ``airflow_var_key`` keys; core
never hardcodes them. Backends without the hook (like the default
``secrets_toml`` passthrough) never claim a source; they can only serve as the
end of the resolution chain.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import attrs

__all__ = ["SecretBackend", "SecretNotFoundError", "SecretRequest"]


class SecretNotFoundError(LookupError):
    """A secret reference could not be resolved by its backend.

    The failure contract of :meth:`SecretBackend.get`: backends raise this
    (never return a placeholder) when the referenced secret is missing, so
    ``setup_secrets`` can honor its ``fail_on_missing`` knob uniformly.
    """


@attrs.frozen
class SecretRequest:
    """One secret a backend will serve for a source: fetch ``ref``, write ``key``.

    Produced by a backend's ``secret_requests`` hook; consumed by
    ``setup_secrets``, which calls ``backend.get(ref)`` and writes the value to
    ``dlt.secrets[f"sources.{section}.{key}"]``.
    """

    ref: str
    """Backend-native reference to fetch (e.g. an Airflow Variable name)."""

    key: str
    """``dlt.secrets`` leaf key under ``sources.<section>.``."""


@runtime_checkable
class SecretBackend(Protocol):
    """Everything the runtime may know about a secret backend.

    Raising :class:`SecretNotFoundError` from ``get`` on a missing secret is
    part of the contract. The optional ``secret_requests`` selection hook is
    documented in the module docstring — it is not a required member.
    """

    name: str
    """Registry name: ``"secrets_toml"``, ``"airflow"``, ..."""

    def get(self, key: str) -> str:
        """Fetch one secret by backend-native reference.

        Raises:
            SecretNotFoundError: the referenced secret does not exist.
        """
        ...
