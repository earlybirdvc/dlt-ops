"""Default ``secrets_toml`` backend — a no-op passthrough over dlt's native secrets.

dlt already reads ``.dlt/secrets.toml`` (and its other config providers)
natively, so this backend fetches nothing at ``setup_secrets`` time: it
implements no ``secret_requests`` hook and therefore never claims a source.
It exists so resolution is uniform — "which backend serves this source?"
always has an answer — and so ``validate`` / preflight can check the chain
(the resolved backend, default included, must be registered and healthy).
``get`` delegates to ``dlt.secrets`` so the Protocol surface stays honest.
"""

from __future__ import annotations

import dlt

from dlt_ops.secrets.protocol import SecretNotFoundError

__all__ = ["DEFAULT_BACKEND_NAME", "SecretsTomlBackend"]

DEFAULT_BACKEND_NAME = "secrets_toml"


class SecretsTomlBackend:
    """Passthrough to dlt's native secret resolution (``.dlt/secrets.toml``, env, ...)."""

    name = DEFAULT_BACKEND_NAME

    def get(self, key: str) -> str:
        """Read ``key`` through ``dlt.secrets``.

        Raises:
            SecretNotFoundError: no dlt config provider resolves the key.
        """
        try:
            return dlt.secrets[key]
        except KeyError as exc:
            raise SecretNotFoundError(
                f"secret {key!r} not found by any dlt config provider; "
                f"add it to .dlt/secrets.toml (or another provider dlt reads natively)"
            ) from exc
