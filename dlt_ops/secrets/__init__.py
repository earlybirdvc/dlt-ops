"""Secret-backend axis: Protocol, default ``secrets_toml`` backend, resolution + setup.

Public subpackage — the names in ``__all__`` are the stable surface for
secret-backend plugin authors (``SecretBackend`` is also re-exported from the
package top level).
"""

from dlt_ops.secrets.default import SecretsTomlBackend
from dlt_ops.secrets.protocol import SecretBackend, SecretNotFoundError, SecretRequest
from dlt_ops.secrets.setup import setup_secrets

__all__ = [
    "SecretBackend",
    "SecretNotFoundError",
    "SecretRequest",
    "SecretsTomlBackend",
    "setup_secrets",
]
