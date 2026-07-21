"""Airflow-Variable secret backend (secret-backend axis plugin ``airflow``).

Claims a source when its ``[sources.<X>.dlt_ops]`` table carries
``airflow_var`` (trigger keys are plugin-owned — core never hardcodes them):
``setup_secrets`` then fetches ``Variable.get(airflow_var)`` and writes the
value to the ``airflow_var_key`` leaf (default ``api_secret_key``) under
``sources.<X>.``. The ``fail_on_missing`` contract is owned by
``setup_secrets``; this backend just raises :class:`SecretNotFoundError` for
a Variable that does not exist.

Deliberately importable without Airflow: backend resolution loads every
registered backend to ask for claims (``resolve_backend``), so a bare install
must be able to load this class. Airflow itself is imported lazily inside
``get`` — a claimed source on an Airflow-less install fails there with the
install hint.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dlt_ops.secrets.protocol import SecretNotFoundError, SecretRequest

__all__ = ["DEFAULT_SECRET_KEY", "AirflowVariableBackend"]

DEFAULT_SECRET_KEY = "api_secret_key"


class AirflowVariableBackend:
    """Secrets from Airflow Variables: ``Variable.get(ref)`` per claimed source."""

    name = "airflow"

    def get(self, key: str) -> str:
        """Fetch one Airflow Variable by name.

        Raises:
            ImportError: Airflow is not installed.
            SecretNotFoundError: the Variable does not exist.
        """
        try:
            from airflow.models import Variable
        except ModuleNotFoundError as exc:
            from dlt_ops.airflow import _INSTALL_HINT

            raise ImportError(_INSTALL_HINT) from exc
        try:
            return Variable.get(key)
        except KeyError as exc:
            raise SecretNotFoundError(
                f"Airflow Variable {key!r} does not exist; create it or fix the "
                f"'airflow_var' key in the source's [sources.<X>.dlt_ops] table"
            ) from exc

    def secret_requests(self, ext: Mapping[str, Any]) -> tuple[SecretRequest, ...]:
        """Claim sources whose ext table sets ``airflow_var`` (the selection hook)."""
        ref = ext.get("airflow_var")
        if not ref:
            return ()
        key = ext.get("airflow_var_key") or DEFAULT_SECRET_KEY
        return (SecretRequest(ref=str(ref), key=str(key)),)
