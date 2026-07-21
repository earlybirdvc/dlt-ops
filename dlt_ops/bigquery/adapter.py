"""Opt-in BigQuery resource-optimization helper (partition + cluster)."""

from typing import Any


def adapter(resource: Any, partition: Any = None, cluster: Any = None, **kwargs: Any) -> Any:
    """Apply BigQuery partition/cluster settings to a dlt resource.

    Thin wrapper over dlt's ``bigquery_adapter`` — the BigQuery-user opt-in
    surface for per-destination table optimizations (kept out of core rules;
    users on other destinations use their own destination's helper). Extra
    keyword arguments pass through unchanged (``table_description``,
    ``insert_api``, ...).

    Imported lazily so ``dlt_ops.bigquery`` loads without dlt's
    BigQuery destination machinery.
    """
    from dlt.destinations.adapters import bigquery_adapter

    return bigquery_adapter(resource, partition=partition, cluster=cluster, **kwargs)
