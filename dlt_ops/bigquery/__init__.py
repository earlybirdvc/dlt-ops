"""BigQuery plugin: opt-in adapter helper + BigQuery-specific validation rules.

``adapter`` is the public opt-in surface for partition/cluster optimization;
the rule group in ``validators`` registers through the
``dlt_ops.validators`` entry point (provider ``bigquery``).
"""

from dlt_ops.bigquery.adapter import adapter

__all__ = ["adapter"]
