---
description: The public Python API of dlt-ops — every name exported from the top-level dlt_ops package, generated from source docstrings, covering plugin registration, discovery and validation, checkpoints, destination adapters, assertions, secret backends, and schema-drift reconciliation.
---

# API reference

The public Python API of `dlt-ops` — the names exported from the top-level `dlt_ops` package (its `__all__`). Everything else is importable but internal, with no stability promise; the public-vs-internal convention and the stability contract live in [versioning](versioning.md). Import any name directly, for example `from dlt_ops import with_checkpoints, DestinationAdapter, register`.

The schema-drift names (`reconcile_all`, `reconcile_source`, `detect_removal`, `ReconcileResult`, `DriftFinding`, `AlertSink`) are re-exported lazily so that `import dlt_ops` never pulls the reconciler's backend dependencies at import time; they are documented below under their `dlt_ops.reconciler` module but resolve equally as `from dlt_ops import reconcile_all`.

## Plugin registration

Register a plugin against one of the entry-point axes; see [plugins](../concepts/plugins.md).

::: dlt_ops.register
    options:
      show_root_heading: true
      heading_level: 3

## Discovery and validation

Scan a project for sources and run the rule framework; see [discovery](../concepts/discovery.md) and [validation](../concepts/validation.md).

::: dlt_ops.discover_sources
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.validate_sources
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.SourceInfo
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.SourceConfig
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.Schedule
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.RuleSpec
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.Validator
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.ValidationContext
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.ValidationError
    options:
      show_root_heading: true
      heading_level: 3

## Checkpoints

Persist pagination progress to the destination and resume mid-run; see [checkpoints](../concepts/checkpoints.md).

::: dlt_ops.with_checkpoints
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.list_checkpoints
    options:
      show_root_heading: true
      heading_level: 3
      docstring_options:
        warn_missing_types: false

::: dlt_ops.cleanup_checkpoints
    options:
      show_root_heading: true
      heading_level: 3
      docstring_options:
        warn_missing_types: false

## Destination adapters

The contract a destination implements to reach full tier; see [destinations and capability tiers](../concepts/destinations-and-tiers.md) and the [adapter guide](../guides/write-a-destination-adapter.md).

::: dlt_ops.DestinationAdapter
    options:
      show_root_heading: true
      heading_level: 3

## Assertions

Pre-load data-quality gates and the error they raise; see [assertions](../concepts/assertions.md).

::: dlt_ops.AssertionType
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.AssertionContext
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.AssertionFailedError
    options:
      show_root_heading: true
      heading_level: 3

## Secret backends

The contract a secret backend implements; see [plugins](../concepts/plugins.md).

::: dlt_ops.SecretBackend
    options:
      show_root_heading: true
      heading_level: 3

## Alert sinks

The contract a drift-alert sink implements; see [reconciler](../concepts/reconciler.md).

::: dlt_ops.reconciler.AlertSink
    options:
      show_root_heading: true
      heading_level: 3

## Schema-drift reconciliation

Diff declared models against the live destination schema; see [reconciler](../concepts/reconciler.md).

::: dlt_ops.reconciler.reconcile_all
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.reconciler.reconcile_source
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.reconciler.detect_removal
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.reconciler.ReconcileResult
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.reconciler.DriftFinding
    options:
      show_root_heading: true
      heading_level: 3

## Pydantic model helpers

Derive column facts from your declared Pydantic models; see [add a source](../guides/add-a-source.md).

::: dlt_ops.drop_unknown_nulls
    options:
      show_root_heading: true
      heading_level: 3

::: dlt_ops.extract_model_column_names
    options:
      show_root_heading: true
      heading_level: 3
