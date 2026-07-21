"""Pre-load assertions — data-quality gates between extract and destination write.

Public surface for plugin authors: :class:`AssertionType` (the accumulator
Protocol), :class:`AssertionContext` (static facts for ``check_config``), and
the typed errors. Assertion types register under the
``dlt_ops.assertion`` entry-point group — the four built-ins in
``builtin.py`` ship through that same path.

Everything else in this package (config parser, engine, quarantine writer)
is internal runtime machinery consumed by ``discovery/runner.py``,
``preflight.py``, and the ``validate`` rules.
"""

from dlt_ops.assertions.models import (
    ASSERTION_AXIS,
    DEFAULT_ON_FAILURE,
    ON_FAILURE_VALUES,
    AssertionConfigurationError,
    AssertionContext,
    AssertionFailedError,
    AssertionType,
    OnFailure,
)

__all__ = [
    "ASSERTION_AXIS",
    "DEFAULT_ON_FAILURE",
    "ON_FAILURE_VALUES",
    "AssertionConfigurationError",
    "AssertionContext",
    "AssertionFailedError",
    "AssertionType",
    "OnFailure",
]
