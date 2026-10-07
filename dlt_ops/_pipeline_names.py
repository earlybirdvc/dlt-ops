"""Pipeline-name derivation — the single place a source name becomes a dlt pipeline name.

dlt keys pipeline state on the pipeline name together with the destination and
dataset; the name also becomes the local working directory
(``~/.dlt/pipelines/<name>``) and, on file-based destinations such as DuckDB, the
database filename. A project that adopts dlt-ops over an existing dlt deployment
must therefore be able to keep the names its state already lives under, which is
what the ``[dlt_ops] pipeline_name_template`` key configures.

This module imports nothing from ``dlt_ops``: both ``dlt_ops.config`` (which
parses the key) and ``dlt_ops.runs`` (which renders it) depend on it.
"""

from __future__ import annotations

import string

from dlt.common.storages import FileStorage

PIPELINE_NAME_TEMPLATE_KEY = "pipeline_name_template"
"""The ``[dlt_ops]`` key holding the template — one copy, imported by config and docs tooling."""

DEFAULT_PIPELINE_NAME_TEMPLATE = "{source}_pipeline"
"""The name shape dlt-ops has always used. Unset key = this value, so existing projects never move."""

SOURCE_PLACEHOLDER = "source"
"""The only placeholder a template may use. Adding another later cannot break an existing template."""

_PROBE_SOURCE_NAME = "probe"
"""A source name dlt accepts verbatim, so rendering it only ever reports faults in the template."""


def validate_pipeline_name_template(template: object) -> None:
    """Raise ``ValueError`` when a template cannot produce usable pipeline names.

    Each condition is checked on its own: the value is a string; its braces
    parse; every placeholder is ``{source}`` with no conversion or format spec;
    ``{source}`` appears at least once; and the rendered name is a component dlt
    accepts. The last check calls
    :meth:`FileStorage.validate_file_name_component`, the same function
    ``dlt.pipeline()`` applies to a pipeline name, so dlt-ops can never drift
    from dlt's own rule.

    Validating a template means validating a *rendered* name: the file-name rule
    bans ``{`` and ``}``, so the template itself never passes it.
    """
    if not isinstance(template, str):
        raise ValueError(f"must be a string, got {type(template).__name__}")
    _validate_placeholders(template)
    rendered = template.format(**{SOURCE_PLACEHOLDER: _PROBE_SOURCE_NAME})
    _validate_rendered_name(rendered, template)


def pipeline_name_for_source(source_name: str, template: str) -> str:
    """The dlt pipeline name a source runs under.

    Every caller derives the name here. The name is dlt's state key and, on
    file-based destinations, the database filename, so a writer and a reader
    that disagree on it address different data without any error.

    The source name is otherwise passed through verbatim: screening it here
    would add a rule on source names, and dlt already refuses a name it cannot
    use when the pipeline is constructed. The empty name is the exception,
    because dlt answers it with its own default name instead of an error.

    Raises:
        ValueError: the template is unusable, or the source name is empty.
            ``load_project_config`` rejects a bad template up front, so that
            half is the backstop for a template arriving from elsewhere.
    """
    validate_pipeline_name_template(template)
    if not source_name.strip():
        raise ValueError(
            "source name is empty, so dlt would substitute its own default pipeline name "
            "and put every source on one pipeline state"
        )
    return template.format(**{SOURCE_PLACEHOLDER: source_name})


def _validate_placeholders(template: str) -> None:
    """Reject anything but a bare ``{source}``; raises ``ValueError``."""
    try:
        fields = list(string.Formatter().parse(template))
    except ValueError as exc:
        raise ValueError(f"{template!r} is not a valid format string: {exc}") from exc

    found_source = False
    for _, field_name, format_spec, conversion in fields:
        if field_name is None:
            continue
        if field_name != SOURCE_PLACEHOLDER:
            shown = f"{{{field_name}}}" if field_name else "{}"
            raise ValueError(
                f"{template!r} uses the placeholder {shown}; {{{SOURCE_PLACEHOLDER}}} is the only placeholder supported"
            )
        if conversion or format_spec:
            raise ValueError(
                f"{template!r} applies a conversion or format spec to "
                f"{{{SOURCE_PLACEHOLDER}}}; write it as {{{SOURCE_PLACEHOLDER}}} with nothing else"
            )
        found_source = True

    if not found_source:
        raise ValueError(
            f"{template!r} contains no {{{SOURCE_PLACEHOLDER}}} placeholder, "
            "so every source would share one pipeline name and one dlt state"
        )


def _validate_rendered_name(rendered: str, template: str) -> None:
    """Reject a rendered name dlt cannot use as a pipeline name; raises ``ValueError``.

    dlt's own check is the whole rule here. It also rejects the empty name,
    which a probe render can never produce — that case is guarded where it can
    occur, on the source name in :func:`pipeline_name_for_source`.
    """
    try:
        FileStorage.validate_file_name_component(rendered)
    except ValueError as exc:
        raise ValueError(f"{template!r} renders to {rendered!r}, which dlt rejects as a pipeline name: {exc}") from exc
