"""Pipeline-name derivation — the single place a source name becomes a dlt pipeline name.

dlt keys pipeline state on the pipeline name together with the destination and
dataset, and the name also becomes the local working directory
(``~/.dlt/pipelines/<name>``) and, on file-based destinations such as DuckDB, the
database filename. A project therefore chooses the shape through
``[dlt_ops] pipeline_name_template``; ``docs/guides/adopt-existing-project.md``
explains when to set it.

This module imports nothing from ``dlt_ops``, so neither ``dlt_ops.config`` nor
``dlt_ops.runs`` gains an import cycle by depending on it.
"""

from __future__ import annotations

import string

from dlt.common.storages import FileStorage

PIPELINE_NAME_TEMPLATE_KEY = "pipeline_name_template"
"""The ``[dlt_ops]`` key holding the template — the single copy of this key name."""

DEFAULT_PIPELINE_NAME_TEMPLATE = "{source}_pipeline"
"""The default template. A project that does not set the key keeps this name shape."""

SOURCE_PLACEHOLDER = "source"
"""The only placeholder a template may use. Adding another later cannot break an existing template."""

_PROBE_SOURCE_NAME = "probe"
"""A source name dlt accepts verbatim, so rendering it only ever reports faults in the template."""


def validate_pipeline_name_template(template: object) -> None:
    """Raise ``ValueError`` when a template cannot produce usable pipeline names.

    The final check calls :meth:`FileStorage.validate_file_name_component`, the
    same function ``dlt.pipeline()`` applies to a pipeline name, so this rule
    cannot drift from dlt's own. It runs on a name rendered from a probe source
    name, because that rule bans ``{`` and ``}`` and so no template can pass it
    directly.
    """
    if not isinstance(template, str):
        raise ValueError(f"must be a string, got {type(template).__name__}")
    _validate_placeholders(template)
    rendered = template.format(**{SOURCE_PLACEHOLDER: _PROBE_SOURCE_NAME})
    _validate_rendered_name(rendered, template)


def pipeline_name_for_source(source_name: str, template: str) -> str:
    """The dlt pipeline name a source runs under.

    A writer and a reader that derive this name differently address different
    data without any error, so every caller derives it here.

    The source name is passed through verbatim apart from the empty case: dlt
    refuses a name it cannot use when the pipeline is constructed, but answers
    an empty one with its own default name, which would put every source on one
    pipeline state.

    Raises:
        ValueError: the template is unusable, or the source name is blank.
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
