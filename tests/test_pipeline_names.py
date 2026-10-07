"""Tests for pipeline-name derivation — ``dlt_ops._pipeline_names``.

Each rejection case is built so that exactly one condition fails: a template
rejected for the wrong reason would pass the test while leaving the intended
check unproven.
"""

import pytest
from dlt.common.storages import FileStorage

from dlt_ops._pipeline_names import (
    DEFAULT_PIPELINE_NAME_TEMPLATE,
    pipeline_name_for_source,
    validate_pipeline_name_template,
)


class TestDefaultTemplate:
    def test_default_reproduces_the_historical_name(self):
        assert pipeline_name_for_source("web_events", DEFAULT_PIPELINE_NAME_TEMPLATE) == "web_events_pipeline"

    def test_default_is_valid(self):
        validate_pipeline_name_template(DEFAULT_PIPELINE_NAME_TEMPLATE)

    def test_bare_source_template_keeps_the_source_name(self):
        """The adoption case: existing dlt state already lives under the bare source name."""
        assert pipeline_name_for_source("web_events", "{source}") == "web_events"

    @pytest.mark.parametrize("template", ["{source}", "{source}_pipeline", "dlt_{source}", "{source}-ingest"])
    def test_accepted_templates_render_names_dlt_accepts(self, template):
        validate_pipeline_name_template(template)
        FileStorage.validate_file_name_component(pipeline_name_for_source("web_events", template))


class TestRejectedTemplates:
    """One test per condition; every other condition holds in each case."""

    def test_non_string_value(self):
        with pytest.raises(ValueError, match="must be a string, got int"):
            validate_pipeline_name_template(7)

    def test_missing_source_placeholder(self):
        """A fixed name is valid as a file name — it is rejected only for sharing one state."""
        FileStorage.validate_file_name_component("one_pipeline")
        with pytest.raises(ValueError, match="no {source} placeholder"):
            validate_pipeline_name_template("one_pipeline")

    def test_unknown_placeholder_alongside_a_valid_source(self):
        with pytest.raises(ValueError, match=r"placeholder \{dataset\}"):
            validate_pipeline_name_template("{source}_{dataset}")

    def test_positional_placeholder_alongside_a_valid_source(self):
        with pytest.raises(ValueError, match=r"placeholder \{\}"):
            validate_pipeline_name_template("{source}_{}")

    def test_attribute_access_on_source_is_not_the_source_placeholder(self):
        with pytest.raises(ValueError, match=r"placeholder \{source\.upper\}"):
            validate_pipeline_name_template("{source.upper}")

    def test_conversion_on_the_source_placeholder(self):
        with pytest.raises(ValueError, match="conversion or format spec"):
            validate_pipeline_name_template("{source!r}")

    def test_format_spec_on_the_source_placeholder(self):
        with pytest.raises(ValueError, match="conversion or format spec"):
            validate_pipeline_name_template("{source:>20}")

    def test_malformed_braces(self):
        with pytest.raises(ValueError, match="not a valid format string"):
            validate_pipeline_name_template("{source")

    def test_rendered_name_dlt_rejects(self):
        """A path separator in the literal part: valid format string, unusable name."""
        validate_pipeline_name_template("{source}_pipeline")  # the same shape without the separator
        with pytest.raises(ValueError, match="which dlt rejects as a pipeline name"):
            validate_pipeline_name_template("pipelines/{source}")


class TestSourceNames:
    """Source names pass through untouched, apart from the blank name this module rejects."""

    def test_hostile_source_name_is_not_rejected_here(self):
        hostile = 'x"; DROP TABLE users;--'
        assert pipeline_name_for_source(hostile, DEFAULT_PIPELINE_NAME_TEMPLATE) == f"{hostile}_pipeline"

    def test_an_unusable_template_still_raises_at_render_time(self):
        with pytest.raises(ValueError, match="no {source} placeholder"):
            pipeline_name_for_source("web_events", "fixed_name")

    @pytest.mark.parametrize("source_name", ["", "   "])
    def test_blank_source_name_is_rejected(self, source_name):
        """dlt answers the empty name with its own default name, so only dlt-ops can catch that one."""
        with pytest.raises(ValueError, match="source name is empty"):
            pipeline_name_for_source(source_name, "{source}")
