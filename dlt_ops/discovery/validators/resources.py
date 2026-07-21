from collections import defaultdict

from dlt_ops.discovery.models import ValidationContext, ValidationError


def validate_no_resource_overlap(ctx: ValidationContext) -> list[ValidationError]:
    """Ensure no two sources share resources within the same pipeline.

    Resources must be unique within a pipeline (directory) to prevent conflicts
    when multiple sources are defined in the same directory.
    """
    errors: list[ValidationError] = []
    # Track resources by pipeline: {pipeline_name: {resource: source_name}}
    pipeline_resources: dict[str, dict[str, str]] = defaultdict(dict)

    # Sort for deterministic error messages
    for source in sorted(ctx.sources.values(), key=lambda s: s.name):
        pipeline = source.pipeline_name
        for resource in source.resources:
            if resource in pipeline_resources[pipeline]:
                errors.append(
                    ValidationError(
                        source_name=source.name,
                        field="resources",
                        message=f"Resource '{resource}' already defined in source "
                        f"'{pipeline_resources[pipeline][resource]}' (same pipeline '{pipeline}')",
                    )
                )
            else:
                pipeline_resources[pipeline][resource] = source.name

    return errors
