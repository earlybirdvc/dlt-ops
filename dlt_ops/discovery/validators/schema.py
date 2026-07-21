import ast
import importlib
import logging
from pathlib import Path
from typing import Any, get_args, get_origin

import pydantic

from dlt_ops.discovery.models import ValidationContext, ValidationError

logger = logging.getLogger(__name__)


def _get_pydantic_dict_fields(model: type[pydantic.BaseModel]) -> list[str]:
    """Extract field names that are dict or list[dict] types."""
    dict_fields = []
    for name, field_info in model.model_fields.items():
        annotation = field_info.annotation
        if annotation is None:
            continue

        # Unwrap Optional/Union types
        origin = get_origin(annotation)
        if origin is type(None):
            continue

        # Handle Union types (including Optional)
        args = get_args(annotation)
        if args:
            # Check each union member
            for arg in args:
                if arg is type(None):
                    continue
                if _is_dict_type(arg):
                    dict_fields.append(name)
                    break
        elif _is_dict_type(annotation):
            dict_fields.append(name)

    return dict_fields


def _is_dict_type(annotation: Any) -> bool:
    """Check if annotation is dict or list[dict]."""
    # Direct dict
    if annotation is dict:
        return True

    origin = get_origin(annotation)

    # Generic dict (e.g., dict[str, Any])
    if origin is dict:
        return True

    # list[dict] or list[dict[...]]
    if origin is list:
        args = get_args(annotation)
        if args:
            inner = args[0]
            if inner is dict or get_origin(inner) is dict:
                return True

    return False


def _parse_resource_pydantic_model(
    resource_file: Path, function_name: str, module_name: str
) -> type[pydantic.BaseModel] | None:
    """Parse @dlt.resource(columns=PydanticModel) from resource file."""
    try:
        with open(resource_file, encoding="utf-8") as f:
            tree = ast.parse(f.read())
    except (OSError, SyntaxError) as e:
        logger.debug(f"Failed to parse {resource_file.name}: {e}")
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    # Check if it's dlt.resource
                    func = decorator.func
                    is_dlt_resource = (
                        isinstance(func, ast.Attribute)
                        and func.attr == "resource"
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "dlt"
                    )
                    if not is_dlt_resource:
                        continue

                    # Find columns= parameter
                    for keyword in decorator.keywords:
                        if keyword.arg == "columns" and isinstance(keyword.value, ast.Name):
                            class_name = keyword.value.id
                            # Import the Pydantic model from the same module
                            try:
                                module = importlib.import_module(module_name)
                                pydantic_class = getattr(module, class_name, None)
                                if (
                                    pydantic_class
                                    and isinstance(pydantic_class, type)
                                    and issubclass(pydantic_class, pydantic.BaseModel)
                                ):
                                    return pydantic_class
                            except Exception as e:
                                logger.debug(f"Failed to import {class_name} from {module_name}: {e}")
                                return None
    return None


def _get_column_hints(resource: Any) -> dict[str, dict[str, Any]]:
    """Get column hints dict from resource.

    Returns empty dict if columns is a Pydantic model type (apply_hints not yet called).
    """
    hints = getattr(resource, "_hints", {})
    columns = hints.get("columns", {})

    # Pydantic model type means apply_hints() hasn't merged additional hints yet
    if isinstance(columns, type):
        return {}

    return columns if isinstance(columns, dict) else {}


def _get_resource_module_file(resource: Any) -> Path | None:
    """Get source file path for a resource function."""
    try:
        # Get the wrapped function (before dlt decoration)
        func = getattr(resource, "__wrapped__", resource)
        if not hasattr(func, "__module__"):
            return None

        module = importlib.import_module(func.__module__)
        if not hasattr(module, "__file__") or module.__file__ is None:
            return None

        return Path(module.__file__)
    except Exception as e:
        logger.debug(f"Could not get module file for resource: {e}")
        return None


def validate_json_column_hints(ctx: ValidationContext) -> list[ValidationError]:
    """Ensure Pydantic dict/list[dict] fields have data_type=json in column hints.

    Without an explicit data_type='json' hint, dlt normalizes dict/list[dict]
    fields into nested child tables instead of a single JSON column — on any
    destination. This validator catches the missing hints.
    """
    errors: list[ValidationError] = []

    for source in ctx.sources.values():
        try:
            source_instance = source.source_fn()
        except Exception as e:
            logger.debug(f"Could not instantiate source {source.name}: {e}")
            continue

        errors.extend(_validate_source_resources(source.name, source_instance))

    return errors


def _validate_source_resources(source_name: str, source_instance: Any) -> list[ValidationError]:
    """Validate JSON column hints for all resources in a source."""
    errors: list[ValidationError] = []

    for resource_name, resource in source_instance.resources.items():
        resource_file = _get_resource_module_file(resource)
        if not resource_file:
            continue

        func = getattr(resource, "__wrapped__", resource)
        function_name = getattr(func, "__name__", None)
        if not function_name:
            continue

        pydantic_model = _parse_resource_pydantic_model(resource_file, function_name, func.__module__)
        if pydantic_model is None:
            continue

        dict_fields = _get_pydantic_dict_fields(pydantic_model)
        if not dict_fields:
            continue

        column_hints = _get_column_hints(resource)
        errors.extend(_check_dict_fields(source_name, resource_name, dict_fields, column_hints))

    return errors


def _check_dict_fields(
    source_name: str,
    resource_name: str,
    dict_fields: list[str],
    column_hints: dict[str, dict[str, Any]],
) -> list[ValidationError]:
    """Check that dict fields have data_type=json hint."""
    errors: list[ValidationError] = []

    for field in dict_fields:
        field_hint = column_hints.get(field, {})
        if isinstance(field_hint, dict) and field_hint.get("data_type") == "json":
            continue

        errors.append(
            ValidationError(
                source_name=source_name,
                field=f"resource.{resource_name}.{field}",
                message=f"Pydantic field '{field}' is dict/list[dict] but missing "
                f"data_type='json' hint — dlt will create a nested child table "
                f"instead of a JSON column. Add: "
                f"res.apply_hints(columns={{'{field}': {{'data_type': 'json'}}}})",
            )
        )

    return errors


def _get_columns_kwarg(resource_file: Path, function_name: str) -> ast.expr | None:
    """Return the columns= keyword value node from the @dlt.resource decorator."""
    try:
        with open(resource_file, encoding="utf-8") as f:
            tree = ast.parse(f.read())
    except (OSError, SyntaxError) as e:
        logger.debug(f"Failed to parse {resource_file.name}: {e}")
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    func = decorator.func
                    is_dlt_resource = (
                        isinstance(func, ast.Attribute)
                        and func.attr == "resource"
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "dlt"
                    )
                    if not is_dlt_resource:
                        continue

                    for keyword in decorator.keywords:
                        if keyword.arg == "columns":
                            return keyword.value
    return None


def _has_columns_kwarg(resource_file: Path, function_name: str) -> bool:
    """Check if @dlt.resource(...) decorator has a columns= keyword argument."""
    return _get_columns_kwarg(resource_file, function_name) is not None


def _check_columns_is_pydantic(
    source_name: str,
    resource_name: str,
    resource_file: Path,
    function_name: str,
    module_name: str,
    columns_node: ast.expr,
) -> ValidationError | None:
    """Verify the columns= value resolves to a Pydantic model.

    ast.Name → import and check issubclass(BaseModel). ast.Attribute (factory
    config pattern, e.g. columns=cfg.model) is accepted — the config class's
    type annotation enforces it. Anything else (inline dict, call) is an error.
    """
    if isinstance(columns_node, ast.Attribute):
        return None

    if isinstance(columns_node, ast.Name):
        model = _parse_resource_pydantic_model(resource_file, function_name, module_name)
        if model is not None:
            return None
        return ValidationError(
            source_name=source_name,
            field=f"resource.{resource_name}",
            message=f"columns={columns_node.id} on '{function_name}' does not resolve to a "
            f"Pydantic model in {resource_file.name}. The model class must be importable "
            f"from the resource module and subclass pydantic.BaseModel.",
        )

    return ValidationError(
        source_name=source_name,
        field=f"resource.{resource_name}",
        message=f"columns= on '{function_name}' is not a Pydantic model reference. "
        f"Define the schema as a Pydantic model (single source of truth): "
        f"@dlt.resource(columns=MyModel, ...)",
    )


def validate_resource_columns_hint(ctx: ValidationContext) -> list[ValidationError]:
    """Ensure every @dlt.resource declares columns= as a Pydantic model.

    Without columns=, dlt infers types at load time. If all values are NULL,
    the column is silently dropped — and with schema_contract columns=freeze,
    it can never be added later.

    Per-source opt-out: [sources.<X>.dlt_ops.rule_exemptions]
    pydantic_columns_required = "<reason>" (the framework filters findings
    for exempted sources).
    """
    errors: list[ValidationError] = []

    for source in ctx.sources.values():
        try:
            source_instance = source.source_fn()
        except Exception as e:
            logger.debug(f"Could not instantiate source {source.name}: {e}")
            continue

        for resource_name, resource in source_instance.resources.items():
            resource_file = _get_resource_module_file(resource)
            if not resource_file:
                continue

            func = getattr(resource, "__wrapped__", resource)
            function_name = getattr(func, "__name__", None)
            if not function_name:
                continue

            columns_node = _get_columns_kwarg(resource_file, function_name)
            if columns_node is None:
                errors.append(
                    ValidationError(
                        source_name=source.name,
                        field=f"resource.{resource_name}",
                        message=f"@dlt.resource for '{function_name}' is missing columns= hint. "
                        f"Add a Pydantic model: @dlt.resource(columns=MyModel, ...)",
                    )
                )
                continue

            error = _check_columns_is_pydantic(
                source.name, resource_name, resource_file, function_name, func.__module__, columns_node
            )
            if error is not None:
                errors.append(error)

    return errors
