from __future__ import annotations

import dataclasses
import json
import types
from datetime import date, datetime, time
from enum import Enum
from typing import Any, Mapping, Union, get_args, get_origin, get_type_hints
from uuid import UUID

from .errors import ValidationError
from .models import JsonValue
from .secrets import SecretRef, SecretValue

_NONE_TYPE = type(None)
_SECRET_MARKER = "__pac_secret_ref__"


def to_json_value(value: Any, *, path: str = "value") -> JsonValue:
    if isinstance(value, SecretValue):
        raise ValidationError(f"Resolved secret cannot be persisted at {path}")
    if isinstance(value, SecretRef):
        return {_SECRET_MARKER: value.name}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: to_json_value(getattr(value, field.name), path=f"{path}.{field.name}")
            for field in dataclasses.fields(value)
        }
    if _is_pydantic_model(value):
        return to_json_value(value.model_dump(mode="json"), path=path)
    if isinstance(value, Enum):
        return to_json_value(value.value, path=path)
    if isinstance(value, (datetime, date, time, UUID)):
        return value.isoformat() if not isinstance(value, UUID) else str(value)
    if value is None or isinstance(value, (str, bool, int, float)):
        try:
            return json.loads(json.dumps(value, allow_nan=False))
        except ValueError as exc:
            raise ValidationError(f"Non-finite number at {path}") from exc
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValidationError(f"Expected string key at {path}; got {key!r}")
            result[key] = to_json_value(child, path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [to_json_value(child, path=f"{path}[{index}]") for index, child in enumerate(value)]
    raise ValidationError(f"Unsupported persisted value {type(value).__name__} at {path}")


def canonical_json_value(value: Any, *, path: str = "value") -> JsonValue:
    converted = to_json_value(value, path=path)
    return json.loads(json.dumps(converted, sort_keys=True, separators=(",", ":"), allow_nan=False))


def from_json_value(value: JsonValue, annotation: Any, *, path: str = "value") -> Any:
    if annotation in (Any, object, None):
        return _restore_secret_refs(value)
    if annotation is _NONE_TYPE:
        if value is not None:
            raise ValidationError(f"Expected null at {path}")
        return None
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (Union, types.UnionType):
        errors = []
        for option in args:
            try:
                return from_json_value(value, option, path=path)
            except ValidationError as exc:
                errors.append(str(exc))
        raise ValidationError(f"Value at {path} does not match any union option: {'; '.join(errors)}")
    if origin is list:
        if not isinstance(value, list):
            raise ValidationError(f"Expected list at {path}")
        item_type = args[0] if args else Any
        return [from_json_value(item, item_type, path=f"{path}[{i}]") for i, item in enumerate(value)]
    if origin is tuple:
        if not isinstance(value, list):
            raise ValidationError(f"Expected array at {path}")
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(from_json_value(item, args[0], path=f"{path}[{i}]") for i, item in enumerate(value))
        if args and len(value) != len(args):
            raise ValidationError(f"Expected {len(args)} items at {path}")
        return tuple(from_json_value(item, args[i], path=f"{path}[{i}]") for i, item in enumerate(value))
    if origin in (dict, Mapping):
        if not isinstance(value, dict):
            raise ValidationError(f"Expected object at {path}")
        key_type, value_type = args or (str, Any)
        if key_type not in (str, Any):
            raise ValidationError(f"Only string-keyed mappings are supported at {path}")
        return {key: from_json_value(child, value_type, path=f"{path}.{key}") for key, child in value.items()}
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        if not isinstance(value, dict):
            raise ValidationError(f"Expected object for {annotation.__name__} at {path}")
        hints = get_type_hints(annotation)
        known = {field.name for field in dataclasses.fields(annotation)}
        extra = sorted(set(value) - known)
        if extra:
            raise ValidationError(f"Unexpected fields at {path}: {', '.join(extra)}")
        kwargs = {}
        for field in dataclasses.fields(annotation):
            if field.name in value:
                kwargs[field.name] = from_json_value(value[field.name], hints.get(field.name, Any), path=f"{path}.{field.name}")
            elif field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING:
                raise ValidationError(f"Missing required field {path}.{field.name}")
        try:
            return annotation(**kwargs)
        except Exception as exc:
            raise ValidationError(f"Invalid {annotation.__name__} at {path}: {exc}") from exc
    if _is_pydantic_class(annotation):
        try:
            return annotation.model_validate(value)
        except Exception as exc:
            raise ValidationError(f"Invalid {annotation.__name__} at {path}: {exc}") from exc
    if annotation is SecretRef:
        if not isinstance(value, dict) or set(value) != {_SECRET_MARKER} or not isinstance(value[_SECRET_MARKER], str):
            raise ValidationError(f"Expected secret reference at {path}")
        name = value[_SECRET_MARKER]
        assert isinstance(name, str)
        return SecretRef(name)
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        try:
            return annotation(value)
        except (ValueError, TypeError) as exc:
            raise ValidationError(f"Invalid {annotation.__name__} at {path}: {value!r}") from exc
    if annotation in (str, bool, int, float):
        if annotation is int and isinstance(value, bool):
            raise ValidationError(f"Expected int at {path}")
        if not isinstance(value, annotation):
            raise ValidationError(f"Expected {annotation.__name__} at {path}; got {type(value).__name__}")
        return value
    if annotation is UUID:
        try:
            return UUID(str(value))
        except ValueError as exc:
            raise ValidationError(f"Expected UUID at {path}") from exc
    if annotation in (date, datetime, time):
        try:
            return annotation.fromisoformat(str(value))
        except ValueError as exc:
            raise ValidationError(f"Expected {annotation.__name__} at {path}") from exc
    raise ValidationError(f"Unsupported type annotation {annotation!r} at {path}")


def validate_and_encode(value: Any, annotation: Any, *, path: str) -> tuple[Any, JsonValue]:
    encoded = canonical_json_value(value, path=path)
    decoded = from_json_value(encoded, annotation, path=path)
    return decoded, encoded


def schema_spec(annotation: Any) -> JsonValue:
    """Return a portable validation description for durable external payloads."""

    if annotation in (Any, object, None):
        return {"type": "any"}
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (Union, types.UnionType):
        return {"type": "union", "options": [schema_spec(item) for item in args]}
    if origin is list:
        return {"type": "list", "items": schema_spec(args[0] if args else Any)}
    if origin in (dict, Mapping):
        return {"type": "object", "values": schema_spec(args[1] if args else Any)}
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        hints = get_type_hints(annotation)
        return {
            "type": "record",
            "name": schema_identity(annotation),
            "fields": {
                field.name: {
                    "schema": schema_spec(hints.get(field.name, Any)),
                    "required": (
                        field.default is dataclasses.MISSING
                        and field.default_factory is dataclasses.MISSING
                    ),
                }
                for field in dataclasses.fields(annotation)
            },
        }
    names = {str: "string", bool: "boolean", int: "integer", float: "number", _NONE_TYPE: "null"}
    if annotation in names:
        return {"type": names[annotation]}
    return {"type": "any", "identity": schema_identity(annotation)}


def validate_schema_spec(value: JsonValue, spec: JsonValue, *, path: str = "payload") -> None:
    if not isinstance(spec, dict):
        return
    kind = spec.get("type")
    if kind == "any":
        return
    if kind == "string" and not isinstance(value, str):
        raise ValidationError(f"Expected string at {path}")
    if kind == "boolean" and not isinstance(value, bool):
        raise ValidationError(f"Expected boolean at {path}")
    if kind == "integer" and (not isinstance(value, int) or isinstance(value, bool)):
        raise ValidationError(f"Expected integer at {path}")
    if kind == "number" and (not isinstance(value, (int, float)) or isinstance(value, bool)):
        raise ValidationError(f"Expected number at {path}")
    if kind == "null" and value is not None:
        raise ValidationError(f"Expected null at {path}")
    if kind == "list":
        if not isinstance(value, list):
            raise ValidationError(f"Expected list at {path}")
        for index, item in enumerate(value):
            validate_schema_spec(item, spec.get("items"), path=f"{path}[{index}]")
    if kind == "object":
        if not isinstance(value, dict):
            raise ValidationError(f"Expected object at {path}")
        for key, item in value.items():
            validate_schema_spec(item, spec.get("values"), path=f"{path}.{key}")
    if kind == "record":
        if not isinstance(value, dict):
            raise ValidationError(f"Expected object at {path}")
        fields = spec.get("fields")
        if not isinstance(fields, dict):
            return
        extra = sorted(set(value) - set(fields))
        if extra:
            raise ValidationError(f"Unexpected fields at {path}: {', '.join(extra)}")
        for name, field in fields.items():
            if not isinstance(field, dict):
                continue
            if name not in value:
                if field.get("required"):
                    raise ValidationError(f"Missing required field {path}.{name}")
                continue
            validate_schema_spec(value[name], field.get("schema"), path=f"{path}.{name}")
    if kind == "union":
        options = spec.get("options")
        if not isinstance(options, list):
            return
        errors = []
        for option in options:
            try:
                validate_schema_spec(value, option, path=path)
                return
            except ValidationError as exc:
                errors.append(str(exc))
        raise ValidationError(f"Value at {path} does not match any option: {'; '.join(errors)}")


def schema_identity(annotation: Any) -> str:
    if annotation in (Any, object, None):
        return "json"
    if isinstance(annotation, type):
        return f"{annotation.__module__}.{annotation.__qualname__}"
    return repr(annotation)


def _restore_secret_refs(value: JsonValue) -> Any:
    if isinstance(value, dict):
        if set(value) == {_SECRET_MARKER}:
            name = value[_SECRET_MARKER]
            if isinstance(name, str):
                return SecretRef(name)
        return {key: _restore_secret_refs(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_restore_secret_refs(child) for child in value]
    return value


def _is_pydantic_class(annotation: Any) -> bool:
    return isinstance(annotation, type) and callable(getattr(annotation, "model_validate", None)) and callable(getattr(annotation, "model_dump", None))


def _is_pydantic_model(value: Any) -> bool:
    return _is_pydantic_class(type(value))
