from __future__ import annotations

import ast
import hashlib
import inspect
import json
import textwrap
import types
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from .codecs import canonical_json_value
from .errors import WorkflowDefinitionError

FINGERPRINT_FORMAT_VERSION = 2


def implementation_identity(value: Any, *, explicit_version: str | None = None) -> dict[str, Any]:
    """Return a stable, fail-closed identity for executable Python behavior.

    Explicit semantic versions are authoritative. Otherwise normalized source is
    preferred, with code-object and module-file hashes as deterministic fallbacks.
    The result detects covered implementation changes; it is not an environment or
    transitive-dependency lockfile.
    """

    target = value
    if inspect.isclass(value):
        target = getattr(value, "run", value)
        if explicit_version is None:
            declared = value.__dict__.get("version")
            explicit_version = declared if isinstance(declared, str) else None
    identity = _qualified_name(value)
    if explicit_version is not None:
        if not explicit_version.strip():
            raise WorkflowDefinitionError(f"Explicit version for {identity} must be non-empty")
        return {"identity": identity, "strategy": "explicit", "version": explicit_version}

    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        source = None
    if source:
        normalized = _normalized_source(source)
        return {
            "identity": identity,
            "strategy": "source",
            "sha256": _sha(normalized.encode("utf-8")),
        }

    code = getattr(target, "__code__", None)
    if isinstance(code, types.CodeType):
        return {
            "identity": identity,
            "strategy": "code",
            "sha256": _sha(_code_payload(code, target).encode("utf-8")),
        }

    module = inspect.getmodule(value)
    module_path = getattr(module, "__file__", None)
    if module_path:
        try:
            content = Path(module_path).read_bytes()
        except OSError:
            pass
        else:
            return {"identity": identity, "strategy": "module", "sha256": _sha(content)}
    raise WorkflowDefinitionError(
        f"Cannot establish a durable implementation identity for {identity}; "
        "declare a non-empty version"
    )


def validator_identity(validator: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"class": _qualified_name(type(validator))}
    if hasattr(validator, "function"):
        result["function"] = implementation_identity(getattr(validator, "function"))
    if hasattr(validator, "validators"):
        result["validators"] = [validator_identity(item) for item in validator.validators]
    if is_dataclass(validator):
        config: dict[str, Any] = {}
        for item in fields(validator):
            if item.name in {"function", "validators"}:
                continue
            config[item.name] = _safe_config(getattr(validator, item.name))
        if config:
            result["config"] = config
    return result


def _qualified_name(value: Any) -> str:
    return f"{getattr(value, '__module__', type(value).__module__)}.{getattr(value, '__qualname__', type(value).__qualname__)}"


def _normalized_source(source: str) -> str:
    source = textwrap.dedent(source)
    try:
        return ast.dump(ast.parse(source), annotate_fields=True, include_attributes=False)
    except SyntaxError:
        return "\n".join(line.rstrip() for line in source.strip().splitlines())


def _code_payload(code: types.CodeType, function: Any) -> str:
    def constant(value: Any) -> Any:
        if isinstance(value, types.CodeType):
            return {"code": json.loads(_code_payload(value, function))}
        if value is None or isinstance(value, (str, int, float, bool, bytes)):
            return value.hex() if isinstance(value, bytes) else value
        return {"type": _qualified_name(type(value)), "repr": repr(value)}

    defaults = getattr(function, "__defaults__", None)
    kwdefaults = getattr(function, "__kwdefaults__", None)
    annotations = getattr(function, "__annotations__", None)
    closure = getattr(function, "__closure__", None)
    payload = {
        "argcount": code.co_argcount,
        "posonlyargcount": code.co_posonlyargcount,
        "kwonlyargcount": code.co_kwonlyargcount,
        "flags": code.co_flags,
        "code": code.co_code.hex(),
        "consts": [constant(item) for item in code.co_consts],
        "names": list(code.co_names),
        "varnames": list(code.co_varnames),
        "freevars": list(code.co_freevars),
        "cellvars": list(code.co_cellvars),
        "defaults": _safe_config(defaults),
        "kwdefaults": _safe_config(kwdefaults),
        "annotations": _safe_config(annotations),
        # Closure values are deliberately excluded: mutable runtime state must not
        # alter a definition fingerprint. Free-variable names above retain shape.
        "closure_size": len(closure or ()),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _safe_config(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, type):
        return _qualified_name(value)
    if callable(value):
        return implementation_identity(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return [_safe_config(item) for item in value]
    if isinstance(value, list):
        return [_safe_config(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _safe_config(child) for key, child in sorted(value.items(), key=lambda item: str(item[0]))}
    try:
        return canonical_json_value(value, path="fingerprint configuration")
    except Exception:
        return {"type": _qualified_name(type(value))}


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
