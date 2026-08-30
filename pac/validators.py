from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from .codecs import validate_and_encode
from .errors import ValidationError


class Validator(Protocol):
    def validate(self, value: Any, context: Any) -> str | None: ...


@dataclass(frozen=True, slots=True)
class SchemaValidator:
    annotation: Any

    def validate(self, value: Any, context: Any = None) -> str | None:
        try:
            validate_and_encode(value, self.annotation, path="output")
        except ValidationError as exc:
            return str(exc)
        return None


@dataclass(frozen=True, slots=True)
class FunctionValidator:
    function: Callable[[Any, Any], str | None]

    def validate(self, value: Any, context: Any) -> str | None:
        return self.function(value, context)


class CompositeValidator:
    def __init__(self, validators: Iterable[Validator]) -> None:
        self.validators = tuple(validators)

    def validate(self, value: Any, context: Any) -> str | None:
        for validator in self.validators:
            reason = validator.validate(value, context)
            if reason is not None:
                return reason
        return None
