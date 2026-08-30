from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol

from .errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class SecretRef:
    """A persistable reference to a secret, never the secret value itself."""

    name: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ConfigurationError("Secret reference name must be a non-empty string")

    def __repr__(self) -> str:
        return f"SecretRef({self.name!r})"


class SecretValue:
    """A resolved secret whose string and repr forms are always redacted."""

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        self.__value = value

    def reveal(self) -> str:
        return self.__value

    def __repr__(self) -> str:
        return "SecretValue(***)"

    def __str__(self) -> str:
        return "***"


@dataclass(frozen=True, slots=True)
class SecretContext:
    workflow_id: str
    run_id: str
    step_id: str


class SecretProvider(Protocol):
    def resolve(self, ref: SecretRef, context: SecretContext) -> SecretValue: ...


class EnvironmentSecretProvider:
    """Resolve secret references from environment variables at execution time."""

    def resolve(self, ref: SecretRef, context: SecretContext) -> SecretValue:
        try:
            value = os.environ[ref.name]
        except KeyError as exc:
            raise ConfigurationError(f"Environment secret {ref.name!r} is not set") from exc
        return SecretValue(value)


class SecretResolver:
    __slots__ = ("_provider", "_context")

    def __init__(self, provider: SecretProvider, context: SecretContext) -> None:
        self._provider = provider
        self._context = context

    def get(self, ref: SecretRef | str) -> SecretValue:
        return self._provider.resolve(
            ref if isinstance(ref, SecretRef) else SecretRef(ref), self._context
        )
