from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Protocol

from ..models import JsonValue
from ..secrets import SecretResolver


@dataclass(frozen=True, slots=True)
class AgentUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_tokens: int | None = None
    total_tokens: int | None = None
    cost: Decimal | None = None
    currency: str | None = None
    latency_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class AgentRequest:
    prompt: Any
    output_schema: dict[str, JsonValue] | None = None
    model: str | None = None
    timeout_seconds: float | None = None
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AgentResult:
    output: JsonValue | str | None
    provider: str
    model: str | None = None
    invocation_id: str | None = None
    session_id: str | None = None
    usage: AgentUsage = field(default_factory=AgentUsage)
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)
    raw: Any = field(default=None, compare=False, repr=False)

    @property
    def text(self) -> str | None:
        return self.output if isinstance(self.output, str) or self.output is None else None

    @property
    def thread_id(self) -> str | None:
        return self.session_id

    @property
    def turn_id(self) -> str | None:
        return self.invocation_id


def _no_session(runtime: str) -> JsonValue | None:
    del runtime
    return None


def _cannot_save_session(runtime: str, value: JsonValue) -> None:
    del value
    raise RuntimeError(f"Runtime session persistence is unavailable for {runtime!r}")


@dataclass(frozen=True, slots=True)
class AgentExecutionContext:
    workflow_id: str
    run_id: str
    step_id: str
    attempt: int
    iteration: int
    secrets: SecretResolver
    cwd: Path = Path(".")
    load_session: Callable[[str], JsonValue | None] = _no_session
    save_session: Callable[[str, JsonValue], None] = _cannot_save_session


@dataclass(frozen=True, slots=True)
class AgentInvocation:
    invocation_id: str
    run_id: str
    step_id: str
    attempt: int
    iteration: int
    runtime: str
    provider: str | None
    model: str | None
    status: str
    started_at: str
    completed_at: str | None = None
    usage: AgentUsage = field(default_factory=AgentUsage)
    error: str | None = None


class AgentRuntime(Protocol):
    async def execute(
        self, request: AgentRequest, context: AgentExecutionContext
    ) -> AgentResult: ...
