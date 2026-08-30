from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .idempotency import IdempotencyManager
from .models import JsonValue, WorkflowRun
from .runtime.binding import BoundAgent
from .secrets import SecretResolver

_MISSING = object()


@dataclass(frozen=True, slots=True)
class StepContext:
    """Durable capabilities and declared inputs available to one step invocation."""

    workflow_id: str
    run_id: str
    step_id: str
    attempt: int
    codex: Any
    agent: BoundAgent
    secrets: SecretResolver
    idempotency: IdempotencyManager
    retry_reason: str | None
    inputs: Any
    _output: Callable[[type[Any] | str], JsonValue]
    _latest_output: Callable[[type[Any] | str], tuple[bool, JsonValue]]
    _state: Callable[[], WorkflowRun]
    iteration: int = 1
    signal_payload: JsonValue = None

    def input(self, name: str, default: Any = _MISSING) -> JsonValue:
        """Return one declared step input, or a default when supplied."""

        if isinstance(self.inputs, Mapping) and name in self.inputs:
            return self.inputs[name]
        if default is not _MISSING:
            return default
        available = (
            ", ".join(sorted(self.inputs)) if isinstance(self.inputs, Mapping) else "none"
        ) or "none"
        raise KeyError(
            f"Input {name!r} is not declared for step {self.step_id!r}; "
            f"available inputs: {available}"
        )

    def output(self, step: type[Any] | str) -> JsonValue:
        return self._output(step)

    def latest_output(self, step: type[Any] | str, default: Any = _MISSING) -> JsonValue:
        """Return the latest persisted output, including a prior cycle iteration."""

        exists, value = self._latest_output(step)
        if exists:
            return value
        if default is not _MISSING:
            return default
        raise ValueError(f"Step {step!r} has no persisted output")

    def state(self) -> WorkflowRun:
        return self._state()

    @property
    def idempotency_key(self) -> str:
        """Stable key for this logical step and cycle iteration, across retries."""

        return self.idempotency.key

    @property
    def attempt_idempotency_key(self) -> str:
        """Stable key for this exact execution attempt."""

        return self.idempotency.attempt_key

    def idempotency_key_for(self, action: str, *, attempt_scoped: bool = False) -> str:
        return self.idempotency.key_for(action, attempt_scoped=attempt_scoped)

    def once(self, action: str, operation: Callable[[], Any]) -> Any:
        return self.idempotency.once(action, operation)

    async def once_async(self, action: str, operation: Callable[[], Any]) -> Any:
        return await self.idempotency.once_async(action, operation)

    @property
    def cancelled(self) -> bool:
        run = self._state()
        step = run.steps[self.step_id]
        return run.status.value in {"CANCELLING", "CANCELLED"} or step.cancellation_requested

    def check_cancelled(self) -> None:
        if self.cancelled:
            raise RuntimeError(f"Workflow run {self.run_id} was cancelled")
