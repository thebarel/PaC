from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .models import JsonValue, WorkflowRun

_MISSING = object()


@dataclass(frozen=True, slots=True)
class StepContext:
    """Durable capabilities and declared inputs available to one step invocation."""

    workflow_id: str
    run_id: str
    step_id: str
    attempt: int
    codex: Any
    retry_reason: str | None
    inputs: Mapping[str, JsonValue]
    _output: Callable[[type[Any] | str], JsonValue]
    _latest_output: Callable[[type[Any] | str], tuple[bool, JsonValue]]
    _state: Callable[[], WorkflowRun]
    iteration: int = 1

    def input(self, name: str, default: Any = _MISSING) -> JsonValue:
        """Return one declared step input, or a default when supplied."""

        if name in self.inputs:
            return self.inputs[name]
        if default is not _MISSING:
            return default
        available = ", ".join(sorted(self.inputs)) or "none"
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
