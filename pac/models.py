from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeAlias

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    RETRY = "RETRY"
    REPEAT = "REPEAT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CycleStatus(str, Enum):
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"


def step_identity(step: type[Any] | str) -> str:
    if isinstance(step, str):
        return step
    return f"{step.__module__}.{step.__qualname__}"


@dataclass(frozen=True, slots=True)
class StepDefinition:
    id: str
    step_class: type[Any] = field(compare=False, repr=False)
    registration_order: int
    dependencies: tuple[str, ...]
    max_attempts: int
    inputs: dict[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    name: str
    steps: tuple[StepDefinition, ...]
    fingerprint: str
    canonical_json: str
    cycles: tuple["CycleDefinition", ...] = ()

    @property
    def step_ids(self) -> frozenset[str]:
        return frozenset(step.id for step in self.steps)


@dataclass(frozen=True, slots=True)
class CycleDefinition:
    name: str
    members: tuple[str, ...]
    controller: str
    entry: str
    max_iterations: int


@dataclass(frozen=True, slots=True)
class CycleState:
    name: str
    members: tuple[str, ...]
    controller: str
    entry: str
    max_iterations: int
    iteration: int
    status: CycleStatus


@dataclass(frozen=True, slots=True)
class StepState:
    id: str
    registration_order: int
    dependencies: tuple[str, ...]
    max_attempts: int
    status: StepStatus
    attempt: int
    inputs: dict[str, JsonValue] = field(default_factory=dict)
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    output: JsonValue = None
    codex_thread_id: str | None = None
    retry_reason: str | None = None
    waiting_reason: str | None = None
    iteration: int = 1
    has_output: bool = False


@dataclass(frozen=True, slots=True)
class WorkflowEvent:
    sequence: int
    type: str
    timestamp: str
    step_id: str | None = None
    attempt: int | None = None
    data: dict[str, JsonValue] = field(default_factory=dict)
    iteration: int | None = None


@dataclass(frozen=True, slots=True)
class WorkflowRun:
    id: str
    name: str
    status: WorkflowStatus
    definition_fingerprint: str
    created_at: str
    started_at: str | None
    completed_at: str | None
    error: str | None
    steps: dict[str, StepState]
    outputs: dict[str, JsonValue]
    events: tuple[WorkflowEvent, ...] = ()
    cycles: dict[str, CycleState] = field(default_factory=dict)

    def output(self, step: type[Any] | str) -> JsonValue:
        step_id = step_identity(step)
        state = self.steps.get(step_id)
        if state is None:
            raise KeyError(f"Step {step_id!r} is not registered in workflow run {self.id}")
        if state.status is not StepStatus.COMPLETED:
            raise ValueError(
                f"Step {step_id!r} has not completed; current status is {state.status.value}"
            )
        return state.output
