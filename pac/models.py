from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
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
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class CycleStatus(str, Enum):
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"


@dataclass(frozen=True, slots=True)
class SignalReceipt:
    run_id: str
    name: str
    event_id: str
    duplicate: bool
    consumed: bool
    payload: JsonValue = None


@dataclass(frozen=True, slots=True)
class IdempotencyClaim:
    run_id: str
    step_id: str
    iteration: int
    action: str
    key: str
    token: str
    completed: bool = False
    result: JsonValue = None


@dataclass(frozen=True, slots=True)
class HumanTask:
    run_id: str
    step_id: str
    status: str
    requested_at: str
    responded_at: str | None = None
    actor: JsonValue = None
    comment: str | None = None
    payload: JsonValue = None
    timeout_at: str | None = None


@dataclass(frozen=True, slots=True)
class StepClaim:
    """Exclusive, leased permission to execute one logical step attempt."""

    run_id: str
    step_id: str
    worker_id: str
    token: str
    attempt: int
    iteration: int
    claimed_at: str
    lease_expires_at: str


DEFAULT_LEASE_DURATION = timedelta(minutes=2)


def step_identity(step: type[Any] | str) -> str:
    if isinstance(step, str):
        return step
    return f"{step.__module__}.{step.__qualname__}"


@dataclass(frozen=True, slots=True)
class ConditionalDependency:
    """A dependency selected by a human decision outcome."""

    step: type[Any]
    outcome: str


@dataclass(frozen=True, slots=True)
class StepDefinition:
    id: str
    step_class: type[Any] = field(compare=False, repr=False)
    registration_order: int
    dependencies: tuple[str, ...]
    max_attempts: int
    inputs: JsonValue = field(default_factory=dict)
    input_type: Any = field(default=Any, compare=False, repr=False)
    output_type: Any = field(default=Any, compare=False, repr=False)
    accepts_typed_input: bool = False
    dependency_conditions: tuple[tuple[str, str], ...] = ()


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
    inputs: JsonValue = field(default_factory=dict)
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    output: JsonValue = None
    codex_thread_id: str | None = None
    retry_reason: str | None = None
    waiting_reason: str | None = None
    iteration: int = 1
    has_output: bool = False
    claim_owner: str | None = None
    claim_token: str | None = None
    claimed_at: str | None = None
    lease_expires_at: str | None = None
    heartbeat_at: str | None = None
    available_at: str | None = None
    signal_payload: JsonValue = None
    cancellation_requested: bool = False


@dataclass(frozen=True, slots=True)
class WorkflowEvent:
    sequence: int
    type: str
    timestamp: str
    step_id: str | None = None
    attempt: int | None = None
    data: dict[str, JsonValue] = field(default_factory=dict)
    iteration: int | None = None
    schema_version: int = 1


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
    cancellation_reason: str | None = None

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
