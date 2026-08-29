from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager

from ..models import JsonValue, StepState, WorkflowDefinition, WorkflowRun


class StateStore(ABC):
    """Persistence boundary used by the deterministic workflow engine."""

    @abstractmethod
    def execution_lock(self, workflow_name: str) -> AbstractContextManager[None]: ...

    @abstractmethod
    def active_run(self, workflow_name: str) -> WorkflowRun | None: ...

    @abstractmethod
    def create_run(self, definition: WorkflowDefinition) -> WorkflowRun: ...

    @abstractmethod
    def get_run(self, run_id: str) -> WorkflowRun: ...

    @abstractmethod
    def start_workflow(self, run_id: str) -> None: ...

    @abstractmethod
    def resume_waiting(self, run_id: str) -> None: ...

    @abstractmethod
    def recover_running(self, run_id: str) -> None: ...

    @abstractmethod
    def start_step(self, run_id: str, step_id: str) -> StepState: ...

    @abstractmethod
    def complete_step(self, run_id: str, step_id: str, output: JsonValue) -> None: ...

    @abstractmethod
    def repeat_cycle(
        self,
        run_id: str,
        cycle_name: str,
        step_id: str,
        output: JsonValue,
        reason: str | None,
    ) -> bool: ...

    @abstractmethod
    def retry_step(
        self,
        run_id: str,
        step_id: str,
        reason: str,
        *,
        candidate: JsonValue = None,
        rejected: bool = False,
    ) -> bool: ...

    @abstractmethod
    def wait_step(self, run_id: str, step_id: str, reason: str | None) -> None: ...

    @abstractmethod
    def fail_step(self, run_id: str, step_id: str, error: str) -> None: ...

    @abstractmethod
    def complete_workflow(self, run_id: str) -> None: ...

    @abstractmethod
    def wait_workflow(self, run_id: str) -> None: ...

    @abstractmethod
    def fail_workflow(self, run_id: str, error: str) -> None: ...

    @abstractmethod
    def set_codex_thread(self, run_id: str, step_id: str, thread_id: str) -> None: ...

    @abstractmethod
    def record_codex_turn(
        self,
        run_id: str,
        step_id: str,
        event_type: str,
        turn_id: str,
        data: dict[str, JsonValue] | None = None,
    ) -> None: ...
