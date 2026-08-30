from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from datetime import timedelta
from typing import TYPE_CHECKING

from ..models import (
    HumanTask,
    IdempotencyClaim,
    JsonValue,
    SignalReceipt,
    StepClaim,
    StepState,
    WorkflowDefinition,
    WorkflowEvent,
    WorkflowRun,
)

if TYPE_CHECKING:
    from ..runtime.base import AgentInvocation, AgentUsage
    from ..usage import UsageSummary
from ..waits import WaitRequest


class StateStore(ABC):
    """Persistence boundary used by the deterministic workflow engine."""

    @abstractmethod
    def execution_lock(self, workflow_name: str) -> AbstractContextManager[None]: ...

    @abstractmethod
    def active_run(self, workflow_name: str) -> WorkflowRun | None:
        """Return the sole active run, or ``None`` when there is no active run.

        Implementations must raise a concurrency error when multiple active runs
        make this legacy lookup ambiguous.
        """

    def active_runs(self, workflow_name: str) -> tuple[WorkflowRun, ...]:
        """List active runs; legacy stores expose at most one."""

        run = self.active_run(workflow_name)
        return () if run is None else (run,)

    def list_runs(self, workflow_name: str | None = None) -> tuple[WorkflowRun, ...]:
        raise NotImplementedError("This state store does not support run listing")

    @abstractmethod
    def create_run(
        self, definition: WorkflowDefinition, *, run_id: str | None = None
    ) -> WorkflowRun: ...

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

    def claim_step(
        self,
        run_id: str,
        step_id: str,
        worker_id: str,
        *,
        lease_duration: timedelta,
    ) -> StepClaim:
        raise NotImplementedError("This state store does not support leased step claims")

    def heartbeat_claim(
        self, token: str, *, lease_duration: timedelta
    ) -> StepClaim:
        raise NotImplementedError("This state store does not support claim heartbeats")

    def recover_expired_claims(self) -> tuple[StepClaim, ...]:
        return ()

    def interrupt_claim(self, token: str, reason: str) -> None:
        raise NotImplementedError("This state store does not support claim interruption")

    @abstractmethod
    def complete_step(
        self, run_id: str, step_id: str, output: JsonValue, *, claim_token: str | None = None
    ) -> None: ...

    @abstractmethod
    def repeat_cycle(
        self,
        run_id: str,
        cycle_name: str,
        step_id: str,
        output: JsonValue,
        reason: str | None,
        *,
        claim_token: str | None = None,
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
        claim_token: str | None = None,
    ) -> bool: ...

    @abstractmethod
    def wait_step(
        self,
        run_id: str,
        step_id: str,
        reason: str | None,
        *,
        claim_token: str | None = None,
        request: WaitRequest | None = None,
    ) -> None: ...

    def signal(
        self,
        run_id: str,
        name: str,
        payload: JsonValue = None,
        *,
        event_id: str | None = None,
        actor: JsonValue = None,
    ) -> SignalReceipt:
        raise NotImplementedError("This state store does not support signals")

    def process_due_waits(self) -> tuple[str, ...]:
        return ()

    def ready_runs(self) -> tuple[str, ...]:
        return ()

    def next_wakeup_at(self) -> str | None:
        return None

    def register_worker(self, worker_id: str, metadata: JsonValue = None) -> None:
        raise NotImplementedError("This state store does not support worker registration")

    def heartbeat_worker(self, worker_id: str) -> None:
        raise NotImplementedError("This state store does not support worker heartbeats")

    def list_workers(self) -> tuple[dict[str, JsonValue], ...]:
        return ()

    def rotate_encryption(self) -> int:
        raise NotImplementedError("This state store does not support encryption rotation")

    def cancel_run(
        self, run_id: str, *, reason: str | None = None, actor: JsonValue = None
    ) -> WorkflowRun:
        raise NotImplementedError("This state store does not support cancellation")

    def human_task(self, run_id: str, step_id: str) -> HumanTask:
        raise NotImplementedError("This state store does not support human tasks")

    def respond_human(
        self,
        run_id: str,
        step_id: str,
        decision: str,
        *,
        payload: JsonValue = None,
        comment: str | None = None,
        actor: JsonValue = None,
        event_id: str | None = None,
    ) -> HumanTask:
        raise NotImplementedError("This state store does not support human tasks")

    def skip_step(self, run_id: str, step_id: str, *, reason: str) -> None:
        raise NotImplementedError("This state store does not support skipped steps")

    @abstractmethod
    def fail_step(
        self, run_id: str, step_id: str, error: str, *, claim_token: str | None = None
    ) -> None: ...

    @abstractmethod
    def complete_workflow(self, run_id: str) -> None: ...

    @abstractmethod
    def wait_workflow(self, run_id: str) -> None: ...

    @abstractmethod
    def fail_workflow(self, run_id: str, error: str) -> None: ...

    def get_runtime_session(
        self, run_id: str, step_id: str, runtime: str
    ) -> JsonValue | None:
        return None

    def set_runtime_session(
        self, run_id: str, step_id: str, runtime: str, data: JsonValue
    ) -> None:
        raise NotImplementedError("This state store does not support runtime sessions")

    def record_agent_invocation(
        self,
        run_id: str,
        step_id: str,
        event_type: str,
        invocation_id: str,
        data: dict[str, JsonValue] | None = None,
    ) -> None:
        raise NotImplementedError("This state store does not support agent invocations")

    def start_agent_invocation(
        self,
        run_id: str,
        step_id: str,
        invocation_id: str,
        *,
        runtime: str,
        model: str | None = None,
    ) -> None:
        raise NotImplementedError("This state store does not support agent invocations")

    def finish_agent_invocation(
        self,
        invocation_id: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        usage: AgentUsage | None = None,
        error: str | None = None,
    ) -> None:
        raise NotImplementedError("This state store does not support agent invocations")

    def agent_invocations(self, run_id: str) -> tuple[AgentInvocation, ...]:
        return ()

    def usage(self, run_id: str, step_id: str | None = None) -> UsageSummary:
        from ..usage import UsageSummary

        return UsageSummary()

    def pending_export_events(
        self, run_id: str, exporter: str, *, limit: int = 100
    ) -> tuple[WorkflowEvent, ...]:
        raise NotImplementedError("This state store does not support export cursors")

    def advance_export_cursor(self, run_id: str, exporter: str, sequence: int) -> None:
        raise NotImplementedError("This state store does not support export cursors")

    def claim_idempotency(
        self,
        run_id: str,
        step_id: str,
        iteration: int,
        action: str,
        key: str,
        *,
        lease_duration: timedelta = timedelta(minutes=2),
    ) -> IdempotencyClaim:
        raise NotImplementedError("This state store does not support idempotency records")

    def complete_idempotency(self, token: str, result: JsonValue) -> None:
        raise NotImplementedError("This state store does not support idempotency records")

    def release_idempotency(self, token: str) -> None:
        raise NotImplementedError("This state store does not support idempotency records")

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
