from __future__ import annotations

from .models import CycleState, CycleStatus, StepState, StepStatus, WorkflowDefinition


def next_runnable_step(
    definition: WorkflowDefinition,
    states: dict[str, StepState],
    cycles: dict[str, CycleState] | None = None,
) -> str | None:
    """Return the next runnable step ID using registration order only."""

    cycles = cycles or {}
    member_cycle = {
        member: cycle
        for cycle in cycles.values()
        for member in cycle.members
    }

    for step in definition.steps:
        state = states[step.id]
        is_pending = state.status is StepStatus.PENDING
        is_retryable = (
            state.status is StepStatus.RETRY and state.attempt < step.max_attempts
        )
        if not (is_pending or is_retryable):
            continue

        def dependency_ready(dependency: str) -> bool:
            if states[dependency].status is not StepStatus.COMPLETED:
                return False
            dependency_cycle = member_cycle.get(dependency)
            step_cycle = member_cycle.get(step.id)
            return not (
                dependency_cycle is not None
                and dependency_cycle.status is CycleStatus.ACTIVE
                and dependency_cycle.name != getattr(step_cycle, "name", None)
            )

        if all(dependency_ready(dependency) for dependency in step.dependencies):
            return step.id
    return None
