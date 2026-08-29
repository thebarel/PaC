from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import WorkflowRun


class PacError(Exception):
    """Base class for PaC errors."""


class WorkflowDefinitionError(PacError):
    """The declared workflow cannot be executed."""


class WorkflowCycleError(WorkflowDefinitionError):
    """The workflow dependency graph contains a cycle."""


class WorkflowDefinitionChanged(WorkflowDefinitionError):
    """A persisted unfinished run has a different definition."""


class WorkflowExecutionError(PacError):
    """The workflow runtime could not safely execute a run."""


class WorkflowFailed(WorkflowExecutionError):
    """A workflow reached its persisted FAILED state."""

    def __init__(self, message: str, run: WorkflowRun) -> None:
        super().__init__(message)
        self.run = run


class WorkflowDeadlockError(WorkflowExecutionError):
    """No step can make progress and the workflow is not terminal."""


class StepExecutionError(WorkflowExecutionError):
    """A step violated the runtime contract."""


class StepOutputSerializationError(StepExecutionError):
    """A completed step returned a value that cannot be stored as JSON."""


class StateStoreError(PacError):
    """Persistent workflow state could not be read or updated."""

