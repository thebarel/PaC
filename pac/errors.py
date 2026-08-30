from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import WorkflowRun


class PaCError(Exception):
    """Base class for PaC errors."""


# Keep the original spelling as a source-compatible alias.
PacError = PaCError


class ConfigurationError(PaCError):
    """PaC was configured with incompatible or invalid options."""


class WorkflowDefinitionError(PaCError):
    """The declared workflow cannot be executed."""


class WorkflowCycleError(WorkflowDefinitionError):
    """The workflow dependency graph contains a cycle."""


class WorkflowDefinitionChanged(WorkflowDefinitionError):
    """A persisted unfinished run has a different definition."""


class ExecutionError(PaCError):
    """The workflow runtime could not safely execute a run."""


# Preserve the public pre-0.2 name as an alias so existing handlers keep working.
WorkflowExecutionError = ExecutionError


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


class PersistenceError(PaCError):
    """Persistent workflow state could not be read or updated."""


# Preserve the public pre-0.2 name as an alias.
StateStoreError = PersistenceError


class ConcurrencyError(ExecutionError):
    """Concurrent execution could not proceed safely."""


class RuntimeError(ExecutionError):
    """An agent or execution runtime failed."""


class SignalError(ExecutionError):
    """An external signal could not be accepted or applied."""


class ValidationError(ExecutionError):
    """Data crossing a workflow boundary failed validation."""


class EncryptionError(PersistenceError):
    """Persisted encrypted data could not be protected or authenticated."""

