from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .models import StepStatus
from .results import StepResult

if TYPE_CHECKING:
    from .context import StepContext


class Step:
    """Base class for a unit of durable workflow work."""

    max_attempts: int = 1

    def run(self, ctx: StepContext) -> StepResult:
        raise NotImplementedError

    def validate_output(self, output: Any, ctx: StepContext) -> str | None:
        """Return ``None`` to accept output or a non-empty rejection reason."""

        return None

    @staticmethod
    def complete(value: Any = None) -> StepResult:
        return StepResult(StepStatus.COMPLETED, output=value)

    @staticmethod
    def retry(reason: str | None = None) -> StepResult:
        return StepResult(StepStatus.RETRY, reason=reason)

    @staticmethod
    def repeat(value: Any = None, *, reason: str | None = None) -> StepResult:
        """Complete this cycle pass and request another iteration."""

        return StepResult(StepStatus.REPEAT, output=value, reason=reason)

    @staticmethod
    def wait(reason: str | None = None) -> StepResult:
        return StepResult(StepStatus.WAITING, reason=reason)

    @staticmethod
    def fail(reason: str) -> StepResult:
        return StepResult(StepStatus.FAILED, reason=reason)
