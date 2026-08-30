from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from .waits import WaitRequest

from .models import StepStatus

OutputT = TypeVar("OutputT")


@dataclass(frozen=True, slots=True)
class StepResult(Generic[OutputT]):
    """The outcome requested by one invocation of ``Step.run``."""

    status: StepStatus
    output: OutputT | None = None
    reason: str | None = None
    wait: WaitRequest | None = None
