from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import StepStatus


@dataclass(frozen=True, slots=True)
class StepResult:
    """The outcome requested by one invocation of ``Step.run``."""

    status: StepStatus
    output: Any = None
    reason: str | None = None
