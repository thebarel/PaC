from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any


class WaitKind(str, Enum):
    LEGACY = "legacy"
    SIGNAL = "signal"
    TIMER = "timer"
    HUMAN = "human"


class TimeoutAction(str, Enum):
    FAIL = "fail"
    RETRY = "retry"
    CANCEL = "cancel"
    RESUME = "resume"


@dataclass(frozen=True, slots=True)
class WaitRequest:
    kind: WaitKind
    reason: str | None = None
    signal: str | None = None
    wake_at: datetime | None = None
    timeout_at: datetime | None = None
    timeout_action: TimeoutAction = TimeoutAction.FAIL
    payload_type: Any = Any

    def __post_init__(self) -> None:
        for field_name in ("wake_at", "timeout_at"):
            value = getattr(self, field_name)
            if value is not None and value.tzinfo is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if self.kind in (WaitKind.SIGNAL, WaitKind.HUMAN) and not self.signal:
            raise ValueError(f"{self.kind.value} waits require a signal name")
        if self.kind is WaitKind.TIMER and self.wake_at is None:
            raise ValueError("timer waits require wake_at")

    @classmethod
    def until(
        cls,
        when: datetime,
        *,
        reason: str | None = None,
        on_timeout: TimeoutAction | str = TimeoutAction.RESUME,
    ) -> WaitRequest:
        return cls(
            WaitKind.TIMER,
            reason=reason,
            wake_at=when.astimezone(UTC),
            timeout_action=TimeoutAction(on_timeout),
        )

    @classmethod
    def for_duration(
        cls,
        duration: timedelta,
        *,
        reason: str | None = None,
        now: datetime | None = None,
    ) -> WaitRequest:
        if duration.total_seconds() < 0:
            raise ValueError("wait duration cannot be negative")
        base = now or datetime.now(UTC)
        return cls.until(base + duration, reason=reason)
