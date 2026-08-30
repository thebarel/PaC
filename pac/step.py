from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar, get_args, get_origin

from .models import StepStatus
from .results import StepResult
from .waits import TimeoutAction, WaitKind, WaitRequest

if TYPE_CHECKING:
    from .context import StepContext
    from .validators import Validator

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Step(Generic[InputT, OutputT]):
    """Base class for a unit of durable workflow work.

    Untyped subclasses keep the original JSON-only ``run(ctx)`` contract.
    Typed subclasses may use ``run(ctx, inputs)`` and return typed output values.
    """

    max_attempts: int = 1
    version: str | None = None
    validator_version: str | None = None
    input_type: Any = Any
    output_type: Any = Any
    validators: tuple[Validator, ...] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is Step:
                arguments = get_args(base)
                if len(arguments) == 2:
                    if "input_type" not in cls.__dict__:
                        cls.input_type = arguments[0]
                    if "output_type" not in cls.__dict__:
                        cls.output_type = arguments[1]

    def run(self, ctx: StepContext, inputs: InputT | None = None) -> StepResult[OutputT]:
        raise NotImplementedError

    def validate_output(self, output: Any, ctx: StepContext) -> str | None:
        """Return ``None`` to accept output or a non-empty rejection reason."""

        return None

    @staticmethod
    def complete(value: Any = None) -> StepResult[Any]:
        return StepResult(StepStatus.COMPLETED, output=value)

    @staticmethod
    def retry(reason: str | None = None) -> StepResult[Any]:
        return StepResult(StepStatus.RETRY, reason=reason)

    @staticmethod
    def repeat(value: Any = None, *, reason: str | None = None) -> StepResult[Any]:
        """Complete this cycle pass and request another iteration."""

        return StepResult(StepStatus.REPEAT, output=value, reason=reason)

    @staticmethod
    def wait(
        reason: str | None = None,
        *,
        signal: str | None = None,
        timeout: datetime | timedelta | None = None,
        on_timeout: TimeoutAction | str = TimeoutAction.FAIL,
        payload_type: Any = Any,
    ) -> StepResult[Any]:
        if signal is None and timeout is None:
            return StepResult(
                StepStatus.WAITING,
                reason=reason,
                wait=WaitRequest(WaitKind.LEGACY, reason=reason),
            )
        timeout_at = timeout if isinstance(timeout, datetime) else None
        if isinstance(timeout, timedelta):
            timeout_at = datetime.now(UTC) + timeout
        action = TimeoutAction(on_timeout)
        if signal is None:
            if timeout_at is None:
                raise ValueError("A timed wait requires a datetime or timedelta timeout")
            if action is not TimeoutAction.RESUME:
                return StepResult(
                    StepStatus.WAITING,
                    reason=reason,
                    wait=WaitRequest(
                        WaitKind.LEGACY,
                        reason=reason,
                        timeout_at=timeout_at,
                        timeout_action=action,
                        payload_type=payload_type,
                    ),
                )
            return Step.wait_until(timeout_at, reason=reason)
        return StepResult(
            StepStatus.WAITING,
            reason=reason,
            wait=WaitRequest(
                WaitKind.SIGNAL,
                reason=reason,
                signal=signal,
                timeout_at=timeout_at,
                timeout_action=action,
                payload_type=payload_type,
            ),
        )

    @staticmethod
    def wait_until(
        when: datetime,
        *,
        reason: str | None = None,
    ) -> StepResult[Any]:
        request = WaitRequest.until(when, reason=reason)
        return StepResult(StepStatus.WAITING, reason=reason, wait=request)

    @staticmethod
    def wait_for(duration: timedelta, *, reason: str | None = None) -> StepResult[Any]:
        request = WaitRequest.for_duration(duration, reason=reason)
        return StepResult(StepStatus.WAITING, reason=reason, wait=request)

    @staticmethod
    def fail(reason: str) -> StepResult[Any]:
        return StepResult(StepStatus.FAILED, reason=reason)
