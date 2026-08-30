from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Generic, TypeVar, get_args, get_origin

from .context import StepContext
from .models import JsonValue, StepStatus
from .results import StepResult
from .step import Step
from .waits import TimeoutAction, WaitKind, WaitRequest

PayloadT = TypeVar("PayloadT")


def approved(step: type[HumanApproval[Any]]) -> Any:
    """Run a dependent step only when the human gate was approved."""

    from .models import ConditionalDependency

    return ConditionalDependency(step, "approved")


def rejected(step: type[HumanApproval[Any]]) -> Any:
    """Run a dependent step only when the human gate was rejected."""

    from .models import ConditionalDependency

    return ConditionalDependency(step, "rejected")


def timed_out(step: type[HumanApproval[Any]]) -> Any:
    """Run a dependent step only when the human gate timed out."""

    from .models import ConditionalDependency

    return ConditionalDependency(step, "timed_out")


class HumanApproval(Step[Any, Any], Generic[PayloadT]):
    """A durable human decision point with no UI or transport dependency."""

    timeout: timedelta | None = None
    timeout_action: TimeoutAction = TimeoutAction.FAIL
    route_timeout: bool = False
    payload_type: Any = Any
    _pac_routed_outcomes: frozenset[str] = frozenset()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is HumanApproval:
                arguments = get_args(base)
                if arguments and "payload_type" not in cls.__dict__:
                    cls.payload_type = arguments[0]

    def run(
        self, ctx: StepContext, inputs: Any = None
    ) -> StepResult[dict[str, JsonValue]]:
        del inputs
        response = ctx.signal_payload
        if response is None:
            timeout_at = datetime.now(UTC) + self.timeout if self.timeout else None
            return StepResult(
                status=StepStatus.WAITING,
                reason="Human approval required",
                wait=WaitRequest(
                    WaitKind.HUMAN,
                    reason="Human approval required",
                    signal=f"human:{ctx.step_id}",
                    timeout_at=timeout_at,
                    timeout_action=(
                        TimeoutAction.RESUME if self.route_timeout else self.timeout_action
                    ),
                    payload_type=self.payload_type,
                ),
            )
        if not isinstance(response, dict):
            return self.fail("Invalid human approval response")
        decision = response.get("decision")
        if decision not in {"approved", "rejected", "timed_out"}:
            return self.fail("Invalid human approval decision")
        routed: frozenset[str] = getattr(self, "_pac_routed_outcomes", frozenset())
        if decision == "rejected" and decision not in routed:
            comment = response.get("comment")
            return self.fail(
                comment if isinstance(comment, str) and comment else "Human approval rejected"
            )
        if decision == "timed_out" and decision not in routed:
            return self.fail("Human approval timed out")
        return self.complete(response)
