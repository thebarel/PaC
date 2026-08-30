from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .runtime.base import AgentUsage


@dataclass(frozen=True, slots=True)
class UsageSummary:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_tokens: int | None = None
    total_tokens: int | None = None
    cost: Decimal | None = None
    currency: str | None = None
    latency_seconds: float | None = None
    invocations: int = 0

    @classmethod
    def combine(cls, usages: list[AgentUsage]) -> UsageSummary:
        def total(name: str):
            values = [getattr(usage, name) for usage in usages if getattr(usage, name) is not None]
            return sum(values) if values else None

        currencies = {usage.currency for usage in usages if usage.cost is not None and usage.currency}
        cost = total("cost") if len(currencies) <= 1 else None
        return cls(
            input_tokens=total("input_tokens"),
            output_tokens=total("output_tokens"),
            cached_tokens=total("cached_tokens"),
            total_tokens=total("total_tokens"),
            cost=cost,
            currency=next(iter(currencies)) if len(currencies) == 1 else None,
            latency_seconds=total("latency_seconds"),
            invocations=len(usages),
        )
