from __future__ import annotations

from collections.abc import Iterable

from ..errors import RuntimeError
from .base import AgentExecutionContext, AgentRequest, AgentResult


class FakeAgentRuntime:
    """Deterministic scripted runtime for tests and local examples."""

    def __init__(self, results: Iterable[AgentResult | str]) -> None:
        self._results = iter(results)
        self.requests: list[tuple[AgentRequest, AgentExecutionContext]] = []

    async def execute(
        self, request: AgentRequest, context: AgentExecutionContext
    ) -> AgentResult:
        self.requests.append((request, context))
        try:
            result = next(self._results)
        except StopIteration as exc:
            raise RuntimeError("FakeAgentRuntime has no scripted result remaining") from exc
        if isinstance(result, AgentResult):
            return result
        return AgentResult(output=result, provider="fake", model=request.model)
