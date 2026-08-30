from __future__ import annotations

import asyncio
from time import perf_counter
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from ..state.base import StateStore

from .base import AgentExecutionContext, AgentRequest, AgentResult, AgentRuntime


class BoundAgent:
    """Provider-neutral agent capability bound to one step invocation."""

    def __init__(
        self,
        runtime: AgentRuntime,
        context: AgentExecutionContext,
        store: StateStore | None = None,
    ) -> None:
        self._runtime = runtime
        self._context = context
        self._store = store

    async def execute(self, request: AgentRequest | str, **kwargs: Any) -> AgentResult:
        if isinstance(request, str):
            request = AgentRequest(prompt=request, **kwargs)
        invocation_id = str(uuid4())
        runtime_name = f"{type(self._runtime).__module__}.{type(self._runtime).__qualname__}"
        if self._store is not None:
            self._store.start_agent_invocation(
                self._context.run_id,
                self._context.step_id,
                invocation_id,
                runtime=runtime_name,
                model=request.model,
            )
        started = perf_counter()
        try:
            invocation = self._runtime.execute(request, self._context)
            result = (
                await invocation
                if request.timeout_seconds is None
                else await asyncio.wait_for(invocation, timeout=request.timeout_seconds)
            )
        except asyncio.CancelledError:
            if self._store is not None:
                self._store.finish_agent_invocation(
                    invocation_id, error="CancelledError: agent invocation cancelled"
                )
            raise
        except Exception as exc:
            if self._store is not None:
                self._store.finish_agent_invocation(
                    invocation_id, error=f"{type(exc).__name__}: {exc}"
                )
            raise
        if result.usage.latency_seconds is None:
            from dataclasses import replace

            result = replace(
                result,
                usage=replace(result.usage, latency_seconds=perf_counter() - started),
            )
        if self._store is not None:
            self._store.finish_agent_invocation(
                invocation_id,
                provider=result.provider,
                model=result.model,
                usage=result.usage,
            )
        return result

    def run(self, prompt: Any, **kwargs: Any) -> AgentResult:
        """Synchronous convenience for synchronous steps.

        Async steps should await :meth:`execute` instead.
        """

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.execute(AgentRequest(prompt=prompt, **kwargs))
            )
        raise RuntimeError("BoundAgent.run() cannot be used inside an active event loop; await execute()")
