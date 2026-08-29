from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from ..models import JsonValue
from ..state.base import StateStore

if TYPE_CHECKING:
    from openai_codex import Sandbox, TurnResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AgentResult:
    text: str | None
    thread_id: str
    turn_id: str
    raw: TurnResult


class _StepCodex:
    def __init__(self, runtime: CodexRuntime, step_id: str) -> None:
        self._runtime = runtime
        self._step_id = step_id

    def run(
        self,
        prompt: Any,
        *,
        output_schema: dict[str, JsonValue] | None = None,
    ) -> AgentResult:
        return self._runtime.run(self._step_id, prompt, output_schema=output_schema)


class CodexRuntime:
    """PaC's focused wrapper around the official Python Codex SDK."""

    def __init__(
        self,
        *,
        store: StateStore,
        run_id: str,
        cwd: str | Path,
        model: str | None = None,
        sandbox: Sandbox | None = None,
    ) -> None:
        self._store = store
        self._run_id = run_id
        self._cwd = str(cwd)
        self._model = model
        self._sandbox = sandbox
        self._client: Any = None
        self._threads: dict[str, Any] = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._threads.clear()

    def bind_step(self, step_id: str) -> _StepCodex:
        return _StepCodex(self, step_id)

    def _ensure_client(self) -> Any:
        if self._client is None:
            from openai_codex import Codex, CodexConfig

            self._client = Codex(CodexConfig(cwd=self._cwd))
        return self._client

    def _thread(self, step_id: str) -> Any:
        cached = self._threads.get(step_id)
        if cached is not None:
            return cached

        client = self._ensure_client()
        state = self._store.get_run(self._run_id).steps[step_id]
        options = {
            "cwd": self._cwd,
            "model": self._model,
            "sandbox": self._sandbox,
        }
        options = {key: value for key, value in options.items() if value is not None}
        if state.codex_thread_id is None:
            thread = client.thread_start(**options)
            self._store.set_codex_thread(self._run_id, step_id, thread.id)
            logger.info(
                "Codex thread started",
                extra={
                    "run_id": self._run_id,
                    "step_id": step_id,
                    "codex_thread_id": thread.id,
                },
            )
        else:
            thread = client.thread_resume(state.codex_thread_id, **options)
            logger.info(
                "Codex thread resumed",
                extra={
                    "run_id": self._run_id,
                    "step_id": step_id,
                    "codex_thread_id": thread.id,
                },
            )
        self._threads[step_id] = thread
        return thread

    def run(
        self,
        step_id: str,
        prompt: Any,
        *,
        output_schema: dict[str, JsonValue] | None = None,
    ) -> AgentResult:
        thread = self._thread(step_id)
        turn = thread.turn(prompt, output_schema=output_schema)
        self._store.record_codex_turn(
            self._run_id, step_id, "codex.turn_started", turn.id
        )
        try:
            raw = turn.run()
        except Exception as exc:
            self._store.record_codex_turn(
                self._run_id,
                step_id,
                "codex.turn_failed",
                turn.id,
                {"error": f"{type(exc).__name__}: {exc}"},
            )
            raise
        status = getattr(raw.status, "value", str(raw.status))
        self._store.record_codex_turn(
            self._run_id,
            step_id,
            "codex.turn_completed",
            turn.id,
            {"status": status},
        )
        logger.info(
            "Codex turn completed",
            extra={
                "run_id": self._run_id,
                "step_id": step_id,
                "codex_thread_id": thread.id,
                "codex_turn_id": turn.id,
            },
        )
        return AgentResult(
            text=raw.final_response,
            thread_id=thread.id,
            turn_id=turn.id,
            raw=raw,
        )
