from __future__ import annotations

import hashlib
from typing import Awaitable, Callable, TypeVar

from .codecs import canonical_json_value
from .state.base import StateStore

T = TypeVar("T")


class IdempotencyManager:
    """Durable idempotency records scoped to one logical step iteration.

    This cannot make an unrelated external side effect exactly once. For external
    APIs, pass ``key_for(action)`` to the provider's own idempotency facility.
    """

    __slots__ = ("_store", "_run_id", "_step_id", "_iteration", "_attempt")

    def __init__(
        self,
        store: StateStore,
        run_id: str,
        step_id: str,
        iteration: int,
        attempt: int,
    ) -> None:
        self._store = store
        self._run_id = run_id
        self._step_id = step_id
        self._iteration = iteration
        self._attempt = attempt

    @property
    def key(self) -> str:
        return self.key_for("step")

    @property
    def attempt_key(self) -> str:
        return self.key_for("attempt", attempt_scoped=True)

    def key_for(self, action: str, *, attempt_scoped: bool = False) -> str:
        if not isinstance(action, str) or not action.strip():
            raise ValueError("Idempotency action must be a non-empty string")
        scope = [self._run_id, self._step_id, str(self._iteration), action]
        if attempt_scoped:
            scope.append(str(self._attempt))
        digest = hashlib.sha256("\x1f".join(scope).encode("utf-8")).hexdigest()
        return f"pac:{digest}"

    def once(self, action: str, operation: Callable[[], T]) -> T:
        claim = self._store.claim_idempotency(
            self._run_id,
            self._step_id,
            self._iteration,
            action,
            self.key_for(action),
        )
        if claim.completed:
            return claim.result  # type: ignore[return-value]
        try:
            result = operation()
            if hasattr(result, "__await__"):
                self._store.release_idempotency(claim.token)
                raise TypeError("once() received an async operation; use await once_async()")
            encoded = canonical_json_value(result, path=f"idempotency result {action}")
        except BaseException:
            self._store.release_idempotency(claim.token)
            raise
        self._store.complete_idempotency(claim.token, encoded)
        return result

    async def once_async(self, action: str, operation: Callable[[], Awaitable[T]]) -> T:
        claim = self._store.claim_idempotency(
            self._run_id,
            self._step_id,
            self._iteration,
            action,
            self.key_for(action),
        )
        if claim.completed:
            return claim.result  # type: ignore[return-value]
        try:
            result = await operation()
            encoded = canonical_json_value(result, path=f"idempotency result {action}")
        except BaseException:
            self._store.release_idempotency(claim.token)
            raise
        self._store.complete_idempotency(claim.token, encoded)
        return result
