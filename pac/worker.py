from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Iterable

from .errors import ConfigurationError
from .models import WorkflowRun
from .workflow import Workflow


@dataclass(slots=True)
class WorkflowRegistry:
    """Explicit mapping from persisted workflow names to executable definitions."""

    workflows: Iterable[Workflow] = field(default_factory=tuple)
    _by_name: dict[str, Workflow] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._by_name = {workflow.name: workflow for workflow in self.workflows}

    def register(self, workflow: Workflow) -> None:
        if workflow.name in self._by_name:
            raise ConfigurationError(f"Workflow {workflow.name!r} is already registered")
        self._by_name[workflow.name] = workflow

    def get(self, name: str) -> Workflow:
        try:
            return self._by_name[name]
        except KeyError as exc:
            raise ConfigurationError(f"Workflow {name!r} is not registered") from exc


class Worker:
    """Drive ready persisted runs using storage-backed claims and deadlines."""

    def __init__(
        self,
        registry: WorkflowRegistry,
        *,
        worker_id: str,
        max_concurrency: int = 1,
        lease_duration: timedelta = timedelta(minutes=2),
    ) -> None:
        if max_concurrency < 1:
            raise ConfigurationError("max_concurrency must be >= 1")
        self.registry = registry
        self.worker_id = worker_id
        self.max_concurrency = max_concurrency
        self.lease_duration = lease_duration
        for store in self._stores():
            store.register_worker(
                worker_id,
                {
                    "max_concurrency": max_concurrency,
                    "lease_seconds": lease_duration.total_seconds(),
                },
            )

    def _stores(self):
        return tuple(
            {id(workflow.state_store): workflow.state_store for workflow in self.registry._by_name.values()}.values()
        )

    async def run(self, run_id: str) -> WorkflowRun:
        workflow = self._workflow_for_run(run_id)
        workflow.worker_id = self.worker_id
        workflow.max_concurrency = self.max_concurrency
        workflow.lease_duration = self.lease_duration
        return await workflow.aresume(run_id)

    def run_sync(self, run_id: str) -> WorkflowRun:
        return asyncio.run(self.run(run_id))

    async def run_once(self) -> tuple[WorkflowRun, ...]:
        """Recover leases/timers and drive the current deterministic ready set once."""

        ready: list[tuple[Workflow, str]] = []
        for store in self._stores():
            store.heartbeat_worker(self.worker_id)
            store.recover_expired_claims()
            store.process_due_waits()
            for run_id in store.ready_runs():
                try:
                    workflow = self._workflow_for_run(run_id)
                except ConfigurationError:
                    continue
                ready.append((workflow, run_id))
        ready.sort(key=lambda item: (item[0].name, item[1]))
        results: list[WorkflowRun] = []
        for _, run_id in ready[: self.max_concurrency]:
            results.append(await self.run(run_id))
        return tuple(results)

    async def run_forever(
        self,
        *,
        stop: asyncio.Event | None = None,
        idle_interval: float = 30.0,
        max_cycles: int | None = None,
    ) -> None:
        """Run queue cycles, sleeping until a deadline or explicit stop.

        ``max_cycles`` exists for deterministic embedders and tests. A notification-capable
        deployment can set ``stop`` from its signal/webhook listener to wake the worker.
        """

        if idle_interval <= 0:
            raise ConfigurationError("idle_interval must be positive")
        cycles = 0
        while stop is None or not stop.is_set():
            results = await self.run_once()
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                return
            delay = 0.0 if results else self._sleep_delay(idle_interval)
            if delay <= 0:
                await asyncio.sleep(0)
                continue
            if stop is None:
                await asyncio.sleep(delay)
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass

    def _sleep_delay(self, idle_interval: float) -> float:
        deadlines = [store.next_wakeup_at() for store in self._stores()]
        parsed = [datetime.fromisoformat(value) for value in deadlines if value is not None]
        if not parsed:
            return idle_interval
        now = datetime.now(UTC)
        return max(0.0, min(idle_interval, (min(parsed) - now).total_seconds()))

    def recover_expired_claims(self) -> int:
        return sum(len(store.recover_expired_claims()) for store in self._stores())

    def _workflow_for_run(self, run_id: str) -> Workflow:
        for workflow in self.registry._by_name.values():
            try:
                run = workflow.state_store.get_run(run_id)
            except Exception:
                continue
            return self.registry.get(run.name)
        raise ConfigurationError(f"Workflow run {run_id!r} is not available in this registry")
