from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pac import ConcurrencyError, Step, Workflow, WorkflowStatus
from pac.models import StepDefinition, WorkflowDefinition
from pac.scheduler import runnable_steps
from pac.state import SQLiteStateStore


def test_native_async_step_and_dependency_barrier(tmp_path):
    calls: list[str] = []

    class First(Step):
        async def run(self, ctx):
            await asyncio.sleep(0)
            calls.append("first")
            return self.complete("a")

    class Second(Step):
        async def run(self, ctx):
            calls.append("second")
            return self.complete(ctx.output(First) + "b")

    async def execute():
        workflow = Workflow("async", state_path=tmp_path / "state.db")
        workflow.add_step(First)
        workflow.add_step(Second, depends_on=[First])
        return await workflow.arun()

    run = asyncio.run(execute())
    assert run.status is WorkflowStatus.COMPLETED
    assert run.output(Second) == "ab"
    assert calls == ["first", "second"]


def test_independent_steps_execute_concurrently_with_limit(tmp_path):
    running = 0
    peak = 0
    started = asyncio.Event()

    def branch(name):
        class Branch(Step):
            async def run(self, ctx):
                nonlocal running, peak
                running += 1
                peak = max(peak, running)
                if running == 2:
                    started.set()
                await asyncio.wait_for(started.wait(), timeout=1)
                await asyncio.sleep(0.01)
                running -= 1
                return self.complete(name)
        Branch.__name__ = name
        Branch.__qualname__ = name
        return Branch

    A, B, C = branch("A"), branch("B"), branch("C")

    async def execute():
        workflow = Workflow(
            "parallel", state_path=tmp_path / "state.db", max_concurrency=2
        )
        workflow.add_step(A)
        workflow.add_step(B)
        workflow.add_step(C)
        return await workflow.arun()

    run = asyncio.run(execute())
    assert run.status is WorkflowStatus.COMPLETED
    assert peak == 2
    claimed = [event.step_id for event in run.events if event.type == "step.claimed"]
    assert claimed == [f"{A.__module__}.A", f"{B.__module__}.B", f"{C.__module__}.C"]


def test_long_running_step_heartbeats_its_claim(tmp_path):
    heartbeats = 0
    store = SQLiteStateStore(tmp_path / "heartbeat.db")
    original = store.heartbeat_claim

    def count_heartbeat(token, *, lease_duration):
        nonlocal heartbeats
        heartbeats += 1
        return original(token, lease_duration=lease_duration)

    store.heartbeat_claim = count_heartbeat

    class Slow(Step):
        async def run(self, ctx):
            await asyncio.sleep(0.12)
            return self.complete("done")

    workflow = Workflow(
        "heartbeat",
        state_store=store,
        lease_duration=timedelta(milliseconds=90),
    ).add_step(Slow)

    assert workflow.run().output(Slow) == "done"
    assert heartbeats >= 1


def test_cancellation_while_async_step_runs_returns_cancelled_run(tmp_path):
    started = asyncio.Event()

    class Slow(Step):
        async def run(self, ctx):
            started.set()
            await asyncio.sleep(10)
            return self.complete("late")

    workflow = Workflow(
        "live-cancel",
        state_path=tmp_path / "cancel.db",
        lease_duration=timedelta(milliseconds=150),
    ).add_step(Slow)
    run = workflow.start()

    async def execute():
        running = asyncio.create_task(workflow.aresume(run.id))
        await started.wait()
        workflow.cancel(run.id, reason="stop")
        return await running

    cancelled = asyncio.run(execute())
    assert cancelled.status is WorkflowStatus.CANCELLED


def test_runnable_steps_returns_registration_ordered_set():
    class A(Step):
        pass

    class B(Step):
        pass

    definition = WorkflowDefinition(
        name="ordered",
        fingerprint="x",
        canonical_json="{}",
        steps=(
            StepDefinition("A", A, 0, (), 1),
            StepDefinition("B", B, 1, (), 1),
        ),
    )
    from pac.models import StepState

    states = {
        "A": StepState("A", 0, (), 1, status=__import__("pac").StepStatus.PENDING, attempt=0),
        "B": StepState("B", 1, (), 1, status=__import__("pac").StepStatus.PENDING, attempt=0),
    }
    assert runnable_steps(definition, states) == ("A", "B")


def test_two_workers_cannot_claim_same_step_and_stale_ack_is_rejected(tmp_path):
    class Only(Step):
        pass

    definition = WorkflowDefinition(
        name="claims",
        fingerprint="x",
        canonical_json="{}",
        steps=(StepDefinition("Only", Only, 0, (), 2),),
    )
    store = SQLiteStateStore(tmp_path / "state.db")
    run = store.create_run(definition)
    store.start_workflow(run.id)
    first = store.claim_step(
        run.id, "Only", "worker-one", lease_duration=timedelta(minutes=1)
    )
    with pytest.raises(ConcurrencyError):
        store.claim_step(
            run.id, "Only", "worker-two", lease_duration=timedelta(minutes=1)
        )
    with pytest.raises(ConcurrencyError):
        store.complete_step(run.id, "Only", "bad", claim_token="wrong")
    store.complete_step(run.id, "Only", "ok", claim_token=first.token)
    assert store.get_run(run.id).steps["Only"].output == "ok"


def test_losing_a_claim_race_does_not_mark_the_run_deadlocked(tmp_path):
    class Only(Step):
        def run(self, ctx):
            return self.complete("ok")

    path = tmp_path / "race.db"
    workflow = Workflow("claim-race", state_path=path, worker_id="observer").add_step(Only)
    run = workflow.start()
    rival = SQLiteStateStore(path)
    original = workflow.state_store.claim_step
    rival_claim = None

    def lose_race(run_id, step_id, worker_id, *, lease_duration):
        nonlocal rival_claim
        if rival_claim is None:
            rival_claim = rival.claim_step(
                run_id, step_id, "rival", lease_duration=lease_duration
            )
        return original(
            run_id, step_id, worker_id, lease_duration=lease_duration
        )

    workflow.state_store.claim_step = lose_race
    observed = workflow.resume(run.id)
    assert observed.status is WorkflowStatus.RUNNING
    assert rival_claim is not None
    rival.interrupt_claim(rival_claim.token, "test cleanup")


def test_expired_claim_consumes_attempt_and_is_recoverable(tmp_path):
    now = datetime(2026, 1, 1, tzinfo=UTC)

    class Clock:
        def __call__(self):
            return now

    class Only(Step):
        pass

    definition = WorkflowDefinition(
        name="expired",
        fingerprint="x",
        canonical_json="{}",
        steps=(StepDefinition("Only", Only, 0, (), 2),),
    )
    store = SQLiteStateStore(tmp_path / "state.db", clock=Clock())
    run = store.create_run(definition)
    store.start_workflow(run.id)
    claim = store.claim_step(
        run.id, "Only", "worker", lease_duration=timedelta(seconds=1)
    )
    now += timedelta(seconds=2)
    assert store.recover_expired_claims() == (claim,)
    state = store.get_run(run.id).steps["Only"]
    assert state.status.value == "RETRY"
    assert state.attempt == 1
    with pytest.raises(ConcurrencyError):
        store.complete_step(run.id, "Only", "late", claim_token=claim.token)
