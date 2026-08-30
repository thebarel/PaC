from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from pac import (
    HumanApproval,
    StateStoreError,
    Step,
    StepStatus,
    Worker,
    Workflow,
    WorkflowRegistry,
    approved,
    rejected,
    timed_out,
)
from pac.state import SQLiteStateStore


def test_human_routes_select_one_branch_and_skip_others(tmp_path):
    class Gate(HumanApproval):
        pass

    class Deploy(Step):
        def run(self, ctx):
            return self.complete("deployed")

    class Revise(Step):
        def run(self, ctx):
            return self.complete("revised")

    workflow = Workflow("routes", state_path=tmp_path / "state.db")
    workflow.add_step(Gate)
    workflow.add_step(Deploy, depends_on=[approved(Gate)])
    workflow.add_step(Revise, depends_on=[rejected(Gate)])
    waiting = workflow.loop()
    workflow.reject(waiting.id, Gate, reason="needs work")

    completed = workflow.resume(waiting.id)
    assert completed.output(Revise) == "revised"
    assert completed.steps[next(key for key in completed.steps if key.endswith("Deploy"))].status is StepStatus.SKIPPED
    assert "step.skipped" in [event.type for event in completed.events]


def test_human_timeout_route_is_durable(tmp_path):
    now = datetime.now(UTC)

    class Clock:
        def __call__(self):
            return now

    class Gate(HumanApproval):
        timeout = timedelta(seconds=5)
        route_timeout = True

    class Escalate(Step):
        def run(self, ctx):
            return self.complete("escalated")

    store = SQLiteStateStore(tmp_path / "state.db", clock=Clock())
    workflow = Workflow("timeout-route", state_store=store)
    workflow.add_step(Gate)
    workflow.add_step(Escalate, depends_on=[timed_out(Gate)])
    waiting = workflow.loop()
    now += timedelta(seconds=6)
    store.process_due_waits()
    completed = workflow.resume(waiting.id)
    assert completed.output(Escalate) == "escalated"
    assert store.human_task(waiting.id, next(key for key in completed.steps if key.endswith("Gate"))).status == "TIMED_OUT"


def test_signal_payload_is_validated_against_wait_schema(tmp_path):
    @dataclass
    class Payment:
        payment_id: str
        amount: int

    class WaitForPayment(Step):
        def run(self, ctx):
            if ctx.signal_payload is None:
                return self.wait(signal="payment", payload_type=Payment)
            return self.complete(ctx.signal_payload)

    workflow = Workflow("typed-signal", state_path=tmp_path / "state.db").add_step(WaitForPayment)
    waiting = workflow.loop()
    with pytest.raises(StateStoreError, match="Expected integer"):
        workflow.signal(waiting.id, "payment", {"payment_id": "p1", "amount": "bad"})
    workflow.signal(waiting.id, "payment", {"payment_id": "p1", "amount": 10})
    assert workflow.resume(waiting.id).output(WaitForPayment)["amount"] == 10


def test_worker_run_once_discovers_ready_runs_and_heartbeats(tmp_path):
    class Work(Step):
        def run(self, ctx):
            return self.complete(ctx.input("value"))

    store = SQLiteStateStore(tmp_path / "state.db")
    workflow = Workflow("queued", state_store=store).add_step(Work, inputs={"value": 3})
    run = workflow.start()
    worker = Worker(WorkflowRegistry([workflow]), worker_id="worker-1")
    results = asyncio.run(worker.run_once())
    assert results[0].id == run.id
    assert results[0].output(Work) == 3
    assert store.list_workers()[0]["heartbeat_at"]


def test_worker_run_forever_can_stop_without_busy_poll(tmp_path):
    workflow = Workflow("idle", state_path=tmp_path / "state.db")
    worker = Worker(WorkflowRegistry([workflow]), worker_id="idle-worker")
    asyncio.run(worker.run_forever(idle_interval=60, max_cycles=1))
