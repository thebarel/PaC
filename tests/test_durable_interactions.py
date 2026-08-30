from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pac import HumanApproval, Step, Workflow, WorkflowFailed, WorkflowStatus
from pac.state import SQLiteStateStore


def test_signal_wait_is_durable_idempotent_and_restart_safe(tmp_path):
    database = tmp_path / "state.db"

    class WaitForPayment(Step):
        def run(self, ctx):
            if ctx.signal_payload is None:
                return self.wait(signal="payment_received")
            return self.complete(ctx.signal_payload)

    first = Workflow("payment", state_path=database).add_step(WaitForPayment)
    waiting = first.loop()
    assert waiting.status is WorkflowStatus.WAITING

    restarted = Workflow("payment", state_path=database).add_step(WaitForPayment)
    still_waiting = restarted.resume(waiting.id)
    assert still_waiting.status is WorkflowStatus.WAITING

    receipt = restarted.signal(
        waiting.id,
        "payment_received",
        {"payment_id": "pay_1"},
        event_id="evt_1",
        actor={"service": "billing"},
    )
    duplicate = restarted.signal(
        waiting.id, "payment_received", {"payment_id": "pay_1"}, event_id="evt_1"
    )
    assert receipt.consumed
    assert duplicate.duplicate

    completed = restarted.resume(waiting.id)
    assert completed.output(WaitForPayment) == {"payment_id": "pay_1"}
    assert [event.type for event in completed.events].count("signal.received") == 1
    assert "signal.consumed" in [event.type for event in completed.events]


def test_signal_can_arrive_before_wait(tmp_path):
    class WaitForCustomer(Step):
        def run(self, ctx):
            if ctx.signal_payload is None:
                return self.wait(signal="customer")
            return self.complete(ctx.signal_payload)

    workflow = Workflow("early-signal", state_path=tmp_path / "state.db")
    workflow.add_step(WaitForCustomer)
    run = workflow.start()
    receipt = workflow.signal(run.id, "customer", "ready", event_id="early")
    assert not receipt.consumed
    completed = workflow.resume(run.id)
    assert completed.output(WaitForCustomer) == "ready"


def test_timer_wakes_without_manual_generic_resume(tmp_path):
    now = datetime(2026, 1, 1, tzinfo=UTC)

    class Clock:
        def __call__(self):
            return now

    class Timer(Step):
        calls = 0

        def run(self, ctx):
            self.__class__.calls += 1
            if self.__class__.calls == 1:
                return self.wait_until(now + timedelta(seconds=10))
            return self.complete("awake")

    store = SQLiteStateStore(tmp_path / "state.db", clock=Clock())
    workflow = Workflow("timer", state_store=store).add_step(Timer)
    waiting = workflow.loop()
    assert waiting.status is WorkflowStatus.WAITING
    assert store.next_wakeup_at() is not None
    assert workflow.resume(waiting.id).status is WorkflowStatus.WAITING
    now += timedelta(seconds=11)
    assert store.process_due_waits() == (waiting.id,)
    assert waiting.id in store.ready_runs()
    assert workflow.resume(waiting.id).output(Timer) == "awake"


def test_human_approval_approve_and_reject_are_audited(tmp_path):
    class Gate(HumanApproval):
        pass

    approved_workflow = Workflow("human-ok", state_path=tmp_path / "ok.db").add_step(Gate)
    waiting = approved_workflow.loop()
    task = approved_workflow.approve(
        waiting.id,
        Gate,
        payload={"ticket": "CHG-1"},
        comment="reviewed",
        actor={"id": "alice"},
        event_id="approval-1",
    )
    assert task.status == "APPROVED"
    completed = approved_workflow.resume(waiting.id)
    assert completed.output(Gate)["decision"] == "approved"
    assert "human.approval_received" in [event.type for event in completed.events]

    rejected_workflow = Workflow("human-no", state_path=tmp_path / "no.db").add_step(Gate)
    rejected_wait = rejected_workflow.loop()
    rejected_workflow.reject(
        rejected_wait.id, Gate, reason="unsafe", actor={"id": "bob"}
    )
    with pytest.raises(WorkflowFailed, match="unsafe"):
        rejected_workflow.resume(rejected_wait.id)


def test_cancellation_is_persisted_and_prevents_resume(tmp_path):
    class Waiting(Step):
        def run(self, ctx):
            return self.wait(signal="never")

    workflow = Workflow("cancel", state_path=tmp_path / "state.db").add_step(Waiting)
    waiting = workflow.loop()
    cancelled = workflow.cancel(waiting.id, reason="operator request", actor={"id": "ops"})
    assert cancelled.status is WorkflowStatus.CANCELLED
    assert cancelled.cancellation_reason == "operator request"
    assert workflow.resume(waiting.id).status is WorkflowStatus.CANCELLED
    assert "workflow.cancelled" in [event.type for event in cancelled.events]


def test_async_step_timeout_is_persisted(tmp_path):
    class Slow(Step):
        async def run(self, ctx):
            await asyncio.sleep(0.1)
            return self.complete()

    workflow = Workflow(
        "timeout", state_path=tmp_path / "state.db", step_timeout=timedelta(milliseconds=5)
    ).add_step(Slow)
    with pytest.raises(WorkflowFailed, match="timed out"):
        workflow.loop()
