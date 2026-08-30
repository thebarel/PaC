from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pac import ConcurrencyError, Step, Workflow, WorkflowDefinitionChanged
from pac.models import StepDefinition, WorkflowDefinition
from pac.state import SQLiteStateStore


def test_idempotent_action_is_reused_across_retries(tmp_path):
    side_effects: list[str] = []
    logical_keys: list[str] = []
    attempt_keys: list[str] = []

    class Send(Step):
        max_attempts = 2

        def run(self, ctx):
            logical_keys.append(ctx.idempotency_key_for("send"))
            attempt_keys.append(ctx.attempt_idempotency_key)
            result = ctx.once("send", lambda: side_effects.append("sent") or {"sent": True})
            if ctx.attempt == 1:
                return self.retry("verify delivery")
            return self.complete(result)

    workflow = Workflow("idempotent-retry", state_path=tmp_path / "state.db")
    workflow.add_step(Send)
    run = workflow.loop()

    assert run.output(Send) == {"sent": True}
    assert side_effects == ["sent"]
    assert logical_keys[0] == logical_keys[1]
    assert attempt_keys[0] != attempt_keys[1]
    assert [event.type for event in run.events].count("idempotency.completed") == 1


def test_async_once_and_cycle_iterations_have_distinct_keys(tmp_path):
    actions: list[int] = []
    keys: list[str] = []

    class CycleAction(Step):
        async def run(self, ctx):
            keys.append(ctx.idempotency_key_for("action"))

            async def action():
                await asyncio.sleep(0)
                actions.append(ctx.iteration)
                return ctx.iteration

            value = await ctx.once_async("action", action)
            if ctx.iteration == 1:
                return self.repeat(value)
            return self.complete(value)

    workflow = Workflow("idempotent-cycle", state_path=tmp_path / "state.db")
    workflow.add_step(CycleAction)
    workflow.add_cycle(
        "cycle", steps=[CycleAction], back_edge=(CycleAction, CycleAction), max_iterations=2
    )

    assert workflow.loop().output(CycleAction) == 2
    assert actions == [1, 2]
    assert len(set(keys)) == 2


def test_idempotency_claims_detect_duplicates_and_recover_expired_lease(tmp_path):
    now = datetime(2026, 1, 1, tzinfo=UTC)

    def clock():
        return now

    class Stored(Step):
        pass

    definition = WorkflowDefinition(
        name="stored",
        fingerprint="v1",
        canonical_json="{}",
        steps=(StepDefinition("stored.Step", Stored, 0, (), 1),),
    )
    store = SQLiteStateStore(tmp_path / "state.db", clock=clock)
    run = store.create_run(definition)
    claim = store.claim_idempotency(
        run.id, "stored.Step", 1, "charge", "stable-key", lease_duration=timedelta(seconds=1)
    )
    with pytest.raises(ConcurrencyError, match="already in progress"):
        store.claim_idempotency(run.id, "stored.Step", 1, "charge", "stable-key")

    now += timedelta(seconds=2)
    recovered = store.claim_idempotency(run.id, "stored.Step", 1, "charge", "stable-key")
    assert recovered.token != claim.token
    store.complete_idempotency(recovered.token, {"ok": True})
    duplicate = store.claim_idempotency(run.id, "stored.Step", 1, "charge", "stable-key")
    assert duplicate.completed is True
    assert duplicate.result == {"ok": True}


def _dynamic_step(value: str) -> type[Step]:
    namespace: dict[str, object] = {"Step": Step}
    exec(
        "class Dynamic(Step):\n"
        "    def run(self, ctx):\n"
        f"        return self.complete({value!r})\n",
        namespace,
    )
    dynamic = namespace["Dynamic"]
    assert isinstance(dynamic, type)
    dynamic.__module__ = "fingerprint_fixture"
    dynamic.__qualname__ = "Dynamic"
    return dynamic


def test_fingerprint_detects_python_implementation_change(tmp_path):
    first_step = _dynamic_step("before")
    second_step = _dynamic_step("after")
    first = Workflow("source-change", state_path=tmp_path / "state.db")
    first.add_step(first_step)
    started = first.start()

    second = Workflow("source-change", state_path=tmp_path / "state.db")
    second.add_step(second_step)
    assert first._definition().fingerprint != second._definition().fingerprint
    with pytest.raises(WorkflowDefinitionChanged):
        second.resume(started.id)


def test_explicit_step_version_controls_implementation_identity(tmp_path):
    first_step = _dynamic_step("before")
    second_step = _dynamic_step("after")
    first_step.version = "behavior-v1"
    second_step.version = "behavior-v1"

    first = Workflow("versioned-source", state_path=tmp_path / "first.db", version="workflow-v1")
    first.add_step(first_step)
    second = Workflow("versioned-source", state_path=tmp_path / "second.db", version="workflow-v1")
    second.add_step(second_step)

    assert first._definition().fingerprint == second._definition().fingerprint
