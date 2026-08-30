from __future__ import annotations

from decimal import Decimal

from pac import (
    AgentResult,
    AgentUsage,
    EventExportCursor,
    FakeAgentRuntime,
    SQLiteStateStore,
    Step,
    Workflow,
)
from pac.cli import main


class AgentStep(Step):
    async def run(self, ctx):
        result = await ctx.agent.execute("do not persist this prompt")
        return self.complete(result.output)


def test_agent_usage_events_and_aggregation(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    runtime = FakeAgentRuntime(
        [
            AgentResult(
                "ok",
                provider="fake",
                model="test-model",
                usage=AgentUsage(
                    input_tokens=5,
                    output_tokens=3,
                    total_tokens=8,
                    cost=Decimal("0.012"),
                    currency="USD",
                ),
            )
        ]
    )
    workflow = Workflow("observed", state_store=store, agent_runtime=runtime)
    workflow.add_step(AgentStep)

    run = workflow.run()

    invocations = store.agent_invocations(run.id)
    assert len(invocations) == 1
    assert invocations[0].usage.total_tokens == 8
    usage = store.usage(run.id)
    assert usage.total_tokens == 8
    assert usage.cost == Decimal("0.012")
    assert usage.invocations == 1
    event_types = [event.type for event in run.events]
    assert "step.runnable" in event_types
    assert "agent.request_started" in event_types
    assert "agent.request_finished" in event_types
    assert all("do not persist" not in repr(event.data) for event in run.events)
    finished = next(event for event in run.events if event.type == "agent.request_finished")
    assert finished.data["usage"]["input_tokens"] == 5
    assert finished.data["usage"]["output_tokens"] == 3
    assert finished.data["usage"]["total_tokens"] == 8
    assert [event.sequence for event in run.events] == list(range(1, len(run.events) + 1))


class Collector:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail

    def export(self, events):
        if self.fail:
            raise RuntimeError("offline")
        self.events.extend(events)


def test_export_cursor_advances_only_after_success(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    workflow = Workflow("exported", state_store=store)
    workflow.add_step(type("Done", (Step,), {"run": lambda self, ctx: self.complete("ok")}))
    run = workflow.run()

    failing = EventExportCursor(store, Collector(fail=True), name="sink")
    try:
        failing.export_run(run.id)
    except RuntimeError:
        pass
    assert len(store.pending_export_events(run.id, "sink")) == len(run.events)

    collector = Collector()
    cursor = EventExportCursor(store, collector, name="sink")
    assert cursor.export_run(run.id) == len(run.events)
    assert cursor.export_run(run.id) == 0


def test_cli_lists_and_inspects_runs(tmp_path, capsys):
    path = tmp_path / "state.db"
    workflow = Workflow("cli-test", state_path=path)
    workflow.add_step(type("Done", (Step,), {"run": lambda self, ctx: self.complete("ok")}))
    run = workflow.run()

    assert main(["--db", str(path), "runs"]) == 0
    assert run.id in capsys.readouterr().out
    assert main(["--db", str(path), "inspect", run.id]) == 0
    output = capsys.readouterr().out
    assert '"status": "COMPLETED"' in output
    assert '"usage"' in output
