from __future__ import annotations

from pac import Step, Workflow, WorkflowStatus


def test_basic_workflow_executes_each_step_once(tmp_path):
    calls: list[str] = []

    class First(Step):
        def run(self, ctx):
            calls.append("first")
            return self.complete("hello")

    class Second(Step):
        def run(self, ctx):
            calls.append("second")
            return self.complete(ctx.output(First) + " world")

    workflow = Workflow("demo", state_path=tmp_path / "state.db")
    workflow.add_step(First)
    workflow.add_step(Second, depends_on=[First])

    run = workflow.loop()

    assert run.status is WorkflowStatus.COMPLETED
    assert run.output(First) == "hello"
    assert run.output(Second) == "hello world"
    assert calls == ["first", "second"]
    assert [event.sequence for event in run.events] == list(range(1, len(run.events) + 1))


def test_terminal_loop_call_creates_a_new_run(tmp_path):
    class Only(Step):
        def run(self, ctx):
            return self.complete("done")

    workflow = Workflow("repeatable", state_path=tmp_path / "state.db")
    workflow.add_step(Only)
    first = workflow.loop()
    second = workflow.loop()

    assert first.id != second.id
    assert second.output(Only) == "done"

