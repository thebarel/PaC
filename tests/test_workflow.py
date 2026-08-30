from __future__ import annotations

import pytest

from pac import ConcurrencyError, Step, Workflow, WorkflowDefinitionChanged, WorkflowStatus


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


def test_explicit_runs_with_same_workflow_name_execute_independently(tmp_path):
    class Echo(Step):
        def run(self, ctx):
            return self.complete(ctx.input("value"))

    database = tmp_path / "state.db"
    first_workflow = Workflow("parallel-runs", state_path=database)
    first_workflow.add_step(Echo, inputs={"value": "first"})
    second_workflow = Workflow("parallel-runs", state_path=database)
    second_workflow.add_step(Echo, inputs={"value": "first"})

    first = first_workflow.start(run_id="first")
    second = second_workflow.start(run_id="second")
    assert first.id != second.id

    first = first_workflow.resume(first.id)
    second = second_workflow.resume(second.id)
    assert first.output(Echo) == "first"
    assert second.output(Echo) == "first"


def test_new_instance_requires_run_id_when_resume_is_ambiguous(tmp_path):
    class Waiting(Step):
        def run(self, ctx):
            return self.wait("pause")

    database = tmp_path / "state.db"
    one = Workflow("ambiguous", state_path=database)
    one.add_step(Waiting)
    two = Workflow("ambiguous", state_path=database)
    two.add_step(Waiting)
    first = one.start()
    second = two.start()

    restarted = Workflow("ambiguous", state_path=database)
    restarted.add_step(Waiting)
    with pytest.raises(ConcurrencyError, match="specify a run ID"):
        restarted.loop()

    assert restarted.resume(first.id).id == first.id
    assert restarted.resume(second.id).id == second.id


def test_explicit_resume_validates_workflow_identity(tmp_path):
    class Only(Step):
        def run(self, ctx):
            return self.complete()

    database = tmp_path / "state.db"
    original = Workflow("original", state_path=database)
    original.add_step(Only)
    run = original.start()

    other = Workflow("other", state_path=database)
    other.add_step(Only)
    with pytest.raises(WorkflowDefinitionChanged, match="belongs to workflow"):
        other.resume(run.id)

