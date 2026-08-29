from __future__ import annotations

import pytest

from pac import Step, Workflow, WorkflowDefinitionChanged


class ProcessStopped(BaseException):
    pass


def test_resume_skips_completed_steps_and_recovers_running(tmp_path):
    first_calls = 0
    second_calls = 0
    stop = True

    class First(Step):
        def run(self, ctx):
            nonlocal first_calls
            first_calls += 1
            return self.complete("a")

    class Second(Step):
        max_attempts = 2

        def run(self, ctx):
            nonlocal second_calls, stop
            second_calls += 1
            if stop:
                stop = False
                raise ProcessStopped()
            return self.complete(ctx.output(First) + "b")

    database = tmp_path / "state.db"
    first_process = Workflow("resume", state_path=database)
    first_process.add_step(First)
    first_process.add_step(Second, depends_on=[First])
    with pytest.raises(ProcessStopped):
        first_process.loop()

    resumed = Workflow("resume", state_path=database)
    resumed.add_step(First)
    resumed.add_step(Second, depends_on=[First])
    run = resumed.loop()

    assert run.output(Second) == "ab"
    assert first_calls == 1
    assert second_calls == 2
    assert "step.recovered" in [event.type for event in run.events]


def test_definition_change_is_detected_before_resume(tmp_path):
    class First(Step):
        def run(self, ctx):
            return self.complete("a")

    class Waiting(Step):
        def run(self, ctx):
            return self.wait("pause")

    class Inserted(Step):
        def run(self, ctx):
            return self.complete("new")

    database = tmp_path / "state.db"
    original = Workflow("changed", state_path=database)
    original.add_step(First)
    original.add_step(Waiting, depends_on=[First])
    unfinished = original.loop()

    changed = Workflow("changed", state_path=database)
    changed.add_step(First)
    changed.add_step(Inserted, depends_on=[First])
    changed.add_step(Waiting, depends_on=[Inserted])

    with pytest.raises(WorkflowDefinitionChanged):
        changed.loop()

    assert changed.state_store.get_run(unfinished.id).status.value == "WAITING"

