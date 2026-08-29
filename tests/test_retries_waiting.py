from __future__ import annotations

import pytest

from pac import Step, Workflow, WorkflowFailed, WorkflowStatus


def test_explicit_retry_is_bounded(tmp_path):
    calls = 0

    class Retrying(Step):
        max_attempts = 3

        def run(self, ctx):
            nonlocal calls
            calls += 1
            return self.retry("again")

    workflow = Workflow("retry", state_path=tmp_path / "state.db")
    workflow.add_step(Retrying)

    with pytest.raises(WorkflowFailed):
        workflow.loop()
    assert calls == 3


def test_wait_returns_and_next_loop_resumes_same_attempt(tmp_path):
    calls = 0

    class Waiting(Step):
        def run(self, ctx):
            nonlocal calls
            calls += 1
            if calls == 1:
                return self.wait("external condition")
            return self.complete("ready")

    workflow = Workflow("waiting", state_path=tmp_path / "state.db")
    workflow.add_step(Waiting)

    waiting = workflow.loop()
    assert waiting.status is WorkflowStatus.WAITING
    assert calls == 1

    completed = workflow.loop()
    assert completed.status is WorkflowStatus.COMPLETED
    assert completed.output(Waiting) == "ready"
    assert completed.steps[next(iter(completed.steps))].attempt == 1
    assert calls == 2
    assert "step.resumed" in [event.type for event in completed.events]

