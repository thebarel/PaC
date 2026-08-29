from __future__ import annotations

import pytest

from pac import (
    Step,
    StepOutputSerializationError,
    Workflow,
    WorkflowCycleError,
    WorkflowDefinitionError,
    WorkflowFailed,
)


def test_failure_stops_downstream_work(tmp_path):
    calls: list[str] = []

    class A(Step):
        def run(self, ctx):
            calls.append("A")
            return self.complete("a")

    class B(Step):
        def run(self, ctx):
            calls.append("B")
            raise RuntimeError("boom")

    class C(Step):
        def run(self, ctx):
            calls.append("C")
            return self.complete("c")

    workflow = Workflow("failure", state_path=tmp_path / "state.db")
    workflow.add_step(A)
    workflow.add_step(B, depends_on=[A])
    workflow.add_step(C, depends_on=[B])

    with pytest.raises(WorkflowFailed, match="boom"):
        workflow.loop()
    assert calls == ["A", "B"]


def test_cycle_is_rejected_before_execution(tmp_path):
    calls = 0

    class A(Step):
        def run(self, ctx):
            nonlocal calls
            calls += 1
            return self.complete()

    class B(Step):
        pass

    class C(Step):
        pass

    workflow = Workflow("cycle", state_path=tmp_path / "state.db")
    workflow.add_step(A, depends_on=[C])
    workflow.add_step(B, depends_on=[A])
    workflow.add_step(C, depends_on=[B])

    with pytest.raises(WorkflowCycleError, match="A.*C.*B.*A|A.*B.*C.*A"):
        workflow.loop()
    assert calls == 0


def test_unknown_dependency_is_rejected(tmp_path):
    class A(Step):
        pass

    class Missing(Step):
        pass

    workflow = Workflow("unknown", state_path=tmp_path / "state.db")
    workflow.add_step(A, depends_on=[Missing])
    with pytest.raises(WorkflowDefinitionError, match="unregistered"):
        workflow.loop()


def test_non_json_output_fails_cleanly(tmp_path):
    class BadOutput(Step):
        def run(self, ctx):
            return self.complete(object())

    workflow = Workflow("serialization", state_path=tmp_path / "state.db")
    workflow.add_step(BadOutput)
    with pytest.raises(StepOutputSerializationError):
        workflow.loop()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_rejected(tmp_path, value):
    class BadNumber(Step):
        def run(self, ctx):
            return self.complete(value)

    workflow = Workflow(f"number-{value!r}", state_path=tmp_path / "state.db")
    workflow.add_step(BadNumber)
    with pytest.raises(StepOutputSerializationError):
        workflow.loop()

