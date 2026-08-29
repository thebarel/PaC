from __future__ import annotations

from pac import Step, StepStatus
from pac.models import StepDefinition, StepState, WorkflowDefinition
from pac.scheduler import next_runnable_step


class A(Step):
    pass


class B(Step):
    pass


class C(Step):
    pass


def _definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        name="scheduler",
        fingerprint="test",
        canonical_json="{}",
        steps=(
            StepDefinition("A", A, 0, (), 1),
            StepDefinition("B", B, 1, ("A",), 1),
            StepDefinition("C", C, 2, ("A",), 1),
        ),
    )


def _state(step_id: str, order: int, status: StepStatus) -> StepState:
    return StepState(step_id, order, (), 1, status, 0)


def test_scheduler_is_pure_and_uses_registration_order():
    definition = _definition()
    states = {
        "C": _state("C", 2, StepStatus.PENDING),
        "B": _state("B", 1, StepStatus.PENDING),
        "A": _state("A", 0, StepStatus.COMPLETED),
    }

    assert {next_runnable_step(definition, states) for _ in range(100)} == {"B"}


def test_diamond_executes_deterministically(tmp_path):
    calls: list[str] = []

    class Root(Step):
        def run(self, ctx):
            calls.append("A")
            return self.complete("a")

    class Left(Step):
        def run(self, ctx):
            calls.append("B")
            return self.complete("b")

    class Right(Step):
        def run(self, ctx):
            calls.append("C")
            return self.complete("c")

    class Join(Step):
        def run(self, ctx):
            calls.append("D")
            return self.complete(ctx.output(Left) + ctx.output(Right))

    from pac import Workflow

    workflow = Workflow("diamond", state_path=tmp_path / "state.db")
    workflow.add_step(Root)
    workflow.add_step(Left, depends_on=[Root])
    workflow.add_step(Right, depends_on=[Root])
    workflow.add_step(Join, depends_on=[Right, Left])

    assert workflow.loop().output(Join) == "bc"
    assert calls == ["A", "B", "C", "D"]

