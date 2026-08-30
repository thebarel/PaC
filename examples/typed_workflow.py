from __future__ import annotations

from dataclasses import dataclass
from tempfile import TemporaryDirectory

from pac import Step, Workflow


@dataclass
class ResearchInput:
    topic: str


@dataclass
class ResearchOutput:
    findings: list[str]
    confidence: float


class Research(Step[ResearchInput, ResearchOutput]):
    def run(self, ctx, inputs: ResearchInput):
        return self.complete(
            ResearchOutput(
                findings=[f"PaC can durably coordinate {inputs.topic}"],
                confidence=0.95,
            )
        )


class Present(Step):
    def run(self, ctx):
        research = ctx.output(Research)
        return self.complete(research.findings)


def main() -> None:
    with TemporaryDirectory() as directory:
        workflow = Workflow("typed", state_path=f"{directory}/state.db")
        workflow.add_step(Research, inputs=ResearchInput(topic="workflows"))
        workflow.add_step(Present, depends_on=[Research])
        print(workflow.run().output(Present))


if __name__ == "__main__":
    main()
