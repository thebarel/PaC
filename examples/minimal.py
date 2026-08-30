from __future__ import annotations

from tempfile import TemporaryDirectory

from pac import Step, Workflow


class First(Step):
    def run(self, ctx):
        return self.complete("hello")


class Second(Step):
    def run(self, ctx):
        return self.complete(ctx.output(First) + " world")


def main() -> None:
    with TemporaryDirectory() as directory:
        workflow = Workflow("minimal", state_path=f"{directory}/state.db")
        workflow.add_step(First)
        workflow.add_step(Second, depends_on=[First])
        print(workflow.run().output(Second))


if __name__ == "__main__":
    main()
