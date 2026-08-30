from __future__ import annotations

from tempfile import TemporaryDirectory

from pac import HumanApproval, Workflow


class ProductionApproval(HumanApproval):
    pass


def main() -> None:
    with TemporaryDirectory() as directory:
        workflow = Workflow("human-approval", state_path=f"{directory}/state.db")
        workflow.add_step(ProductionApproval)

        waiting = workflow.run()
        print("waiting:", waiting.id, waiting.status.value)

        workflow.approve(
            waiting.id,
            ProductionApproval,
            payload={"ticket": "CHG-42"},
            comment="approved for the example",
            actor={"id": "example-reviewer"},
            event_id="approval-42",
        )
        completed = workflow.resume(waiting.id)
        print(completed.output(ProductionApproval))


if __name__ == "__main__":
    main()
