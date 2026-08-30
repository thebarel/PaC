from __future__ import annotations

from tempfile import TemporaryDirectory

from pac import Step, Worker, Workflow, WorkflowRegistry


class Work(Step):
    def run(self, ctx):
        return self.complete({"worker-safe": True, "attempt": ctx.attempt})


def main() -> None:
    with TemporaryDirectory() as directory:
        workflow = Workflow("multi-worker", state_path=f"{directory}/state.db")
        workflow.add_step(Work)
        run = workflow.start()

        registry = WorkflowRegistry([workflow])
        worker = Worker(registry, worker_id="worker-1", max_concurrency=4)
        completed = worker.run_sync(run.id)
        print(completed.output(Work))
        print(workflow.state_store.list_workers())


if __name__ == "__main__":
    main()
