from __future__ import annotations

from tempfile import TemporaryDirectory

from pac import Step, Workflow


class Recoverable(Step):
    max_attempts = 2

    def run(self, ctx):
        return self.complete({"attempt": ctx.attempt, "recovered": bool(ctx.retry_reason)})


def main() -> None:
    with TemporaryDirectory() as directory:
        path = f"{directory}/state.db"
        first_process = Workflow("crash-recovery", state_path=path)
        first_process.add_step(Recoverable)
        created = first_process.start()

        # Simulate the original pre-lease execution record left by a process that
        # stopped after persisting RUNNING. Resume detects it, consumes attempt 1,
        # records recovery, and executes attempt 2.
        first_process.state_store.start_workflow(created.id)
        first_process.state_store.start_step(
            created.id,
            f"{Recoverable.__module__}.{Recoverable.__qualname__}",
        )

        restarted = Workflow("crash-recovery", state_path=path)
        restarted.add_step(Recoverable)
        run = restarted.resume(created.id)
        print(run.output(Recoverable))
        print([event.type for event in run.events if "recover" in event.type or "interrupt" in event.type])


if __name__ == "__main__":
    main()
