from __future__ import annotations

from datetime import UTC, datetime, timedelta
from tempfile import TemporaryDirectory

from pac import SQLiteStateStore, Step, Workflow, WorkflowStatus


class Reminder(Step):
    calls = 0

    def run(self, ctx):
        self.__class__.calls += 1
        if self.__class__.calls == 1:
            return self.wait_until(
                datetime(2026, 1, 1, 0, 0, 5, tzinfo=UTC),
                reason="durable delay",
            )
        return self.complete("timer fired")


def main() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)

    def clock():
        return now

    with TemporaryDirectory() as directory:
        store = SQLiteStateStore(f"{directory}/state.db", clock=clock)
        workflow = Workflow("durable-timer", state_store=store)
        workflow.add_step(Reminder)
        waiting = workflow.run()
        assert waiting.status is WorkflowStatus.WAITING
        print("next wake:", store.next_wakeup_at())

        now += timedelta(seconds=6)
        store.process_due_waits()
        print(workflow.resume(waiting.id).output(Reminder))


if __name__ == "__main__":
    main()
