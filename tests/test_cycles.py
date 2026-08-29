from __future__ import annotations

import sqlite3

import pytest

from pac import (
    CycleStatus,
    Step,
    StepExecutionError,
    Workflow,
    WorkflowDefinitionError,
    WorkflowFailed,
    WorkflowStatus,
)


def test_cycle_repeats_deterministically_carries_output_and_gates_downstream(tmp_path):
    calls: list[tuple[str, int, object]] = []

    class Entry(Step):
        def run(self, ctx):
            previous = ctx.latest_output(Controller, None)
            calls.append(("entry", ctx.iteration, previous))
            return self.complete({"iteration": ctx.iteration})

    class After(Step):
        def run(self, ctx):
            calls.append(("after", ctx.iteration, ctx.output(Entry)))
            return self.complete("after")

    class Controller(Step):
        def run(self, ctx):
            calls.append(("controller", ctx.iteration, ctx.output(Entry)))
            if ctx.iteration < 3:
                return self.repeat({"feedback": ctx.iteration}, reason="another pass")
            return self.complete({"feedback": "done"})

    database = tmp_path / "state.db"
    workflow = Workflow("repeatable-cycle", state_path=database)
    workflow.add_step(Entry)
    workflow.add_step(After, depends_on=[Entry])
    workflow.add_step(Controller, depends_on=[Entry])
    workflow.add_cycle(
        "review",
        steps=[Entry, Controller],
        back_edge=(Controller, Entry),
        max_iterations=4,
    )

    run = workflow.loop()

    assert run.cycles["review"].status is CycleStatus.COMPLETED
    assert run.cycles["review"].iteration == 3
    assert run.output(Controller) == {"feedback": "done"}
    assert calls == [
        ("entry", 1, None),
        ("controller", 1, {"iteration": 1}),
        ("entry", 2, {"feedback": 1}),
        ("controller", 2, {"iteration": 2}),
        ("entry", 3, {"feedback": 2}),
        ("controller", 3, {"iteration": 3}),
        ("after", 1, {"iteration": 3}),
    ]
    assert [event.type for event in run.events].count("cycle.repeated") == 2
    assert [event.type for event in run.events].count("cycle.completed") == 1
    with sqlite3.connect(database) as connection:
        attempts = connection.execute(
            """
            SELECT iteration, attempt, outcome FROM step_attempts
            WHERE step_id LIKE '%Controller' ORDER BY iteration, attempt
            """
        ).fetchall()
    assert attempts == [(1, 1, "REPEAT"), (2, 1, "REPEAT"), (3, 1, "COMPLETED")]


def test_cycle_limit_is_a_persisted_workflow_failure(tmp_path):
    class Again(Step):
        def run(self, ctx):
            return self.repeat(ctx.iteration)

    workflow = Workflow("bounded-cycle", state_path=tmp_path / "state.db")
    workflow.add_step(Again)
    workflow.add_cycle(
        "bounded", steps=[Again], back_edge=(Again, Again), max_iterations=2
    )

    with pytest.raises(WorkflowFailed, match="maximum of 2") as caught:
        workflow.loop()

    assert caught.value.run.cycles["bounded"].iteration == 2
    assert caught.value.run.steps[next(iter(caught.value.run.steps))].iteration == 2
    assert [event.type for event in caught.value.run.events].count("cycle.limit_exceeded") == 1
    assert [event.type for event in caught.value.run.events].count("step.failed") == 1


def test_repeat_validation_retries_within_the_same_iteration(tmp_path):
    calls: list[tuple[int, int]] = []

    class ValidatedController(Step):
        max_attempts = 2

        def run(self, ctx):
            calls.append((ctx.iteration, ctx.attempt))
            if ctx.iteration == 1:
                return self.repeat("bad" if ctx.attempt == 1 else "accepted")
            return self.complete("done")

        def validate_output(self, output, ctx):
            return "invalid feedback" if output == "bad" else None

    workflow = Workflow("validated-cycle", state_path=tmp_path / "state.db")
    workflow.add_step(ValidatedController)
    workflow.add_cycle(
        "validated",
        steps=[ValidatedController],
        back_edge=(ValidatedController, ValidatedController),
        max_iterations=2,
    )

    assert workflow.loop().output(ValidatedController) == "done"
    assert calls == [(1, 1), (1, 2), (2, 1)]


def test_latest_output_distinguishes_json_null_from_missing(tmp_path):
    seen: list[object] = []

    class Nullable(Step):
        def run(self, ctx):
            seen.append(ctx.latest_output(Nullable, "missing"))
            if ctx.iteration == 1:
                return self.repeat(None)
            return self.complete("done")

    workflow = Workflow("nullable-cycle", state_path=tmp_path / "state.db")
    workflow.add_step(Nullable)
    workflow.add_cycle(
        "nullable", steps=[Nullable], back_edge=(Nullable, Nullable), max_iterations=2
    )

    workflow.loop()
    assert seen == ["missing", None]


def test_waiting_controller_resumes_same_attempt_then_repeats(tmp_path):
    calls: list[tuple[int, int]] = []

    class WaitingController(Step):
        def run(self, ctx):
            calls.append((ctx.iteration, ctx.attempt))
            if len(calls) == 1:
                return self.wait("approval pending")
            if ctx.iteration == 1:
                return self.repeat("approved")
            return self.complete("done")

    workflow = Workflow("waiting-cycle", state_path=tmp_path / "state.db")
    workflow.add_step(WaitingController)
    workflow.add_cycle(
        "waiting",
        steps=[WaitingController],
        back_edge=(WaitingController, WaitingController),
        max_iterations=2,
    )

    waiting = workflow.loop()
    assert waiting.status is WorkflowStatus.WAITING
    assert waiting.cycles["waiting"].iteration == 1

    completed = workflow.loop()
    assert completed.status is WorkflowStatus.COMPLETED
    assert calls == [(1, 1), (1, 1), (2, 1)]
    assert [event.type for event in completed.events].count("step.resumed") == 1


def test_only_the_declared_controller_can_repeat(tmp_path):
    class Entry(Step):
        def run(self, ctx):
            return self.repeat("invalid")

    class Controller(Step):
        def run(self, ctx):
            return self.complete()

    workflow = Workflow("illegal-repeat", state_path=tmp_path / "state.db")
    workflow.add_step(Entry)
    workflow.add_step(Controller, depends_on=[Entry])
    workflow.add_cycle(
        "loop", steps=[Entry, Controller], back_edge=(Controller, Entry), max_iterations=2
    )

    with pytest.raises(StepExecutionError, match="not a cycle controller"):
        workflow.loop()


def test_invalid_cycle_definitions_are_rejected(tmp_path):
    class A(Step):
        pass

    class B(Step):
        pass

    class C(Step):
        pass

    unreachable = Workflow("unreachable", state_path=tmp_path / "one.db")
    unreachable.add_step(A)
    unreachable.add_step(B)
    unreachable.add_cycle("bad", steps=[A, B], back_edge=(B, A), max_iterations=2)
    with pytest.raises(WorkflowDefinitionError, match="reaches every member"):
        unreachable._definition()

    overlap = Workflow("overlap", state_path=tmp_path / "two.db")
    overlap.add_step(A)
    overlap.add_step(B, depends_on=[A])
    overlap.add_step(C, depends_on=[B])
    overlap.add_cycle("first", steps=[A, B], back_edge=(B, A), max_iterations=2)
    overlap.add_cycle("second", steps=[B, C], back_edge=(C, B), max_iterations=2)
    with pytest.raises(WorkflowDefinitionError, match="overlaps"):
        overlap._definition()


def test_sqlite_store_migrates_pre_cycle_attempt_schema(tmp_path):
    from pac.state import SQLiteStateStore

    database = tmp_path / "legacy.db"
    SQLiteStateStore(database)
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            ALTER TABLE step_attempts RENAME TO step_attempts_new;
            CREATE TABLE step_attempts (
                run_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                outcome TEXT NOT NULL,
                error TEXT,
                candidate_output_json TEXT,
                rejection_reason TEXT,
                PRIMARY KEY (run_id, step_id, attempt),
                FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
            );
            DROP TABLE step_attempts_new;
            """
        )

    SQLiteStateStore(database)
    with sqlite3.connect(database) as connection:
        columns = connection.execute("PRAGMA table_info(step_attempts)").fetchall()
    assert "iteration" in {row[1] for row in columns}
    assert [row[1] for row in columns if row[5]] == ["run_id", "step_id", "iteration", "attempt"]
