from __future__ import annotations

import sqlite3

import pytest

from pac import Step, Workflow, WorkflowFailed


def test_rejected_agent_outputs_retry_until_valid(tmp_path):
    candidates = iter(["Not possible", "41", "42"])
    seen_retry_reasons: list[str | None] = []

    class Calculate(Step):
        max_attempts = 3

        def run(self, ctx):
            seen_retry_reasons.append(ctx.retry_reason)
            return self.complete(next(candidates))

        def validate_output(self, output, ctx):
            if output != "42":
                return f"Expected 42, received {output!r}"
            return None

    database = tmp_path / "state.db"
    workflow = Workflow("validated", state_path=database)
    workflow.add_step(Calculate)
    run = workflow.loop()

    assert run.output(Calculate) == "42"
    assert run.steps[next(iter(run.steps))].attempt == 3
    assert seen_retry_reasons == [None, "Expected 42, received 'Not possible'", "Expected 42, received '41'"]
    assert [e.type for e in run.events].count("step.output_rejected") == 2

    with sqlite3.connect(database) as connection:
        rejected = connection.execute(
            "SELECT candidate_output_json FROM step_attempts WHERE outcome = 'REJECTED' ORDER BY attempt"
        ).fetchall()
    assert [row[0] for row in rejected] == ['"Not possible"', '"41"']


def test_invalid_output_exhaustion_fails_workflow(tmp_path):
    calls = 0

    class NeverValid(Step):
        max_attempts = 3

        def run(self, ctx):
            nonlocal calls
            calls += 1
            return self.complete("Not possible")

        def validate_output(self, output, ctx):
            return "A numeric answer is required"

    workflow = Workflow("invalid", state_path=tmp_path / "state.db")
    workflow.add_step(NeverValid)

    with pytest.raises(WorkflowFailed) as caught:
        workflow.loop()

    assert calls == 3
    assert caught.value.run.status.value == "FAILED"
    assert [e.type for e in caught.value.run.events].count("step.output_rejected") == 3


def test_validator_exception_fails_without_retry(tmp_path):
    calls = 0

    class BrokenValidator(Step):
        max_attempts = 3

        def run(self, ctx):
            nonlocal calls
            calls += 1
            return self.complete("candidate")

        def validate_output(self, output, ctx):
            raise RuntimeError("validator bug")

    workflow = Workflow("broken-validator", state_path=tmp_path / "state.db")
    workflow.add_step(BrokenValidator)

    with pytest.raises(WorkflowFailed, match="validator bug"):
        workflow.loop()
    assert calls == 1

