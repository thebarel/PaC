from __future__ import annotations

import sqlite3

import pytest

from pac import (
    Step,
    Workflow,
    WorkflowDefinitionChanged,
    WorkflowDefinitionError,
    WorkflowFailed,
)
from pac.state import SQLiteStateStore


def test_step_receives_declared_inputs_and_defaults(tmp_path):
    source = {
        "company": "Acme",
        "options": {"regions": ["us", "eu"]},
    }

    class InspectInputs(Step):
        def run(self, ctx):
            assert ctx.input("company") == "Acme"
            assert ctx.input("missing", "fallback") == "fallback"
            assert ctx.inputs["options"] == {"regions": ["us", "eu"]}
            return self.complete(dict(ctx.inputs))

    workflow = Workflow("inputs", state_path=tmp_path / "state.db")
    workflow.add_step(InspectInputs, inputs=source)

    source["company"] = "Mutated after registration"
    source["options"]["regions"].append("apac")

    run = workflow.loop()
    assert run.output(InspectInputs) == {
        "company": "Acme",
        "options": {"regions": ["us", "eu"]},
    }
    state = run.steps[next(iter(run.steps))]
    assert state.inputs == run.output(InspectInputs)
    registered = next(event for event in run.events if event.type == "step.registered")
    assert registered.data["inputs"] == state.inputs


def test_missing_required_input_has_a_clear_error(tmp_path):
    class NeedsCompany(Step):
        def run(self, ctx):
            ctx.input("company")
            return self.complete()

    workflow = Workflow("missing-input", state_path=tmp_path / "state.db")
    workflow.add_step(NeedsCompany)

    with pytest.raises(WorkflowFailed, match="available inputs: none"):
        workflow.loop()


@pytest.mark.parametrize(
    "inputs",
    [
        {"bad": object()},
        {"bad": float("nan")},
        {1: "non-string key"},
        {"nested": {1: "non-string key"}},
    ],
)
def test_inputs_must_be_strict_json(tmp_path, inputs):
    class UsesInputs(Step):
        pass

    workflow = Workflow("bad-input", state_path=tmp_path / "state.db")
    with pytest.raises(WorkflowDefinitionError, match="Inputs"):
        workflow.add_step(UsesInputs, inputs=inputs)


def test_input_change_rejects_resume(tmp_path):
    class WaitingForInput(Step):
        def run(self, ctx):
            return self.wait(f"paused for {ctx.input('company')}")

    database = tmp_path / "state.db"
    original = Workflow("input-change", state_path=database)
    original.add_step(WaitingForInput, inputs={"company": "Acme"})
    unfinished = original.loop()

    changed = Workflow("input-change", state_path=database)
    changed.add_step(WaitingForInput, inputs={"company": "Other"})

    with pytest.raises(WorkflowDefinitionChanged):
        changed.loop()
    assert changed.state_store.get_run(unfinished.id).status.value == "WAITING"


def test_sqlite_store_migrates_pre_input_schema(tmp_path):
    database = tmp_path / "state.db"
    SQLiteStateStore(database)
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE step_runs DROP COLUMN inputs_json")

    SQLiteStateStore(database)
    with sqlite3.connect(database) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(step_runs)").fetchall()
        }
    assert "inputs_json" in columns
