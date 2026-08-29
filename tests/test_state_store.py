from __future__ import annotations

import pytest

from pac import StateStoreError, Step, WorkflowExecutionError
from pac.models import StepDefinition, WorkflowDefinition
from pac.state import SQLiteStateStore


class StoredStep(Step):
    pass


def _definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        name="stored",
        fingerprint="fingerprint",
        canonical_json="{}",
        steps=(StepDefinition("stored.Step", StoredStep, 0, (), 1),),
    )


def test_invalid_persisted_transition_is_rejected(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    run = store.create_run(_definition())

    with pytest.raises(StateStoreError, match="not RUNNING"):
        store.complete_step(run.id, "stored.Step", "invalid")


def test_concurrent_workflow_execution_lock_is_rejected(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")

    with (
        store.execution_lock("same-workflow"),
        pytest.raises(WorkflowExecutionError, match="already being executed"),
        store.execution_lock("same-workflow"),
    ):
        pass
