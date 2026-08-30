from __future__ import annotations

import pytest

from pac import ConcurrencyError, StateStoreError, Step
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


def test_multiple_active_runs_are_isolated_and_legacy_lookup_is_ambiguous(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    first = store.create_run(_definition())
    second = store.create_run(_definition())

    assert first.id != second.id
    assert [run.id for run in store.active_runs("stored")] == [first.id, second.id]
    assert [run.id for run in store.list_runs("stored")] == [first.id, second.id]
    with pytest.raises(ConcurrencyError, match="multiple active runs"):
        store.active_run("stored")


def test_explicit_run_id_is_supported_and_unique(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    run = store.create_run(_definition(), run_id="run-123")

    assert run.id == "run-123"
    with pytest.raises(StateStoreError, match="already exists"):
        store.create_run(_definition(), run_id="run-123")
