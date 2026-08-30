from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4

import pytest

from pac import ConcurrencyError, PostgreSQLStateStore, Step
from pac.models import StepDefinition, WorkflowDefinition

pytestmark = pytest.mark.skipif(
    not os.environ.get("PAC_TEST_POSTGRES_DSN"),
    reason="PAC_TEST_POSTGRES_DSN is not configured",
)


class PostgreSQLStep(Step):
    pass


def _definition(name: str) -> WorkflowDefinition:
    return WorkflowDefinition(
        name=name,
        fingerprint="postgres-contract-v1",
        canonical_json="{}",
        steps=(StepDefinition(f"{name}.step", PostgreSQLStep, 0, (), 1),),
    )


def test_postgres_atomic_claim_allows_only_one_worker():
    dsn = os.environ["PAC_TEST_POSTGRES_DSN"]
    store = PostgreSQLStateStore(dsn)
    name = f"postgres-{uuid4()}"
    run = store.create_run(_definition(name))
    store.start_workflow(run.id)

    def claim(worker: str):
        try:
            return store.claim_step(
                run.id, f"{name}.step", worker, lease_duration=timedelta(seconds=30)
            )
        except ConcurrencyError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(claim, ("worker-a", "worker-b")))

    assert sum(claim is not None for claim in claims) == 1
    claim = next(claim for claim in claims if claim is not None)
    store.complete_step(run.id, f"{name}.step", "ok", claim_token=claim.token)
    assert store.get_run(run.id).steps[f"{name}.step"].output == "ok"
