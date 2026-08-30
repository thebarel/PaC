# Operating PaC

## Run identity

Workflow name identifies a process definition; run ID identifies one execution. Several active run IDs may use the same name. Prefer explicit IDs in services and operations:

```python
run = workflow.start()
workflow.resume(run.id)
```

`loop()` retains compatibility for a workflow object's remembered run and for a sole unambiguous active run after restart.

## SQLite

SQLite is the default and needs no service:

```python
workflow = Workflow("orders", state_path="var/orders.db")
```

It enables WAL and transactionally serializes writes. Put the database on reliable local storage, back it up, and avoid filesystems whose locking semantics do not satisfy SQLite requirements. Query-critical indexes cover active runs, leases, and waits.

## PostgreSQL

```python
from pac import PostgreSQLStateStore, Workflow

store = PostgreSQLStateStore("postgresql://user:pass@db/pac")
workflow = Workflow("orders", state_store=store)
```

Install `process-as-code[postgres]`. PostgreSQL uses row locks for event sequencing and `FOR UPDATE SKIP LOCKED` for claims. Before production use, run the DSN-gated tests against the target service:

```bash
PAC_TEST_POSTGRES_DSN='postgresql://...' pytest tests/test_postgres_state.py
```

This repository does not claim that an arbitrary PostgreSQL proxy, connection pooler mode, replica topology, or custom backend preserves these semantics without testing.

## Worker claims and leases

```python
registry = WorkflowRegistry([workflow])
worker = Worker(
    registry,
    worker_id="worker-eu-1",
    max_concurrency=8,
    lease_duration=timedelta(minutes=2),
)
worker.run_sync(run_id)
# Or discover ready runs and wait on durable deadlines:
# asyncio.run(worker.run_forever(stop=shutdown_event))
```

Choose a lease longer than normal heartbeat/transition delays but short enough for useful recovery. Expiry consumes the current attempt. Configure enough `max_attempts` for crash recovery where safe. `recover_expired_claims()` returns recovered claims.

Long-running user code must be designed with lease/heartbeat and idempotency in mind. A lease prevents stale state commits; it does not undo an external side effect.

## Timers and wakeups

A service loop should:

1. process externally triggered signals immediately;
2. call `process_due_waits()` at or after `next_wakeup_at()`;
3. select `ready_runs()`;
4. dispatch explicit run IDs to registered workflows;
5. sleep or block until notification/the next deadline.

Do not repeatedly call `workflow.loop()` in a tight poll.

## CLI

The CLI defaults to `.pac/state.db`:

```bash
pac --db path/state.db runs --workflow orders
pac --db path/state.db inspect RUN_ID
pac --db path/state.db events RUN_ID
pac --db path/state.db workers
pac --db path/state.db rotate-key 2026_09
pac --db path/state.db signal RUN_ID payment_received --payload '{"id":"p1"}' --event-id p1
pac --db path/state.db cancel RUN_ID --reason 'operator request'
```

Commands that execute workflow Python require `module:attribute`:

```bash
pac --db path/state.db validate app.workflows:orders
pac --db path/state.db resume app.workflows:orders RUN_ID
pac --db path/state.db retry app.workflows:orders RUN_ID app.steps.Send
pac --db path/state.db worker app.workflows:orders RUN_ID
```

The attribute may be a `Workflow` or a zero-argument factory. Treat module loading as code execution and only load trusted application code.

## Observability

Persisted events are always available on `WorkflowRun.events` and through `pac events`. Agent invocation and usage queries:

```python
store.agent_invocations(run_id)
store.usage(run_id)
```

Export after commit:

```python
cursor = EventExportCursor(store, LoggingExporter(), name="logs")
while cursor.export_run(run_id, limit=100):
    pass
```

A failed exporter leaves its cursor unchanged. Re-export can therefore duplicate delivery if a sink accepted a batch but the cursor update failed; sinks should deduplicate by `(run_id, sequence)`.

## Cancellation and timeout operations

Cancellation is cooperative for executing Python and durable for scheduler state. It immediately prevents new claims and invalidates relevant work, but arbitrary sync code or external requests may still run outside PaC's control.

Timeouts have the same external-effect caveat. Use stable idempotency keys and provider reconciliation for operations that may outlive a local timeout.

## Migrations and backups

SQLite initializes ordered checksum-protected migrations automatically and recognizes legacy unversioned schemas. Back up databases before upgrading. Never edit applied migration identities/checksums in place.

For PostgreSQL, test migrations and concurrency in a staging database matching production. PaC separates schema, definition, event, state, and encryption-envelope versions; application workflow versions remain the author's responsibility.
