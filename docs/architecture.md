# Architecture and Guarantees

## Design goal

PaC places nondeterministic actors inside deterministic, persisted process execution. It is a process runtime, not an agent framework.

```text
Definition -> Scheduler -> Claim -> Executor -> Validation -> Transition
     |           |          |         |             |            |
 fingerprint  runnable set  lease   Python/agent   accept/reject  state+event
```

## Components

### Process definition

`Workflow` stores registered step classes, canonical durable inputs, dependencies, cycles, runtime configuration, schemas, validators, and explicit versions. Building a definition validates graph structure and computes a behavioral fingerprint.

### Scheduler

The scheduler is a pure decision over definition plus persisted state. It returns runnable step IDs in registration order. Dependencies and cycle gating are checked against committed state, never in-memory task completion.

### Execution

The async runner claims a bounded prefix of the runnable set. Native async methods are awaited; sync methods run through a thread. Each execution receives a `StepContext` with explicit capabilities rather than the store itself.

### Agent runtime

`AgentRuntime` is an async provider-neutral protocol over `AgentRequest`, `AgentExecutionContext`, and `AgentResult`. Provider adapters translate these types and can report nullable usage. Core scheduling never branches on provider identity.

### Persistence

A `StateStore` owns atomic transitions, claims, attempts, waits, signals, humans, cycles, idempotency records, sessions, usage, workers, and events. SQLite is local-first. PostgreSQL adds row locking and skip-locked claiming.

### Signals and timers

Waits are persisted conditions. Signals form a durable mailbox and may precede the waiter. Timer deadlines are indexed and queried through ready-run/next-wakeup APIs. PaC intentionally does not embed an HTTP server or daemon supervisor.

### Observability

Every state transition emits an event in the same transaction. Sequence numbers are contiguous per run and define order. Provider prompts and secret values are excluded by default. Exporters operate after commit through durable cursors.

## Critical invariants

1. A claimed attempt is persisted before user code executes.
2. Claim identity includes run, step, attempt, iteration, owner, token, and lease.
3. Result acknowledgement requires the live token; stale results cannot commit.
4. A transition and its event share one transaction.
5. Dependencies become runnable only after committed completion.
6. Runnable/claim order is registration order.
7. Concurrent completion order is not deterministic and is not used as a dependency signal.
8. A retry consumes an attempt. An interrupted or expired claim also consumes its attempt.
9. A wait resumes the same logical attempt unless timeout policy requests retry.
10. Cycle repeat increments iteration and resets member attempts; old-iteration results are invalid.
11. Signals with an event ID are idempotent and consumed at most once by a matching wait.
12. Multiple runs with the same workflow name remain isolated by run ID.
13. Event sequence, not timestamp, is the durable ordering key.
14. Query-critical scheduling metadata stays readable even when payload encryption is enabled.

## Local execution

`Workflow.run()` is a synchronous facade over the async reconciliation engine. With SQLite and `max_concurrency=1`, it is the simplest mode. Increasing concurrency allows independent branches to overlap while using the same claim protocol.

## Worker execution

`WorkflowRegistry` maps persisted names to executable Python definitions. Persisted metadata is never executed. `Worker` sets a stable worker ID, concurrency, and lease duration, then resumes an explicit run.

SQLite serializes writes with `BEGIN IMMEDIATE` and uses conditional updates. This is suitable for local and modest same-filesystem concurrency. PostgreSQL uses transactions, locked event-sequence mutation, and `FOR UPDATE SKIP LOCKED` for distributed claiming.

A worker deployment still needs surrounding process supervision. `Worker.run_once()` discovers ready runs and processes expired leases/timers; `run_forever()` heartbeats and sleeps until the next persisted deadline or its bounded idle interval, and accepts a stop event for notification-driven wakeups. It is a focused Python worker, not a hosted queue service.

## Failure behavior

The default is fail-fast for a committed unrecoverable step failure: no downstream claims become runnable. Other already-running branches may have performed work. Their stale acknowledgements are rejected after cancellation/terminal transition where applicable.

A process crash while a claim is live leaves it recoverable after lease expiry. The attempt is marked interrupted, then retried only if quota remains.

## Guarantees

For tested store implementations and documented APIs, PaC provides:

- durable explicit state and outcomes;
- transactional transition/event coupling;
- deterministic dependency and runnable ordering;
- strict claim-token and run isolation;
- bounded retries and cycles;
- validation before successful output persistence;
- durable signals, timers, approvals, cancellation, and audit events;
- source/configuration compatibility checks for unfinished-run resume.

## Non-guarantees

- Models and external services remain nondeterministic.
- Exactly-once arbitrary side effects are impossible without cooperation from the external system.
- Arbitrary synchronous Python cannot be safely force-terminated.
- Parallel wall-clock completion order is not deterministic.
- Fingerprints do not capture every dependency, environment fact, or remote system.
- Encryption does not protect plaintext during active execution or scheduling metadata.
- A custom `StateStore` is not distributed-safe merely because it implements the interface.
- PostgreSQL correctness claims apply to implemented transaction paths and should be verified by integration tests against the deployment environment.
