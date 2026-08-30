# Migration Guide

## From the original Codex-specific API

Existing simple workflows continue to work:

```python
workflow = Workflow("name", cwd=".", model="...", sandbox=...)
workflow.add_step(MyStep, depends_on=[OtherStep], inputs={"x": 1})
run = workflow.loop()
```

Legacy `ctx.codex.run(...)`, result properties `text`, `thread_id`, `turn_id`, and the constructor's Codex options remain compatibility paths.

For new code, inject an `AgentRuntime` and use `ctx.agent`:

```python
workflow = Workflow("name", agent_runtime=my_runtime)

class MyStep(Step):
    async def run(self, ctx):
        result = await ctx.agent.execute(AgentRequest(prompt="..."))
        return self.complete(result.output)
```

Install the Codex integration through the `codex` extra when packaging no longer brings it transitively:

```bash
pip install 'process-as-code[codex]'
```

## Explicit runs

The original runtime implicitly selected the latest active run by workflow name and prevented concurrent same-name execution. PaC now separates definition identity from execution identity.

Preferred service code:

```python
created = workflow.start()
# persist/return created.id
result = workflow.resume(created.id)
```

`workflow.run()` creates and executes a fresh run. `loop()` preserves convenient behavior for the workflow object's remembered run and a sole unambiguous active run. When several active runs share a name, pass a run ID; PaC refuses ambiguous selection.

Calling `loop()` after a terminal run still creates another run.

## Async and concurrency

Synchronous `Step.run(ctx)` remains supported. Native async steps may declare `async def run(...)`. Use `await workflow.arun()` or `await workflow.aresume(id)` inside an event loop.

The default `max_concurrency=1` preserves sequential execution. Increasing it allows independent branches to overlap. Do not depend on their wall-clock completion order; registration order remains the claim tie-breaker.

## Typed steps

No schema is required for legacy JSON workflows. Add generic types incrementally:

```python
class Build(Step[BuildInput, BuildOutput]):
    def run(self, ctx, inputs: BuildInput):
        ...
```

Typed steps receive a second `inputs` argument. Existing `run(ctx)` remains valid. Persisted values stay canonical JSON, so adding a type contract to an unfinished workflow changes its fingerprint and requires a new run or an explicit migration strategy.

## Wait behavior

Legacy `self.wait("reason")` remains a manually resumed pause and preserves its attempt. New condition-specific waits do not wake merely because `loop()` was called:

```python
self.wait(signal="customer_response")
self.wait_until(timestamp)
self.wait_for(duration)
```

Submit signals and process due timers before explicitly resuming the run. Use event IDs for webhook idempotency.

## Human approval

Use `HumanApproval` for durable gates rather than ad hoc generic waits. Submit decisions through `workflow.approve()`/`reject()`. Rejection fails the run by default. Existing workflows that encoded alternate rejection branches manually can keep doing so; a general branch-expression DSL was intentionally not introduced.

## State database migrations

Opening an existing SQLite database installs `pac_schema_migrations`, recognizes the legacy schema, and applies ordered forward migrations. Existing step inputs and cycle-attempt history are retained. Migration checksums detect altered history.

Before upgrade:

1. stop writers;
2. back up the database and WAL consistently;
3. test the upgrade against a copy;
4. deploy code and open the database once;
5. verify runs/events and then restart workers.

There is no automatic downgrade. Restore the backup when rolling back across incompatible schema changes.

## Fingerprint changes

Fingerprints now include Python implementation identity, schemas, validators, retries/timeouts, runtime binding, cycles, relevant configuration, and versions. An unfinished legacy run may therefore refuse to resume under changed code even when its old structural fingerprint matched.

This is fail-safe behavior. Options are:

- deploy the compatible code and finish/cancel the old run;
- start a new run under the new definition;
- give dynamic code explicit `Step.version` and `Workflow(version=...)` before creating durable runs.

Never reuse an explicit version across behavioral changes merely to bypass the guard.

## Secrets and encryption

Replace plaintext credentials in inputs with `SecretRef`. Existing plaintext rows are not retroactively removed from backups or old events.

Enabling `AESGCMEncryptionCodec` reads legacy plaintext and encrypts new writes. It does not automatically rewrite every old row. Keep key management external and plan an explicit re-encryption operation if old rows must be encrypted.

## Workers and PostgreSQL

Local `workflow.run()` remains the default. Worker deployments must load trusted executable definitions into `WorkflowRegistry` and resume explicit run IDs. Claims and leases replace the old workflow-name file lock.

When moving SQLite state to PostgreSQL, perform an application-controlled migration with writers stopped and validate row counts, definition fingerprints, event sequences, attempts, waits, and claims. The package does not currently offer a cross-database copy command.
