# PaC Workflow Authoring Reference

This is the normative reference for people and code-generating agents that author PaC workflows.

## Mental model

A workflow is a static Python declaration reconciled against durable state:

1. Define `Step` subclasses.
2. Register the classes in intentional order.
3. Declare durable inputs, dependencies, validators, and bounded cycles.
4. Create a run or reconcile an existing run.
5. Return an explicit result from every invocation.

The model or external service may be nondeterministic. Scheduling from a committed snapshot, transition rules, validation, retry bounds, dependency barriers, and cycle bounds are deterministic.

## Minimal definition

```python
from pac import Step, Workflow

class Produce(Step):
    def run(self, ctx):
        return self.complete({"value": ctx.input("value")})

class Consume(Step):
    def run(self, ctx):
        return self.complete(ctx.output(Produce)["value"])

def build_workflow():
    workflow = Workflow("example")
    workflow.add_step(Produce, inputs={"value": 42})
    workflow.add_step(Consume, depends_on=[Produce])
    return workflow

if __name__ == "__main__":
    print(build_workflow().run().output(Consume))
```

Register classes, not instances. Steps must not require constructor arguments.

## `Workflow`

```python
Workflow(
    name,
    *,
    cwd=".",
    model=None,
    sandbox=None,
    state_path=None,
    state_store=None,
    agent_runtime=None,
    secret_provider=None,
    max_concurrency=1,
    worker_id=None,
    lease_duration=timedelta(minutes=2),
    step_timeout=None,
    workflow_timeout=None,
    codex_runtime_factory=CodexRuntime,
    version=None,
)
```

- `name` is the stable process-definition name.
- `version` is an optional explicit semantic identity included in the fingerprint.
- `state_path` selects SQLite. `state_store` injects SQLite, PostgreSQL, or a custom store; pass only one.
- `agent_runtime` injects the provider-neutral runtime used by `ctx.agent`.
- `secret_provider` defaults to environment variables.
- `max_concurrency` bounds claims executed by this runner.
- `worker_id` and `lease_duration` identify and protect claimed work.
- `step_timeout` and `workflow_timeout` are durable configuration and fingerprint inputs.
- `cwd`, `model`, `sandbox`, and `codex_runtime_factory` retain Codex compatibility.

Run APIs:

```python
created = workflow.start()             # persist only
completed = workflow.run()             # create + execute
completed = workflow.resume(run_id)    # explicit existing run
completed = workflow.loop(run_id)      # compatibility reconciliation

completed = await workflow.arun()
completed = await workflow.aresume(run_id)
completed = await workflow.aloop(run_id)
```

Use async APIs from an active event loop. Multiple active runs may share a workflow name. Implicit `loop()` after restart only works when one compatible active run is unambiguous.

## `add_step`

```python
workflow.add_step(
    StepClass,
    depends_on=[DependencyA, DependencyB],
    inputs={"name": "value"},
)
```

- Registration order is the deterministic runnable/claim tie-breaker.
- Dependencies are classes and must be registered.
- A dependent cannot run until all dependencies have committed `COMPLETED`.
- Untyped inputs must be a strict JSON object with string keys.
- Typed input may be a declared dataclass/Pydantic/annotated value and is encoded to canonical JSON.
- Inputs are snapshotted; changing them changes the definition fingerprint.
- `SecretRef` is allowed and persists only a reference marker.

Do not build registration order from an unordered set or unstable filesystem traversal.

## Typed steps

Schema-free compatibility form:

```python
class Legacy(Step):
    def run(self, ctx):
        return self.complete({"ok": True})
```

Typed form:

```python
@dataclass
class Input:
    count: int

@dataclass
class Output:
    doubled: int

class Typed(Step[Input, Output]):
    def run(self, ctx, inputs: Input):
        return self.complete(Output(inputs.count * 2))
```

PaC validates the supported run signatures during definition building. It decodes typed input before invocation, validates output before persistence, and decodes typed dependency values for `ctx.output()`. The public `WorkflowRun.output()` remains JSON-safe persisted data.

Supported codec families include primitive JSON values, dataclasses, typed collections/unions, enums, UUID/date/time values, and Pydantic v2 models when Pydantic is installed. Unsupported annotations fail explicitly. PaC never pickles workflow values.

## `StepContext`

Identity and process fields:

- `workflow_id`, `run_id`, `step_id`
- `attempt`, `iteration`, `retry_reason`
- `inputs`, `signal_payload`

State access:

- `ctx.input(name[, default])`
- `ctx.output(Dependency)` — dependency must currently be completed
- `ctx.latest_output(Step[, default])` — includes prior cycle iteration output
- `ctx.state()` — current durable snapshot

Capabilities:

- `ctx.agent` — provider-neutral runtime bound to the invocation
- `ctx.codex` — legacy Codex facade
- `ctx.secrets.get(ref)` — execution-time secret resolution
- `ctx.idempotency_key`, `ctx.attempt_idempotency_key`
- `ctx.idempotency_key_for(action, attempt_scoped=False)`
- `ctx.once(action, operation)`, `await ctx.once_async(...)`
- `ctx.cancelled`, `ctx.check_cancelled()`

A step should read data from declared inputs/dependencies rather than hidden global mutable state.

## Explicit results

```python
return self.complete(value)
return self.retry("reason")
return self.fail("reason")
return self.wait("manual pause")
return self.wait(signal="event", timeout=timedelta(hours=1), on_timeout="fail")
return self.wait_until(aware_datetime)
return self.wait_for(timedelta(minutes=5))
return self.repeat(feedback, reason="revise")
```

- `complete` validates and persists output.
- `retry` consumes the current attempt and schedules another only within `max_attempts`.
- `fail` fails the step and then the run.
- legacy `wait(reason)` is manually resumable and preserves the attempt.
- named signal/timer waits are condition-specific and durable.
- `repeat` is legal only for the controller of an explicit cycle.

Unexpected exceptions fail the invocation; they are not silently converted into unbounded retry loops.

## Validation

Validation is outside the model by default:

```python
class Reviewed(Step):
    max_attempts = 3

    def validate_output(self, output, ctx):
        if output.get("score", 0) < 0.8:
            return "score must be at least 0.8"
        return None
```

Return `None` to accept or a non-empty reason to reject. Rejection persists the candidate/reason and consumes an attempt. Validator exceptions fail immediately. Validators must be deterministic.

Reusable validators:

```python
class Reviewed(Step[Input, Output]):
    validators = (
        CompositeValidator([
            SchemaValidator(Output),
            FunctionValidator(check_business_rule),
        ]),
    )
```

Typed/JSON encoding occurs before successful completion; validators then decide domain acceptance. Validator implementation/configuration contributes to the fingerprint.

## Agent runtimes

Use `ctx.agent` in new workflows:

```python
class Ask(Step):
    async def run(self, ctx):
        result = await ctx.agent.execute(
            AgentRequest(
                prompt="Return a concise answer",
                model="provider-model",
                timeout_seconds=30,
            )
        )
        return self.complete(result.output)
```

An `AgentRuntime` implements:

```python
async def execute(request: AgentRequest, context: AgentExecutionContext) -> AgentResult
```

Provider adapters may manage sessions internally through persisted runtime-session facilities. Do not expose provider SDK objects as process state. `AgentResult.raw` is non-persisted compatibility/debug data.

Tests should use `FakeAgentRuntime`; never rely on a live model. The Codex adapter retains `ctx.codex.run(prompt, output_schema=...)`, per-step thread reuse, and workflow-level Codex configuration.

## Parallel scheduling and claims

PaC computes a registration-ordered runnable tuple. A local runner claims at most `max_concurrency` and executes those claims concurrently. Sync functions run in threads; async functions are awaited.

Claiming transactionally persists owner, token, attempt, iteration, timestamps, and lease expiry before user code. Every completion/retry/wait/failure acknowledgement must present that token. Expired, cancelled, or old-iteration results cannot mutate state.

Do not infer deterministic completion order. Only runnable/claim order and committed dependency behavior are deterministic.

## Signals

A signal-aware step must inspect `ctx.signal_payload`:

```python
class WaitForCustomer(Step):
    def run(self, ctx):
        if ctx.signal_payload is None:
            return self.wait(signal="customer_response", payload_type=dict)
        return self.complete(ctx.signal_payload)
```

Submit externally:

```python
workflow.signal(
    run_id,
    "customer_response",
    payload={"answer": "yes"},
    event_id="crm-event-17",
    actor={"service": "crm"},
)
```

Signals persist before or after a waiter exists. `event_id` makes receipt idempotent per run/signal. Actor metadata is audit data, not authentication—authenticate in the surrounding application.

## Timers

Use timezone-aware timestamps. A timer remains waiting until `process_due_waits()` observes its deadline. Operational workers should use `next_wakeup_at()` to choose their sleep and `ready_runs()` after processing deadlines. Do not implement a tight `loop()` poll.

## Human approval

```python
class Approval(HumanApproval):
    payload_type = dict[str, str]
    timeout = timedelta(hours=8)
    timeout_action = TimeoutAction.FAIL
```

PaC creates a durable human task rather than invoking arbitrary approval code. Submit with `approve()` or `reject()`, including actor, comment/reason, payload, and event ID. Approval resumes and completes the gate; rejection fails by default. Declare narrow deterministic routes with `depends_on=[approved(Gate)]`, `depends_on=[rejected(Gate)]`, or `depends_on=[timed_out(Gate)]`; unselected routes become `SKIPPED`. A timeout route requires `Gate.route_timeout = True`. PaC provides no identity provider or UI.

## Idempotency and effects

Logical keys include run, step, cycle iteration, and action; they remain stable across retries. Attempt keys additionally include attempt.

`ctx.once()` stores a JSON result and prevents concurrent duplicate local execution. It cannot close the crash window between an external effect and local persistence. For payments, email, provisioning, or another remote side effect, send `ctx.idempotency_key_for(action)` to the provider's idempotency API.

## Cancellation and timeout

Cancellation is persisted and prevents future claims/acknowledgements. Async work can stop promptly. Sync work must cooperate with `ctx.check_cancelled()`; a thread may continue but cannot commit with an invalidated claim.

A timed-out external operation might already have caused an effect. Combine timeouts with external idempotency and reconciliation.

Wait timeout actions are `FAIL`, `RETRY`, `CANCEL`, and `RESUME`. Human approvals use the same timeout action model.

## Cycles

```python
workflow.add_cycle(
    "review",
    steps=[Draft, Review],
    back_edge=(Review, Draft),
    max_iterations=5,
)
```

The normal dependency graph must omit the back edge and remain acyclic. Members cannot overlap cycles. The entry must reach all members and all members must reach the controller. The controller is the only step allowed to `repeat`. The bound includes the first pass.

Cycles work with validation retries, persisted waits, restarts, and forward parallel branches while preserving iteration-specific attempts and idempotency keys.

## Secrets and encryption

Use `SecretRef` in definition data and resolve through `ctx.secrets`. Do not put secret values in inputs, outputs, prompts, error strings, event actors, workflow/step names, or idempotency action names.

Optional `AESGCMEncryptionCodec` encrypts opaque payload columns using an external key provider. Query-critical scheduling metadata remains plaintext. Rotation requires keeping old key IDs available until old ciphertext has been explicitly rewritten.

## Inspection and observability

`WorkflowRun` contains run status, step snapshots, completed outputs, cycles, and ordered events. `WorkflowFailed.run` carries the failed snapshot.

Use store methods for usage and invocation detail. Use `EventExportCursor` for post-commit export; exporter failure must not alter workflow state. Event data is sanitized, but authors still must avoid putting secrets in names and error text.

## Behavioral fingerprints

The fingerprint includes structure, registration order, dependencies, retries, timeouts, cycles, schemas, validator behavior/configuration, runtime binding, canonical inputs, secret references, workflow/step versions, and implementation identity.

Set explicit `Workflow(version=...)` and `Step.version` for intentionally versioned/dynamic code. Explicit step version becomes the implementation identity; change it when behavior changes. Fingerprints are compatibility guards, not full reproducibility proofs.

## Definition checklist

Before running a generated workflow:

- [ ] Every registered item is a `Step` class with no required constructor arguments.
- [ ] Registration order is explicit and stable.
- [ ] Every `ctx.output(X)` has a matching `depends_on=[X]` path.
- [ ] Inputs and outputs are strict JSON or supported typed models.
- [ ] AI candidates have deterministic validation for required semantics.
- [ ] `max_attempts`, timeouts, and cycles are bounded.
- [ ] Cycles are declared with one controller/back edge and a finite maximum.
- [ ] Async code awaits `ctx.agent.execute()`/`ctx.once_async()`.
- [ ] Signals and approvals have authenticated surrounding transports and event IDs.
- [ ] External effects receive PaC idempotency keys where supported.
- [ ] Secrets use `SecretRef` and are resolved only at execution.
- [ ] Dynamic/decorated code has explicit semantic versions when source identity is unclear.
- [ ] Cancellation and timeout limitations are acceptable for each side effect.
- [ ] Worker deployments use a shared supported store and tested lease settings.
