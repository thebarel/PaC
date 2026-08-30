# PaC — Process as Code

PaC is a small durable workflow runtime for ordinary Python, AI agents, humans, external events, and timers.

```text
nondeterministic intelligence
         inside
deterministic process execution
```

PaC does **not** try to make a model deterministic. It makes the process around model calls explicit and durable: dependencies, scheduling, state transitions, validation, retries, bounded cycles, waits, worker claims, and audit events.

Agents are actors inside the process. They are not the process itself.

## Why PaC

A `Step` returns an explicit outcome. PaC persists that outcome before releasing downstream work. Candidate AI output crosses deterministic validation before it becomes durable state. A process can stop for a signal, timer, or human decision and resume after a restart. Independent DAG branches can run concurrently, while registration order remains the deterministic claim order.

The local path stays small:

```python
from pac import Step, Workflow

class Hello(Step):
    def run(self, ctx):
        return self.complete("hello")

workflow = Workflow("hello")
workflow.add_step(Hello)
run = workflow.run()
print(run.output(Hello))
```

SQLite at `.pac/state.db` is the default. Async execution, workers, PostgreSQL, encryption, agent runtimes, schemas, and exporters are opt-in.

## Installation

PaC requires Python 3.11 or newer.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Optional integrations:

```bash
pip install 'process-as-code[claude-code]' # Anthropic Claude Agent SDK adapter
pip install 'process-as-code[codex]'       # official Codex adapter
pip install 'process-as-code[postgres]'    # psycopg 3 backend
pip install 'process-as-code[encryption]'  # AES-256-GCM payload encryption
pip install 'process-as-code[otel]'        # OpenTelemetry event exporter

# Contributors working from this checkout:
pip install -e '.[dev]'
```

## Architecture

```text
                         Workflow definition
                  (steps, dependencies, cycles, schemas)
                                  |
                     deterministic scheduler
                                  |
              +-------------------+-------------------+
              |                   |                   |
         Python Step          Agent Step       Human Approval
              |                   |                   |
              |              AgentRuntime             |
              |          +--------+--------+           |
              |          |        |        |           |
              |    Claude Code  Codex   Fake / Custom   |
              +-------------------+-------------------+
                                  |
              typed codec -> deterministic validators
                                  |
                   transactional durable state
                                  |
          events / usage / signals / timers / idempotency
                                  |
             local runner or leased multi-worker execution
                                  |
                     SQLite or PostgreSQL
```

The main boundaries are deliberately separate:

- **Definition** — registered step classes, inputs, dependencies, validators, cycles, and behavioral fingerprint.
- **Scheduling** — computes an ordered runnable set from persisted state.
- **Execution** — invokes sync or async Python and acknowledges the exact claimed attempt.
- **Agent runtime** — translates provider-neutral requests/results to a provider.
- **Persistence** — commits transitions and contiguous per-run events transactionally.
- **Interaction** — durable signals, timers, approvals, cancellation, and timeouts.
- **Observability** — structured events, invocation usage, and post-commit exporters.

See [Architecture and guarantees](docs/architecture.md) for invariants and non-guarantees.

## Core API

### Steps, inputs, outputs, and dependencies

```python
from pac import Step, Workflow

class Produce(Step):
    def run(self, ctx):
        subject = ctx.input("subject")
        return self.complete({"message": f"hello {subject}"})

class Consume(Step):
    def run(self, ctx):
        produced = ctx.output(Produce)
        return self.complete(produced["message"].upper())

workflow = Workflow("minimal")
workflow.add_step(Produce, inputs={"subject": "world"})
workflow.add_step(Consume, depends_on=[Produce])
run = workflow.run()
```

Steps are registered as classes and instantiated without constructor arguments. The default identity is `module.qualname`. Untyped values must be strict JSON: no pickle, non-string object keys, NaN, infinity, or arbitrary objects.

A step returns one explicit result:

```python
return self.complete(value)
return self.retry("reason")
return self.wait("manual pause")
return self.wait(signal="payment_received")
return self.wait_until(timestamp)
return self.wait_for(duration)
return self.repeat(value, reason="another bounded cycle pass")
return self.fail("reason")
```

`ctx.input()`, `ctx.output()`, `ctx.latest_output()`, `ctx.state()`, `ctx.attempt`, `ctx.iteration`, and `ctx.retry_reason` expose explicit persisted process state.

### Typed contracts

Typed contracts are optional. Dataclasses, standard scalar/container annotations, enums, UUID/date/time values, and Pydantic v2 models (when installed) are supported.

```python
from dataclasses import dataclass
from pac import Step

@dataclass
class ResearchInput:
    topic: str

@dataclass
class ResearchOutput:
    findings: list[str]
    confidence: float

class Research(Step[ResearchInput, ResearchOutput]):
    def run(self, ctx, inputs: ResearchInput):
        return self.complete(
            ResearchOutput([f"finding about {inputs.topic}"], 0.9)
        )
```

Inputs are validated while building the definition. Outputs are validated and encoded before successful completion is persisted. Schema-free JSON workflows remain unchanged.

### AI through `AgentRuntime`

Core process logic has no provider-specific request or response type:

```python
from pac import AgentResult, FakeAgentRuntime, Step, Workflow

runtime = FakeAgentRuntime([AgentResult("candidate", provider="fake")])

class Ask(Step):
    async def run(self, ctx):
        result = await ctx.agent.execute("Summarize the evidence")
        return self.complete(result.output)

workflow = Workflow("agent-example", agent_runtime=runtime)
workflow.add_step(Ask)
run = workflow.run()
```

Implementing a custom runtime is one async method:

```python
class MyRuntime:
    async def execute(self, request, context):
        # Resolve credentials at execution time; call the provider here.
        return AgentResult(output="...", provider="my-provider", model=request.model)
```

`AgentResult` can carry provider-neutral token, cost, and latency metadata. The deterministic `FakeAgentRuntime` keeps tests independent of live APIs. `CodexRuntime` remains available, and legacy `ctx.codex.run(...)` is retained as a compatibility facade.

#### Claude Code through the Claude Agent SDK

Install the optional adapter:

```bash
pip install 'process-as-code[claude-code]'
```

Then inject it like any other runtime:

```python
from pac import AgentRequest, ClaudeCodeOptions, ClaudeCodeRuntime, Step, Workflow

runtime = ClaudeCodeRuntime(
    ClaudeCodeOptions(
        model="claude-sonnet-4-5",
        permission_mode="dontAsk",
        allowed_tools=("Read", "Grep", "Glob"),
        disallowed_tools=("Bash", "Write", "Edit"),
        max_turns=5,
    )
)

class Analyze(Step):
    async def run(self, ctx):
        result = await ctx.agent.execute(
            AgentRequest(prompt="Analyze this repository without modifying it.")
        )
        return self.complete(result.output)

workflow = Workflow("claude-process", agent_runtime=runtime)
workflow.add_step(Analyze)
run = workflow.run()
```

`ClaudeCodeRuntime` uses the Claude Agent SDK's `query()` API. Steps keep using the provider-neutral `ctx.agent.execute(...)`; there is no `ctx.claude` facade. `AgentRequest.output_schema` maps to the SDK's JSON-schema structured output, after which PaC still applies its own typed and deterministic domain validation. The adapter maps Claude usage, cost, model, duration, result ID, and session ID into provider-neutral `AgentResult` fields.

Claude session IDs are persisted separately for each workflow run and step, then reused across retries and process restarts. The SDK may also rely on session files local to the Claude Code environment, so multi-machine resume needs shared or custom SDK session storage; persisting the ID alone does not make a session portable.

PaC never selects `bypassPermissions`. SDK permission behavior remains explicit. For unattended read-only workers, a conservative starting point is `permission_mode="dontAsk"`, a narrow `allowed_tools` list, and explicit `disallowed_tools`; review the [Claude Agent SDK permission documentation](https://code.claude.com/docs/en/agent-sdk/permissions) for the exact evaluation rules. Claude tools can create external effects that PaC cannot roll back or make universally exactly once.

See the complete [Claude Code example](examples/claude_code_runtime.py).

### Deterministic validation and retries

```text
candidate output -> typed/JSON validation -> deterministic validators
                                            | accept -> persist completion
                                            ` reject -> persist reason -> retry
```

```python
class Calculate(Step):
    max_attempts = 3

    def run(self, ctx):
        return self.complete("41" if ctx.attempt == 1 else "42")

    def validate_output(self, output, ctx):
        return None if output == "42" else f"Expected 42, got {output!r}"
```

A rejected candidate and reason are recorded. The next attempt receives `ctx.retry_reason`. Exhaustion fails the step and run. `SchemaValidator`, `FunctionValidator`, and `CompositeValidator` add reusable validation while preserving `validate_output()`.

### Async and parallel DAG execution

Both sync and native async steps are supported. Sync steps execute through a thread so they do not block the async scheduler.

```python
workflow = Workflow("parallel", max_concurrency=3)
workflow.add_step(ResearchA)
workflow.add_step(ResearchB)
workflow.add_step(ResearchC)
workflow.add_step(Synthesis, depends_on=[ResearchA, ResearchB, ResearchC])
run = await workflow.arun()
```

The runnable set and claim order are deterministic by registration order. Independent claims execute concurrently; wall-clock completion order is intentionally not deterministic. `Synthesis` cannot run until every dependency commits completion.

Use `run()/resume()/loop()` from synchronous code and `arun()/aresume()/aloop()` inside an event loop.

### Explicit bounded cycles

Cycles remain declared, bounded, fingerprinted, observable, and resumable:

```python
workflow.add_step(Draft)
workflow.add_step(Review, depends_on=[Draft])
workflow.add_cycle(
    "review",
    steps=[Draft, Review],
    back_edge=(Review, Draft),  # controller -> entry
    max_iterations=5,
)
```

Only the controller may return `repeat()`. Attempts reset for a new iteration; `ctx.latest_output()` can read prior-iteration feedback. Accidental graph cycles are definition errors, and exceeding the bound fails the run.

### Explicit run identity and restart

```python
run = workflow.start()       # create only
workflow.resume(run.id)      # execute a specific persisted run
workflow.run()               # create and execute a new run
workflow.loop(run_id=run.id) # compatibility reconciliation API
```

Many active runs of the same workflow may coexist. A workflow object remembers the run it created. After process restart, implicit `loop()` resumes only when exactly one compatible active run exists; otherwise pass a run ID. An unfinished run whose behavioral fingerprint changed is refused with `WorkflowDefinitionChanged`.

If a process dies after a claim, its lease can expire and recovery consumes that attempt before retrying. Completed steps are not repeated. See [Crash recovery](examples/crash_recovery.py).

### Durable external signals

```python
class AwaitPayment(Step):
    def run(self, ctx):
        if ctx.signal_payload is None:
            return self.wait(signal="payment_received", payload_type=dict)
        return self.complete(ctx.signal_payload)

waiting = workflow.run()
workflow.signal(
    waiting.id,
    "payment_received",
    {"payment_id": "pay_123"},
    event_id="provider-event-123",
    actor={"service": "billing"},
)
completed = workflow.resume(waiting.id)
```

Signals are durable, auditable, may arrive before the wait, and are idempotent when `event_id` is supplied. Core PaC does not include an HTTP server. A webhook handler only needs to call `workflow.signal(...)`; see [External signal example](examples/external_signal.py).

### Durable timers

```python
return self.wait_for(timedelta(minutes=15))
# or
return self.wait_until(datetime(..., tzinfo=UTC))
```

Timer deadlines survive process termination. Stores expose `process_due_waits()`, `ready_runs()`, and `next_wakeup_at()` so a worker can sleep until the next deadline instead of busy polling.

### Human approval

```python
class SecurityReview(HumanApproval):
    payload_type = dict[str, str]
    timeout = timedelta(hours=24)

waiting = workflow.run()
workflow.approve(
    waiting.id,
    SecurityReview,
    payload={"ticket": "CHG-42"},
    comment="reviewed",
    actor={"id": "alice"},
    event_id="approval-42",
)
completed = workflow.resume(waiting.id)
```

`reject(...)` records the actor/reason and fails the run by default. For explicit narrow routing, declare `depends_on=[approved(SecurityReview)]`, `depends_on=[rejected(SecurityReview)]`, or `depends_on=[timed_out(SecurityReview)]`; unselected branches are persisted as `SKIPPED`. Set `route_timeout = True` on a gate that uses a timeout route. Approval and named-signal payloads are type-validated when declared. There is no built-in UI; CLIs, services, or applications call the programmatic API.

### Idempotent external actions

```python
class Charge(Step):
    max_attempts = 3

    def run(self, ctx):
        key = ctx.idempotency_key_for("charge")
        receipt = payment_api.charge(order_id="o-1", idempotency_key=key)
        return self.complete(receipt)
```

The logical key is stable across retries of the same step iteration. `ctx.attempt_idempotency_key` changes per attempt. `ctx.once("name", fn)` and `await ctx.once_async(...)` persist JSON results for local duplicate suppression.

**This is not universal exactly-once execution.** A crash can occur after an external effect but before local completion. Pass PaC's stable key to the external system's own idempotency mechanism whenever possible.

### Secrets

```python
from pac import SecretRef

workflow.add_step(CallAPI, inputs={"credential": SecretRef("STRIPE_API_KEY")})

class CallAPI(Step):
    def run(self, ctx):
        token = ctx.secrets.get(ctx.input("credential"))
        client = Client(api_key=token.reveal())
        return self.complete("called")
```

The default `EnvironmentSecretProvider` resolves references at execution time. `SecretValue` prints as `***`. Only references are persisted and fingerprinted. Implement `SecretProvider.resolve()` for AWS Secrets Manager, GCP Secret Manager, Vault, Kubernetes Secrets, or another source.

### Optional encrypted persistence

```python
from pac import AESGCMEncryptionCodec, EnvironmentEncryptionKeyProvider, SQLiteStateStore

keys = EnvironmentEncryptionKeyProvider(active_key_id="2026_08")
store = SQLiteStateStore("state.db", payload_codec=AESGCMEncryptionCodec(keys))
workflow = Workflow("secure", state_store=store)
```

Set `PAC_ENCRYPTION_KEY_2026_08` to a base64-encoded 32-byte key. Payloads use authenticated AES-256-GCM envelopes; key material is never stored beside ciphertext. Inputs, outputs, event data, signals, approvals, sessions, rejected candidates, and idempotency results are encrypted. Scheduling metadata—IDs, names, statuses, event types, timestamps, leases, and wake times—remains visible so workers can query it.

Keep old keys available while reading old rows; new writes use `active_key_id`. `reencrypt()` supports explicit rotation tooling. Encryption protects persisted payloads, not plaintext in process memory, malicious workflow code, query metadata, backups containing external keys, or secrets put into names.

### Workers and PostgreSQL

Local execution uses the same durable claim protocol as worker execution. Each claim has an owner, token, attempt, iteration, and lease. Completion is accepted only for the matching live claim.

```python
registry = WorkflowRegistry([workflow])
worker = Worker(registry, worker_id="worker-1", max_concurrency=8)
run = worker.run_sync(run_id)
# Or use await worker.run_once() / await worker.run_forever(stop=shutdown_event)
# to discover ready runs, recover leases, process timers, and heartbeat.
```

SQLite uses WAL, `BEGIN IMMEDIATE`, and conditional updates and is a strong local/default backend. `PostgreSQLStateStore` uses row locking and `FOR UPDATE SKIP LOCKED` for multi-process/multi-machine claiming. Install the `postgres` extra. Distributed claims should only be considered production-validated after running PaC's PostgreSQL integration tests against the deployment's PostgreSQL version and topology.

### Events, tracing, and usage

Every run has a transactionally ordered audit stream. Event sequence—not timestamp—is authoritative. Events cover workflow, runnable/claim/start, agent calls, validation, retries, waits, signals, timers, approvals, cycles, recovery, completion, failure, and cancellation. Full prompts and resolved secrets are omitted by default.

```python
usage = workflow.state_store.usage(run.id)
invocations = workflow.state_store.agent_invocations(run.id)

cursor = EventExportCursor(
    workflow.state_store,
    LoggingExporter(),
    name="logging",
)
cursor.export_run(run.id)
```

The cursor advances only after export succeeds. Optional `OpenTelemetryExporter` maps events to spans. Usage fields are nullable because providers do not report the same data; mixed-currency costs are not falsely summed.

### CLI

```bash
pac --db .pac/state.db runs
pac --db .pac/state.db inspect RUN_ID
pac --db .pac/state.db events RUN_ID
pac --db .pac/state.db signal RUN_ID payment_received --payload '{"id":"pay_1"}' --event-id evt_1
pac --db .pac/state.db cancel RUN_ID --reason 'operator request'
pac --db .pac/state.db workers
pac --db .pac/state.db rotate-key 2026_09
pac --db .pac/state.db validate myapp.workflows:build_workflow
pac --db .pac/state.db resume myapp.workflows:build_workflow RUN_ID
pac --db .pac/state.db retry myapp.workflows:build_workflow RUN_ID module.Step
pac --db .pac/state.db worker myapp.workflows:build_workflow RUN_ID
```

Commands that execute code require a `module:attribute` yielding a `Workflow`; persisted records are never treated as executable Python.

## Cancellation and timeouts

`workflow.cancel(run_id, reason=..., actor=...)` durably cancels pending, retrying, and waiting work and invalidates live claims. Async tasks receive cancellation/timeout behavior promptly. Sync code can call `ctx.cancelled` or `ctx.check_cancelled()` cooperatively.

PaC cannot safely force-kill arbitrary Python or undo side effects already performed. A timed-out synchronous thread may continue in memory, but its invalidated/stale claim cannot commit a later transition.

Configure workflow/step limits on `Workflow(..., workflow_timeout=..., step_timeout=...)`, an agent limit on `AgentRequest(timeout_seconds=...)`, and signal/human limits through wait/approval configuration. Wait timeout actions are explicit: fail, retry, cancel, or resume.

## Fingerprints and migrations

Behavioral fingerprints cover workflow and step versions, graph/order/dependencies, retry and timeout configuration, cycles, canonical inputs, schemas, validators, runtime binding, secret references (never values), and Python implementation identity. PaC prefers explicit versions, then normalized source, stable code-object data, and module-file fallback. It refuses unsafe unfinished-run resumes.

Fingerprinting detects covered changes; it does **not** capture every transitive dependency, external service, interpreter behavior, environment variable, model behavior, or prove reproducibility.

SQLite migrations are ordered and checksum-protected. Existing unversioned PaC databases are detected and upgraded in place. Back up durable databases before application/library upgrades. Definition, persisted-state, event, and encryption-envelope versions are separate.

See [Migration guide](docs/migration.md).

## Guarantees and non-guarantees

PaC guarantees, for a supported and correctly configured store:

- explicit persisted outcomes and per-run transactionally ordered events;
- dependency barriers and bounded declared cycles;
- deterministic runnable/claim ordering from committed state;
- typed/JSON and deterministic validator checks before completion;
- run isolation, claim-token checks, lease recovery, and durable waits;
- no resolved secret persistence through the documented secret API.

PaC does not claim:

- deterministic model output;
- universal exactly-once external side effects;
- safe forcible cancellation of arbitrary synchronous Python;
- deterministic wall-clock completion order under concurrency;
- complete reproducibility from fingerprints;
- protection of plaintext while executing in memory;
- distributed safety for untested custom backends or untested deployment environments.

## Documentation and examples

- [Workflow authoring reference](docs/workflow-authoring.md)
- [Architecture and guarantees](docs/architecture.md)
- [Operations](docs/operations.md)
- [Security](docs/security.md)
- [Migration guide](docs/migration.md)
- [`examples/`](examples/)

Requested runnable examples:

- [minimal](examples/minimal.py)
- [typed workflow](examples/typed_workflow.py)
- [agent runtime](examples/agent_runtime.py)
- [parallel research](examples/parallel_research.py)
- [human approval](examples/human_approval.py)
- [external signal](examples/external_signal.py)
- [durable timer](examples/durable_timer.py)
- [idempotent action](examples/idempotent_action.py)
- [crash recovery](examples/crash_recovery.py)
- [multi-worker execution](examples/multi_worker.py)
