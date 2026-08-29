# PaC Workflow Authoring Reference

This is the normative guide for humans and agents that generate PaC workflows. A generated workflow should follow every rule in the final checklist.

## Mental model

A PaC program declares a static workflow in Python:

1. Define `Step` subclasses.
2. Register the Step classes on one `Workflow` in deterministic order.
3. Declare step inputs and dependencies during registration.
4. Call `workflow.loop()`.

PaC reconciles that declaration with persisted SQLite state. A completed step is not executed again in the same run. Model text may vary, but PaC deterministically chooses scheduling, transitions, retries, persistence, recovery, and acceptance decisions.

## Imports and minimal program

```python
from pac import Step, Workflow


class Produce(Step):
    def run(self, ctx):
        subject = ctx.input("subject")
        return self.complete({"subject": subject, "value": "result"})


class Consume(Step):
    def run(self, ctx):
        produced = ctx.output(Produce)
        return self.complete(produced["value"])


def main():
    workflow = Workflow("example", cwd=".")
    workflow.add_step(Produce, inputs={"subject": "Acme"})
    workflow.add_step(Consume, depends_on=[Produce])

    run = workflow.loop()
    print(run.id)
    print(run.status)
    print(run.output(Consume))


if __name__ == "__main__":
    main()
```

Always register the class (`Produce`), not an instance (`Produce()`). PaC owns instantiation, so steps should not require constructor arguments.

## `Workflow`

Constructor:

```python
Workflow(
    name,
    *,
    cwd=".",
    model=None,
    sandbox=None,
    state_path=None,
    state_store=None,
    codex_runtime_factory=CodexRuntime,
)
```

- `name`: non-empty stable workflow name. It identifies runs in the state database.
- `cwd`: repository directory used by Codex. The default state path is `<cwd>/.pac/state.db`.
- `model`: optional Codex model passed to every step thread.
- `sandbox`: optional `openai_codex.Sandbox` preset.
- `state_path`: optional SQLite path.
- `state_store`: optional `StateStore`; do not pass it together with `state_path`.
- `codex_runtime_factory`: test seam for a fake Codex runtime. Normal workflows should omit it.

`loop()` resumes the latest unfinished run of the same name. If no unfinished run exists, it creates a new run. A call after completion or failure creates a new run.

Explicit cycles are registered after their member steps:

```python
workflow.add_cycle(
    "review",
    steps=[Draft, Review],
    back_edge=(Review, Draft),  # (Controller, Entry)
    max_iterations=10,
)
```

The ordinary dependencies must remain acyclic and omit the feedback edge. Members cannot overlap cycles. The entry must reach every member through forward dependencies, every member must reach the controller, and `max_iterations` is required and includes the first pass.

## `add_step`

Signature:

```python
workflow.add_step(
    StepClass,
    depends_on=[DependencyA, DependencyB],
    inputs={"name": "value"},
)
```

Both keyword arguments are optional.

### Registration order

Registration order is the only tie-breaker when multiple steps are runnable. Do not generate registrations from an unordered set or filesystem listing. Register them explicitly in intended order.

### Dependencies

`depends_on` contains Step classes, not strings or instances. Every dependency must also be registered. Dependency list order does not affect scheduling; PaC normalizes it by registration order.

A dependency must be `COMPLETED` before the dependent step runs. Declare a dependency whenever a step calls `ctx.output(Dependency)`.

### Inputs

`inputs` is a mapping with string keys and JSON-compatible values:

```python
workflow.add_step(
    Scan,
    inputs={
        "company": "Acme",
        "limit": 100,
        "enabled": True,
        "tags": ["external", "dns"],
        "options": {"timeout": 30},
        "optional_value": None,
    },
)
```

Allowed values are strings, integers, finite floats, booleans, null/`None`, lists of allowed values, and objects with string keys and allowed values. Unsupported objects, NaN, and infinity raise `WorkflowDefinitionError`.

PaC canonicalizes and snapshots inputs when `add_step()` is called. Mutating the original Python dictionary afterward does not change the registered inputs. Inputs are persisted and included in the workflow fingerprint. Changing them during an unfinished run raises `WorkflowDefinitionChanged`.

Inputs are stored in plaintext SQLite state and registration events. Do not use step inputs for secrets.

## `StepContext`

Every `run()` and `validate_output()` receives a context with:

- `ctx.workflow_id`: workflow name.
- `ctx.run_id`: stable workflow-run ID.
- `ctx.step_id`: stable `module + qualname` step identity.
- `ctx.attempt`: logical attempt number beginning at 1.
- `ctx.iteration`: cycle iteration beginning at 1; always 1 outside a cycle.
- `ctx.retry_reason`: previous explicit retry, validation rejection, or crash-recovery reason; otherwise `None`.
- `ctx.inputs`: read-only mapping of this step's declared inputs.
- `ctx.input(name)`: required input lookup. Missing input raises `KeyError` listing available keys.
- `ctx.input(name, default)`: optional input lookup.
- `ctx.output(StepClass)`: persisted output of a registered completed step.
- `ctx.latest_output(StepClass, default)`: latest persisted output even after a cycle reset; without a default, missing output raises `ValueError`.
- `ctx.state()`: current immutable-style `WorkflowRun` snapshot.
- `ctx.codex`: the Codex facade bound to this step's private thread.

Prefer required lookups for required configuration:

```python
company = ctx.input("company")
timeout = ctx.input("timeout_seconds", 30)
```

Do not use module globals or environment-dependent values when the value belongs in the durable workflow definition. Put such configuration in `inputs` so resume detects changes.

## Step results

`Step.run()` must return exactly one `StepResult` using a helper:

```python
return self.complete(json_value)
return self.retry("why another attempt is needed")
return self.wait("why external progress is required")
return self.fail("why the workflow cannot continue")
return self.repeat(json_value, reason="why another cycle pass is needed")
```

Returning `None`, a raw string, or an `AgentResult` directly is invalid. Complete with the desired JSON-compatible value, commonly `result.text` or a parsed JSON object.

Only the controller of an explicitly declared cycle may return `repeat()`. Repeat values use the same strict JSON serialization and deterministic `validate_output()` checks as completed values. Rejection retries within the current iteration. The controller exits its cycle by returning `complete()`.

`max_attempts` includes the first execution:

```python
class Scan(Step):
    max_attempts = 3
```

Unexpected exceptions fail the step immediately. They are not automatically retried. Catch only errors that the step can classify and intentionally return `retry()` when appropriate.

## Codex calls

Each step owns one persisted Codex thread within one workflow run. Calls and retries in that step reuse the thread; other steps get separate threads.

```python
result = ctx.codex.run("Analyze this repository.")

text = result.text
thread_id = result.thread_id
turn_id = result.turn_id
raw_sdk_result = result.raw
```

For structured generation, pass the Codex JSON Schema directly:

```python
schema = {
    "type": "object",
    "properties": {
        "company": {"type": "string"},
        "domains": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["company", "domains"],
    "additionalProperties": False,
}

result = ctx.codex.run(
    f"Find the root domains for {ctx.input('company')}.",
    output_schema=schema,
)
```

The PaC result text is still the final response. Parse JSON explicitly and retry malformed responses:

```python
import json

try:
    data = json.loads(result.text)
except (json.JSONDecodeError, TypeError) as exc:
    return self.retry(f"Codex did not return JSON: {exc}")

return self.complete(data)
```

Do not complete with `AgentResult` itself because it contains a raw SDK object that is not JSON-serializable.

## Deterministic output validation

Codex completing a turn does not mean its answer satisfies the workflow. Override `validate_output()` for semantic acceptance:

```python
class Calculate(Step):
    max_attempts = 3

    def run(self, ctx):
        feedback = (
            f" Previous output was rejected: {ctx.retry_reason}"
            if ctx.retry_reason
            else ""
        )
        result = ctx.codex.run(f"Calculate 6 * 7. Return only the integer.{feedback}")
        return self.complete(result.text)

    def validate_output(self, output, ctx):
        if output != "42":
            return f"Expected '42', received {output!r}"
        return None
```

Validation runs after strict JSON serialization and before completion is persisted. Return `None` to accept. Return a non-empty reason string to reject. PaC persists the rejected candidate and reason, then retries if attempts remain. Exhaustion fails the workflow.

Validators must be deterministic Python logic. Do not call Codex from a validator or use another probabilistic model as the final acceptance authority. Use exact comparisons, parsing, schemas, ranges, checksums, database constraints, or other reproducible rules.

## Passing results between steps

Return structured JSON from producers rather than JSON strings when practical:

```python
class RootDomain(Step):
    def run(self, ctx):
        company = ctx.input("company")
        return self.complete({"company": company, "domains": ["a.com", "aa.net"]})


class CertificateLookup(Step):
    def run(self, ctx):
        discovery = ctx.output(RootDomain)
        results = {}
        for domain in discovery["domains"]:
            results[domain] = lookup(domain)
        return self.complete(results)


workflow.add_step(RootDomain, inputs={"company": "Acme"})
workflow.add_step(CertificateLookup, depends_on=[RootDomain])
```

PaC v1 has a static graph and no dynamic fan-out. When one step produces a list of domains, process that list sequentially inside a downstream step, or declare a fixed set of downstream Step classes.

## Waiting, failure, and recovery

- `wait(reason)` records `WAITING` and lets other runnable steps proceed. When none remain, `loop()` returns a `WorkflowRun` with status `WAITING` rather than spinning.
- A later explicit `loop()` call resumes waiting steps within the same logical attempt.
- `fail(reason)`, an uncaught exception, rejected output exhaustion, or retry exhaustion marks the workflow failed. `loop()` raises `WorkflowFailed`; inspect `exception.run` for persisted state.
- A process crash after `RUNNING` was persisted consumes that attempt. Resume records recovery and retries only if attempts remain.
- PaC cannot guarantee exactly-once external side effects. HTTP calls, file writes, or Codex actions may have completed before a process crash prevented PaC from recording completion.

## Outputs and run inspection

On success or waiting, `loop()` returns `WorkflowRun`:

```python
run.id
run.name
run.status
run.steps
run.cycles
run.outputs
run.events
run.output(StepClass)
```

`run.output(StepClass)` requires that the step completed. `run.steps[step_id]` includes attempt, iteration, latest-output presence/value, and the existing step metadata. `run.cycles` contains each cycle's members, controller, entry, current iteration, limit, and status.

Events are append-only and ordered by `sequence`, not timestamp. Important events include workflow creation/start/wait/completion/failure, step registration/start/completion/retry/wait/failure/recovery/output rejection, and Codex thread/turn events.

## Definition and persistence rules

The default store is `<cwd>/.pac/state.db`. PaC fingerprints:

- workflow name
- resolved `cwd`
- model and sandbox
- ordered stable step IDs
- dependencies
- non-empty step inputs
- `max_attempts`
- cycle members, feedback endpoints, and iteration limits

Python source code is not fingerprinted. Changing a Step implementation without changing declarative configuration is not detected in v1.

## Errors authors should expect

- `WorkflowDefinitionError`: invalid name, step, dependency, retry limit, configuration, or inputs.
- `WorkflowCycleError`: raw `depends_on` cycle; use `add_cycle()` for one feedback edge.
- `WorkflowDefinitionChanged`: current declaration differs from an unfinished run.
- `WorkflowFailed`: persisted execution failure; inspect `.run`.
- `WorkflowDeadlockError`: valid execution cannot make progress.
- `StepExecutionError`: invalid step return or validator contract.
- `StepOutputSerializationError`: completed output is not strict JSON.
- `StateStoreError`: persistence or transition failure.

Catch errors at the workflow boundary, not inside unrelated steps.

## Agent generation checklist

Before emitting a PaC workflow, verify all of the following:

1. Import `Step` and `Workflow` from `pac`.
2. Define every unit of work as a `Step` subclass with `run(self, ctx)`.
3. Do not require constructor arguments and do not register Step instances.
4. Register each Step class exactly once and in intentional deterministic order.
5. Put static per-step configuration in `inputs={...}` and read it with `ctx.input()`.
6. Use only string input keys and strict JSON-compatible input values.
7. Declare every data dependency with `depends_on=[...]`.
8. Read upstream persisted results with `ctx.output(UpstreamStep)`.
9. Return only `complete`, `retry`, `wait`, `fail`, or controller-only `repeat` results.
10. Ensure completed values are JSON-compatible; parse agent JSON before completing.
11. Set a finite `max_attempts` when retry or validation rejection is possible.
12. Add deterministic `validate_output()` logic for required agent-output properties.
13. Feed `ctx.retry_reason` into later prompts when corrective feedback helps.
14. Use `output_schema` for shape and Python validation for meaning.
15. Add timeouts and status checks to network requests.
16. Avoid secrets in persisted inputs, outputs, reasons, and events.
17. Remember that v1 is sequential, synchronous, and statically declared.
18. Call `workflow.loop()` and inspect the returned `WorkflowRun` or `WorkflowFailed.run`.
19. For cycles, omit the feedback edge from `depends_on`, declare it with `add_cycle()`, and set a finite iteration limit.
