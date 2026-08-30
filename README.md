# PaC - Process as Code

PaC is a small durable workflow engine for agentic work. Workflows are ordinary Python code; PaC owns deterministic scheduling, dependencies, transitions, retries, persistence, recovery, and Codex thread lifecycle.

> PaC does not make language-model generation deterministic. It makes the process around agent execution deterministic: scheduling, dependencies, state transitions, retries, persistence, validation decisions, and replay decisions.

## Personal Note
This library was created with [flowboard](https://github.com/thebarel/flowboard) to create agentic workflows in a blink of an eye.
I must say, I used it few times, and it worked like a charm. My process was:
- Create the workflow design via `flowboard`
- Export it to PaC code (via the export button)
- Launch an AI agent in plan mode and telling it to fill the todos
- Answering all the questions about each node in the workflow
- Creating a fully working workflow - with deterministic steps

Currently, `PaC` only supports `codex` as its harness.

I really think that what makes this so code is the output validation, you can make sure that your agent is REALLY doing what it is supposed to do.

## Installation

PaC requires Python 3.11 or newer and uses the official Python Codex SDK.

```bash
git clone https://github.com/thebarel/PaC.git
cd PaC
python3 -m venv .venv
source .venv/bin/activate
pip3 install -e .
```

The SDK controls a local Codex app-server and uses your normal Codex authentication.

-----
## First workflow

```python
from pac import Workflow, Step


class First(Step):
    def run(self, ctx):
        return self.complete("hello")


class Second(Step):
    def run(self, ctx):
        first = ctx.output(First)
        return self.complete(first + " world")


workflow = Workflow("demo")
workflow.add_step(First)
workflow.add_step(Second, depends_on=[First])

run = workflow.loop()
assert run.output(Second) == "hello world"
```

Steps are registered as classes and instantiated by the framework. Their default stable identity is `module + qualname`. Outputs must be JSON-compatible; PaC never uses pickle.

## Step inputs and outputs

Pass durable, step-specific inputs when registering the step:

```python
class RootDomain(Step):
    def run(self, ctx):
        company = ctx.input("company")
        timeout = ctx.input("timeout_seconds", 30)

        return self.complete(
            {
                "company": company,
                "timeout_seconds": timeout,
            }
        )


workflow.add_step(
    RootDomain,
    inputs={
        "company": "Acme",
        "timeout_seconds": 20,
    },
)
```

`inputs` must be a mapping with string keys and JSON-compatible values. PaC snapshots inputs at registration, persists them in SQLite, and includes non-empty inputs in the workflow-definition fingerprint. Changing an input while a run is unfinished raises `WorkflowDefinitionChanged`; it never silently resumes with new configuration.

Inside a step:

- `ctx.input("name")` returns a required input and raises a clear `KeyError` when missing.
- `ctx.input("name", default)` returns the default when the input is absent.
- `ctx.inputs` exposes all inputs for the current step as a read-only mapping.
- `ctx.output(OtherStep)` returns the persisted output of a registered, completed step.

Inputs are configuration supplied by the workflow author. Outputs are runtime results supplied by completed steps. To make an upstream output available, declare the dependency explicitly:

```python
class CertificateLookup(Step):
    def run(self, ctx):
        discovery = ctx.output(RootDomain)
        domains = discovery["domains"]
        return self.complete({"searched_domains": domains})


workflow.add_step(RootDomain, inputs={"company": "Acme"})
workflow.add_step(CertificateLookup, depends_on=[RootDomain])
```

Inputs and outputs are stored unencrypted in the configured state database and may appear in events. Do not place passwords, API keys, or other secrets in them.

## Lifecycle and dependencies

A step returns one explicit result:

```python
return self.complete(value)
return self.retry("try again")
return self.wait("waiting for an external condition")
return self.fail("cannot continue")
```

The lifecycle is `PENDING -> RUNNING`, followed by `COMPLETED`, `WAITING`, `RETRY`, `REPEAT`, or `FAILED`. Completed acyclic steps never run again in the same workflow run; explicitly declared cycle members reset when their controller requests another iteration.

Dependencies must complete successfully before a step is runnable. If several steps are eligible, PaC always selects registration order. Execution is intentionally sequential in v1.

## Cyclic workflows

Cycles are explicit so an accidental dependency cycle cannot run forever. Declare forward dependencies normally, omit the feedback edge from `depends_on`, and register it separately:

```python
class Draft(Step):
    def run(self, ctx):
        feedback = ctx.latest_output(Review, None)
        return self.complete({"iteration": ctx.iteration, "feedback": feedback})


class Review(Step):
    def run(self, ctx):
        draft = ctx.output(Draft)
        if ctx.iteration < 3:
            return self.repeat({"requested_changes": ["add evidence"]}, reason="revise")
        return self.complete({"approved": True, "draft": draft})


workflow.add_step(Draft)
workflow.add_step(Review, depends_on=[Draft])
workflow.add_cycle(
    "review",
    steps=[Draft, Review],
    back_edge=(Review, Draft),
    max_iterations=5,
)
```

The back edge is `(Controller, Entry)`. `repeat(value, reason=...)` persists JSON feedback and resets the members for the next pass. `complete(value)` exits the cycle. `ctx.latest_output()` reads the latest persisted value even after a member resets, while `ctx.output()` still requires a currently completed step. Attempts restart at 1 each iteration, the controller's Codex thread is reused, and exceeding `max_iterations` fails the workflow.

Cycle members may contain forward branches and joins, but cannot overlap another cycle. The entry must reach every member and every member must reach the controller. A raw cycle made only with `depends_on` remains a definition error.

## Validating statistical agent output

A successful Codex turn is only a candidate result. Use deterministic Python validation when the workflow requires a specific semantic outcome:

```python
class Calculate(Step):
    max_attempts = 3

    def run(self, ctx):
        result = ctx.codex.run("Calculate 6 * 7. Return only the number.")
        return self.complete(result.text)

    def validate_output(self, output, ctx):
        if output != "42":
            return f"Expected 42, received {output!r}"
        return None
```

`None` accepts the candidate. A non-empty reason rejects it. PaC records the rejected candidate and reason, then retries the whole step while attempts remain. `ctx.retry_reason` exposes the last reason so the next prompt can include feedback. If every candidate is rejected, the step and workflow fail rather than accepting an unwanted answer.

JSON Schema controls structure, while `validate_output` controls domain meaning:

```python
result = ctx.codex.run(
    "Analyze the repository.",
    output_schema={
        "type": "object",
        "properties": {"summary": {"type": "string"}},
        "required": ["summary"],
    },
)
```

Validators must themselves be deterministic. An LLM judge can still vary and therefore cannot provide the same guarantee.

## `Workflow.loop()`

`loop()` is a reconciliation engine, not an unconditional Python loop. It repeatedly loads persisted state, selects one eligible step, records `RUNNING`, invokes it, and atomically persists the result and event. It stops when the workflow completes, fails, waits, or deadlocks.

The return value is a `WorkflowRun` containing its ID, status, step snapshots, outputs, and ordered event log. Failures raise `WorkflowFailed`, whose `.run` contains the failed snapshot.

## Persistence and resume

The default database is `<cwd>/.pac/state.db`; pass `state_path=` or a `StateStore` to configure it. PaC resumes the latest unfinished run with the same workflow name. Once a run is terminal, another `loop()` call creates a new run.

Every event has a monotonically increasing sequence number within its run. Timestamps are metadata, never ordering keys.

If a process stops while a step is `RUNNING`, the next process records a recovery and retries it if attempts remain. The interrupted attempt is consumed. PaC cannot guarantee exactly-once external side effects: a crash after Codex performs work but before PaC persists completion may repeat work.

The declarative workflow structure is fingerprinted. PaC refuses to resume an unfinished run if ordered steps, dependencies, step inputs, retry limits, working directory, model, or sandbox changed. Python source is not hashed in v1.

## Waiting

`self.wait(...)` pauses the workflow without spinning. PaC first runs any other eligible work, then returns a run with status `WAITING`. Calling `loop()` again explicitly resumes waiting steps. Waiting remains within the same logical attempt and does not consume retry quota.

## Codex integration

Each step gets its own Codex thread within a workflow run. Retries and later turns reuse that thread; unrelated steps never share hidden conversation state. Explicit information passes through persisted step outputs.

PaC lazily starts one official `openai_codex.Codex` client for a `loop()` invocation and closes it afterward. Pure Python workflows do not start Codex. Workflow-level `cwd`, `model`, and `sandbox` values are applied to step threads.

See [examples/repository_improvement.py](examples/repository_improvement.py), [examples/validated_calculation.py](examples/validated_calculation.py), and [examples/recon.py](examples/recon.py).

## Complete authoring reference for agents

[docs/workflow-authoring.md](docs/workflow-authoring.md) is the normative workflow-authoring guide. It documents constructor arguments, step registration, inputs, dependency outputs, context capabilities, results, validation, retries, waiting, Codex calls, persistence, errors, and a generation checklist. Give that file to an agent that needs to produce PaC workflows.

## Current limitations

- Sequential, synchronous execution only
- JSON-compatible outputs only
- Bounded retries; valid model output is not guaranteed
- Waiting requires an explicit later `loop()` call
- No timers, webhooks, queues, parallel workers, or distributed execution
- One active process may execute a given workflow name at a time
- No workflow-definition migration or Python source hashing
- No exactly-once guarantee for external side effects
- One explicit feedback edge/controller per non-overlapping cycle
