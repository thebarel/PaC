from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass

import pytest

from pac import (
    AgentRequest,
    AgentResult,
    AgentUsage,
    CompositeValidator,
    FakeAgentRuntime,
    FunctionValidator,
    SecretRef,
    SecretValue,
    Step,
    Workflow,
    WorkflowDefinitionError,
    WorkflowFailed,
)


@dataclass
class TypedInput:
    name: str
    count: int


@dataclass
class TypedOutput:
    greeting: str
    doubled: int


def test_typed_dataclass_inputs_outputs_and_dependency_decoding(tmp_path):
    seen = []

    class Build(Step[TypedInput, TypedOutput]):
        def run(self, ctx, inputs):
            assert isinstance(inputs, TypedInput)
            seen.append(inputs)
            return self.complete(TypedOutput(f"hello {inputs.name}", inputs.count * 2))

    class Consume(Step):
        def run(self, ctx):
            output = ctx.output(Build)
            assert isinstance(output, TypedOutput)
            return self.complete(output.doubled)

    workflow = Workflow("typed", state_path=tmp_path / "state.db")
    workflow.add_step(Build, inputs=TypedInput("Ada", 3))
    workflow.add_step(Consume, depends_on=[Build])
    run = workflow.loop()

    assert seen == [TypedInput("Ada", 3)]
    assert run.output(Build) == {"doubled": 6, "greeting": "hello Ada"}
    assert run.output(Consume) == 6


def test_invalid_typed_input_is_rejected_at_definition_time(tmp_path):
    class NeedsInput(Step[TypedInput, TypedOutput]):
        def run(self, ctx, inputs):
            return self.complete(TypedOutput(inputs.name, inputs.count))

    workflow = Workflow("bad-typed-input", state_path=tmp_path / "state.db")
    with pytest.raises(WorkflowDefinitionError, match="Missing required field"):
        workflow.add_step(NeedsInput, inputs={"name": "Ada"})


def test_invalid_typed_output_fails_before_persistence(tmp_path):
    class BadOutput(Step[TypedInput, TypedOutput]):
        def run(self, ctx, inputs):
            return self.complete({"greeting": "hello", "doubled": "wrong"})

    workflow = Workflow("bad-typed-output", state_path=tmp_path / "state.db")
    workflow.add_step(BadOutput, inputs=TypedInput("Ada", 1))
    with pytest.raises(WorkflowFailed, match="Output validation"):
        workflow.loop()


def test_composable_validators_keep_deterministic_retry(tmp_path):
    values = iter([TypedOutput("short", 1), TypedOutput("long enough", 2)])

    class Validated(Step[TypedInput, TypedOutput]):
        max_attempts = 2
        validators = (
            CompositeValidator(
                [FunctionValidator(lambda value, ctx: "too short" if len(value.greeting) < 8 else None)]
            ),
        )

        def run(self, ctx, inputs):
            return self.complete(next(values))

    workflow = Workflow("typed-validation", state_path=tmp_path / "state.db")
    workflow.add_step(Validated, inputs=TypedInput("Ada", 1))
    run = workflow.loop()
    assert run.output(Validated)["greeting"] == "long enough"
    assert [event.type for event in run.events].count("step.output_rejected") == 1


def test_fake_runtime_uses_provider_neutral_contract(tmp_path):
    fake = FakeAgentRuntime(
        [AgentResult("answer", provider="fake", usage=AgentUsage(input_tokens=2, output_tokens=1))]
    )

    class AgentStep(Step):
        def run(self, ctx):
            result = ctx.agent.run("question", model="test-model")
            return self.complete({"answer": result.text, "tokens": result.usage.total_tokens})

    workflow = Workflow("fake-agent", state_path=tmp_path / "state.db", agent_runtime=fake)
    workflow.add_step(AgentStep)
    run = workflow.loop()

    assert run.output(AgentStep) == {"answer": "answer", "tokens": None}
    request, context = fake.requests[0]
    assert request == AgentRequest(prompt="question", model="test-model")
    assert context.run_id == run.id


def test_fake_runtime_async_execute():
    fake = FakeAgentRuntime(["ok"])
    # The context shape is exercised through Workflow above; this ensures native async API exists.
    assert asyncio.iscoroutinefunction(fake.execute)


def test_secrets_resolve_at_execution_without_persisting_value(tmp_path, monkeypatch):
    monkeypatch.setenv("PAC_TEST_SECRET", "do-not-persist")

    class UsesSecret(Step):
        def run(self, ctx):
            value = ctx.secrets.get(ctx.input("credential"))
            assert repr(value) == "SecretValue(***)"
            assert str(value) == "***"
            assert value.reveal() == "do-not-persist"
            return self.complete("used")

    database = tmp_path / "state.db"
    workflow = Workflow("secrets", state_path=database)
    workflow.add_step(UsesSecret, inputs={"credential": SecretRef("PAC_TEST_SECRET")})
    run = workflow.loop()
    assert run.output(UsesSecret) == "used"

    with sqlite3.connect(database) as connection:
        persisted = "\n".join(
            str(value)
            for table in ("workflow_runs", "step_runs", "events")
            for row in connection.execute(f"SELECT * FROM {table}").fetchall()
            for value in row
        )
    assert "do-not-persist" not in persisted
    assert "PAC_TEST_SECRET" in persisted


def test_resolved_secret_cannot_be_persisted(tmp_path):
    class LeaksSecret(Step):
        def run(self, ctx):
            return self.complete(SecretValue("hidden"))

    workflow = Workflow("secret-leak", state_path=tmp_path / "state.db")
    workflow.add_step(LeaksSecret)
    with pytest.raises(Exception, match="non-JSON-serializable"):
        workflow.loop()
