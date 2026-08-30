from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from types import ModuleType
from typing import Any

import pytest

from pac import (
    AgentExecutionContext,
    AgentRequest,
    ClaudeCodeOptions,
    ClaudeCodeRuntime,
    EnvironmentSecretProvider,
    SecretContext,
    SecretResolver,
    Step,
    Workflow,
    WorkflowDefinitionChanged,
    WorkflowFailed,
)


class FakeOptions:
    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)


class ResultMessage:
    def __init__(self, **values: Any) -> None:
        defaults = {
            "subtype": "success",
            "is_error": False,
            "result": "ok",
            "structured_output": None,
            "session_id": "session-1",
            "uuid": "result-1",
            "usage": {},
            "model": "claude-test",
            "duration_ms": 100,
            "duration_api_ms": 80,
            "num_turns": 1,
            "total_cost_usd": None,
            "permission_denials": [],
            "stop_reason": "end_turn",
            "terminal_reason": None,
            "model_usage": None,
        }
        defaults.update(values)
        self.__dict__.update(defaults)


class ScriptedQuery:
    def __init__(self, scripts: list[list[Any]]) -> None:
        self.scripts = iter(scripts)
        self.calls: list[tuple[Any, FakeOptions]] = []
        self.closed = 0

    def __call__(self, *, prompt: Any, options: FakeOptions):
        owner = self
        messages = iter(next(self.scripts))

        class Stream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return next(messages)
                except StopIteration:
                    raise StopAsyncIteration

            async def aclose(self):
                owner.closed += 1

        self.calls.append((prompt, options))
        return Stream()


def install_fake_sdk(monkeypatch, query=None) -> None:
    package = ModuleType("claude_agent_sdk")
    package.ClaudeAgentOptions = FakeOptions
    package.ResultMessage = ResultMessage
    package.query = query or ScriptedQuery([[ResultMessage()]])
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", package)


def test_options_structured_output_usage_and_session_mapping(tmp_path, monkeypatch):
    query = ScriptedQuery(
        [
            [
                ResultMessage(
                    structured_output={"answer": "durable"},
                    result=None,
                    usage={
                        "input_tokens": 10,
                        "output_tokens": 4,
                        "cache_read_input_tokens": 3,
                        "cache_creation_input_tokens": 2,
                    },
                    total_cost_usd=0.012,
                    model_usage={"claude-test": {"outputTokens": 4}},
                )
            ]
        ]
    )
    install_fake_sdk(monkeypatch, query)
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
    }

    class Ask(Step):
        async def run(self, ctx):
            result = await ctx.agent.execute(
                AgentRequest("private prompt", output_schema=schema, model="request-model")
            )
            assert result.provider == "anthropic"
            assert result.session_id == "session-1"
            assert result.invocation_id == "result-1"
            assert result.usage.input_tokens == 10
            assert result.usage.cached_tokens == 3
            assert result.usage.total_tokens == 14
            assert result.usage.cost == Decimal("0.012")
            assert result.usage.latency_seconds == 0.08
            return self.complete(result.output)

    options = ClaudeCodeOptions(
        model="default-model",
        fallback_model="fallback",
        system_prompt="system",
        max_turns=4,
        max_budget_usd=Decimal("1.25"),
        tools=("Read",),
        allowed_tools=("Read",),
        disallowed_tools=("Bash",),
        permission_mode="dontAsk",
        setting_sources=("project",),
        cli_path="/opt/claude",
        max_thinking_tokens=100,
        effort="high",
        env={"CLAUDE_CODE_MAX_RETRIES": "2"},
    )
    workflow = Workflow(
        "claude-structured",
        cwd=tmp_path,
        state_path=tmp_path / "state.db",
        agent_runtime=ClaudeCodeRuntime(options, query_factory=query),
    ).add_step(Ask)
    run = workflow.run()

    assert run.output(Ask) == {"answer": "durable"}
    prompt, sdk_options = query.calls[0]
    assert prompt == "private prompt"
    assert sdk_options.cwd == str(tmp_path)
    assert sdk_options.model == "request-model"
    assert sdk_options.output_format == {"type": "json_schema", "schema": schema}
    assert sdk_options.permission_mode == "dontAsk"
    assert sdk_options.disallowed_tools == ["Bash"]
    assert workflow.state_store.get_runtime_session(
        run.id, next(iter(run.steps)), "claude-code"
    ) == {"session_id": "session-1"}
    assert all("private prompt" not in repr(event.data) for event in run.events)
    assert query.closed == 1


def test_real_sdk_option_constructor_compatibility(tmp_path):
    sdk = pytest.importorskip("claude_agent_sdk")
    runtime = ClaudeCodeRuntime(
        ClaudeCodeOptions(
            model="claude-sonnet-4-5",
            allowed_tools=("Read",),
            disallowed_tools=("Bash",),
            permission_mode="dontAsk",
            setting_sources=("project",),
            max_turns=2,
            max_budget_usd=Decimal("0.50"),
            max_thinking_tokens=64,
            effort="high",
            env={"CLAUDE_CODE_MAX_RETRIES": "1"},
        )
    )
    context = AgentExecutionContext(
        workflow_id="compatibility",
        run_id="run",
        step_id="step",
        attempt=1,
        iteration=1,
        secrets=SecretResolver(
            EnvironmentSecretProvider(),
            SecretContext("compatibility", "run", "step"),
        ),
        cwd=tmp_path,
    )

    options = sdk.ClaudeAgentOptions(
        **runtime._option_values(AgentRequest("prompt"), context, None)
    )
    assert options.cwd == str(tmp_path)
    assert options.allowed_tools == ["Read"]
    assert options.permission_mode == "dontAsk"


def test_validation_retry_resumes_same_claude_session(tmp_path, monkeypatch):
    query = ScriptedQuery(
        [
            [ResultMessage(result="bad", session_id="session-retry")],
            [ResultMessage(result="accepted", session_id="session-retry")],
        ]
    )
    install_fake_sdk(monkeypatch, query)

    class RetryAgent(Step):
        max_attempts = 2

        async def run(self, ctx):
            return self.complete((await ctx.agent.execute("try")).output)

        def validate_output(self, output, ctx):
            return None if output == "accepted" else "try again"

    workflow = Workflow(
        "claude-retry",
        state_path=tmp_path / "state.db",
        agent_runtime=ClaudeCodeRuntime(query_factory=query),
    ).add_step(RetryAgent)
    run = workflow.run()

    assert run.output(RetryAgent) == "accepted"
    assert not hasattr(query.calls[0][1], "resume")
    assert query.calls[1][1].resume == "session-retry"


def test_separate_steps_have_separate_sessions(tmp_path, monkeypatch):
    query = ScriptedQuery(
        [
            [ResultMessage(result="one", session_id="session-one")],
            [ResultMessage(result="two", session_id="session-two")],
        ]
    )
    install_fake_sdk(monkeypatch, query)

    class One(Step):
        async def run(self, ctx):
            return self.complete((await ctx.agent.execute("one")).output)

    class Two(Step):
        async def run(self, ctx):
            return self.complete((await ctx.agent.execute("two")).output)

    workflow = Workflow(
        "claude-sessions",
        state_path=tmp_path / "state.db",
        agent_runtime=ClaudeCodeRuntime(query_factory=query),
    )
    workflow.add_step(One)
    workflow.add_step(Two, depends_on=[One])
    run = workflow.run()

    sessions = {
        workflow.state_store.get_runtime_session(run.id, step_id, "claude-code")["session_id"]
        for step_id in run.steps
    }
    assert sessions == {"session-one", "session-two"}


def test_error_missing_structured_output_and_missing_result(tmp_path, monkeypatch):
    query = ScriptedQuery(
        [
            [ResultMessage(is_error=True, subtype="error_during_execution")],
            [ResultMessage(structured_output=None)],
            [],
        ]
    )
    install_fake_sdk(monkeypatch, query)
    runtime = ClaudeCodeRuntime(query_factory=query)

    def workflow_for(name, request):
        class Ask(Step):
            async def run(self, ctx):
                return self.complete((await ctx.agent.execute(request)).output)

        return Workflow(
            name,
            state_path=tmp_path / f"{name}.db",
            agent_runtime=runtime,
        ).add_step(Ask)

    with pytest.raises(WorkflowFailed, match="Claude Code query failed"):
        workflow_for("claude-error", "prompt").run()
    with pytest.raises(WorkflowFailed, match="no structured output"):
        workflow_for(
            "claude-no-structure",
            AgentRequest("prompt", output_schema={"type": "object"}),
        ).run()
    with pytest.raises(WorkflowFailed, match="without a terminal ResultMessage"):
        workflow_for("claude-no-result", "prompt").run()


def test_optional_dependency_error_is_clear(tmp_path, monkeypatch):
    from pac.runtime import claude_code

    original = claude_code.importlib.import_module

    def missing(name):
        if name == "claude_agent_sdk":
            raise ImportError(name)
        return original(name)

    monkeypatch.setattr(claude_code.importlib, "import_module", missing)

    class Ask(Step):
        async def run(self, ctx):
            return self.complete((await ctx.agent.execute("prompt")).output)

    workflow = Workflow(
        "claude-missing",
        state_path=tmp_path / "state.db",
        agent_runtime=ClaudeCodeRuntime(),
    ).add_step(Ask)
    with pytest.raises(WorkflowFailed, match="claude-code.*extra"):
        workflow.run()


def test_options_are_immutable_snapshots_and_do_not_fingerprint_secret_values():
    env = {"ANTHROPIC_API_KEY": "first-secret"}
    tools = ["Read"]
    first = ClaudeCodeOptions(tools=tools, env=env)
    fingerprint = first.fingerprint_config()

    env["ANTHROPIC_API_KEY"] = "changed-secret"
    tools.append("Bash")
    second = ClaudeCodeOptions(
        tools=("Read",), env={"ANTHROPIC_API_KEY": "another-secret"}
    )

    assert first.tools == ("Read",)
    assert first.env["ANTHROPIC_API_KEY"] == "first-secret"
    assert fingerprint == second.fingerprint_config()
    assert "secret" not in repr(fingerprint)
    with pytest.raises(TypeError):
        first.env["OTHER"] = "value"


def test_runtime_configuration_changes_fingerprint(tmp_path, monkeypatch):
    install_fake_sdk(monkeypatch)

    class Wait(Step):
        def run(self, ctx):
            return self.wait("pause")

    path = tmp_path / "state.db"
    first = Workflow(
        "claude-fingerprint",
        state_path=path,
        agent_runtime=ClaudeCodeRuntime(ClaudeCodeOptions(allowed_tools=("Read",))),
    ).add_step(Wait)
    waiting = first.run()

    second = Workflow(
        "claude-fingerprint",
        state_path=path,
        agent_runtime=ClaudeCodeRuntime(ClaudeCodeOptions(allowed_tools=("Read", "Grep"))),
    ).add_step(Wait)
    with pytest.raises(WorkflowDefinitionChanged):
        second.resume(waiting.id)


def test_cancellation_closes_query_stream(tmp_path, monkeypatch):
    install_fake_sdk(monkeypatch)
    entered = asyncio.Event()
    closed = asyncio.Event()

    class HangingQuery:
        def __call__(self, **kwargs):
            class Stream:
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    entered.set()
                    await asyncio.Event().wait()
                    raise StopAsyncIteration

                async def aclose(self):
                    closed.set()

            return Stream()

    class Ask(Step):
        async def run(self, ctx):
            return self.complete((await ctx.agent.execute("wait")).output)

    workflow = Workflow(
        "claude-cancel",
        state_path=tmp_path / "state.db",
        agent_runtime=ClaudeCodeRuntime(query_factory=HangingQuery()),
    ).add_step(Ask)

    async def execute():
        task = asyncio.create_task(workflow.arun())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(closed.wait(), timeout=1)

    asyncio.run(execute())
    run = workflow.state_store.list_runs("claude-cancel")[-1]
    invocation = workflow.state_store.agent_invocations(run.id)[0]
    assert invocation.status == "FAILED"
    assert invocation.error == "CancelledError: agent invocation cancelled"
