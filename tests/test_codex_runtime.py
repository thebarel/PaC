from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from typing import ClassVar

from pac import Step, Workflow


class FakeTurn:
    def __init__(self, thread, turn_id):
        self.thread = thread
        self.id = turn_id

    def run(self):
        self.thread.run_count += 1
        if self.thread.id == "thread-1" and self.thread.run_count == 1:
            response = "bad"
        else:
            response = "accepted"
        return SimpleNamespace(
            final_response=response,
            status=SimpleNamespace(value="completed"),
        )


class FakeThread:
    def __init__(self, thread_id):
        self.id = thread_id
        self.run_count = 0
        self.schemas = []

    def turn(self, prompt, *, output_schema=None):
        self.schemas.append(output_schema)
        return FakeTurn(self, f"{self.id}-turn-{len(self.schemas)}")


class FakeCodex:
    instances: ClassVar[list[FakeCodex]] = []
    threads: ClassVar[dict[str, FakeThread]] = {}
    resumed: ClassVar[list[str]] = []

    def __init__(self, config):
        self.config = config
        self.closed = False
        self.instances.append(self)

    def thread_start(self, **options):
        thread_id = f"thread-{len(self.threads) + 1}"
        thread = FakeThread(thread_id)
        self.threads[thread_id] = thread
        return thread

    def thread_resume(self, thread_id, **options):
        self.resumed.append(thread_id)
        return self.threads[thread_id]

    def close(self):
        self.closed = True


class FakeConfig:
    def __init__(self, *, cwd=None):
        self.cwd = cwd


def install_fake_sdk(monkeypatch):
    FakeCodex.instances.clear()
    FakeCodex.threads.clear()
    FakeCodex.resumed.clear()
    package = ModuleType("openai_codex")
    package.Codex = FakeCodex
    package.CodexConfig = FakeConfig
    client = ModuleType("openai_codex.client")
    client.CodexConfig = FakeConfig
    monkeypatch.setitem(sys.modules, "openai_codex", package)
    monkeypatch.setitem(sys.modules, "openai_codex.client", client)


def test_one_client_separate_step_threads_and_schema_forwarding(tmp_path, monkeypatch):
    install_fake_sdk(monkeypatch)
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
    }

    class AgentOne(Step):
        max_attempts = 2

        def run(self, ctx):
            result = ctx.codex.run("first", output_schema=schema)
            assert result.raw.final_response == result.text
            return self.complete(result.text)

        def validate_output(self, output, ctx):
            return None if output == "accepted" else "agent output was rejected"

    class AgentTwo(Step):
        def run(self, ctx):
            return self.complete(ctx.codex.run("second").text)

    workflow = Workflow("codex", cwd=tmp_path, state_path=tmp_path / "state.db")
    workflow.add_step(AgentOne)
    workflow.add_step(AgentTwo, depends_on=[AgentOne])
    run = workflow.loop()

    assert len(FakeCodex.instances) == 1
    assert FakeCodex.instances[0].closed
    assert set(FakeCodex.threads) == {"thread-1", "thread-2"}
    assert FakeCodex.threads["thread-1"].run_count == 2
    assert FakeCodex.threads["thread-2"].run_count == 1
    assert FakeCodex.threads["thread-1"].schemas == [schema, schema]
    thread_ids = {state.codex_thread_id for state in run.steps.values()}
    assert thread_ids == {"thread-1", "thread-2"}


def test_persisted_thread_is_resumed_after_wait(tmp_path, monkeypatch):
    install_fake_sdk(monkeypatch)
    calls = 0

    class AgentWait(Step):
        def run(self, ctx):
            nonlocal calls
            calls += 1
            result = ctx.codex.run("continue")
            if calls == 1:
                return self.wait("pause")
            return self.complete(result.text)

    database = tmp_path / "state.db"
    first = Workflow("codex-resume", cwd=tmp_path, state_path=database)
    first.add_step(AgentWait)
    waiting = first.loop()
    thread_id = next(iter(waiting.steps.values())).codex_thread_id

    second = Workflow("codex-resume", cwd=tmp_path, state_path=database)
    second.add_step(AgentWait)
    completed = second.loop()

    assert completed.output(AgentWait) == "accepted"
    assert FakeCodex.resumed == [thread_id]
    assert len(FakeCodex.instances) == 2
    assert all(instance.closed for instance in FakeCodex.instances)
