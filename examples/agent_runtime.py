from __future__ import annotations

from tempfile import TemporaryDirectory

from pac import AgentResult, AgentUsage, FakeAgentRuntime, Step, Workflow


class Summarize(Step):
    async def run(self, ctx):
        result = await ctx.agent.execute("Summarize deterministic orchestration")
        return self.complete(
            {"summary": result.output, "tokens": result.usage.total_tokens}
        )


def main() -> None:
    runtime = FakeAgentRuntime(
        [
            AgentResult(
                output="Nondeterministic actors inside a durable process.",
                provider="fake",
                model="example",
                usage=AgentUsage(input_tokens=5, output_tokens=7, total_tokens=12),
            )
        ]
    )
    with TemporaryDirectory() as directory:
        workflow = Workflow(
            "agent-runtime",
            state_path=f"{directory}/state.db",
            agent_runtime=runtime,
        )
        workflow.add_step(Summarize)
        run = workflow.run()
        print(run.output(Summarize))
        print(workflow.state_store.usage(run.id))


if __name__ == "__main__":
    main()
