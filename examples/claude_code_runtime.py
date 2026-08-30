from __future__ import annotations

from dataclasses import dataclass
from tempfile import TemporaryDirectory

from pac import AgentRequest, ClaudeCodeOptions, ClaudeCodeRuntime, Step, Workflow


@dataclass
class Summary:
    summary: str
    confidence: float


SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    },
    "required": ["summary", "confidence"],
    "additionalProperties": False,
}


class Summarize(Step[dict, Summary]):
    async def run(self, ctx, inputs):
        result = await ctx.agent.execute(
            AgentRequest(
                prompt=f"Summarize this text: {inputs['text']}",
                output_schema=SCHEMA,
            )
        )
        return self.complete(Summary(**result.output))

    def validate_output(self, output, ctx):
        return None if output.confidence >= 0.7 else "confidence must be at least 0.7"


def main() -> None:
    # For unattended workers, use dontAsk with an explicit tool policy. This
    # example needs no tools, so both tool lists are empty.
    runtime = ClaudeCodeRuntime(
        ClaudeCodeOptions(
            model="claude-sonnet-4-5",
            permission_mode="dontAsk",
            allowed_tools=(),
            disallowed_tools=("Bash", "Write", "Edit"),
            max_turns=3,
        )
    )
    with TemporaryDirectory() as directory:
        workflow = Workflow(
            "claude-code-summary",
            state_path=f"{directory}/state.db",
            agent_runtime=runtime,
        )
        workflow.add_step(
            Summarize,
            inputs={"text": "Nondeterministic intelligence inside deterministic process execution."},
        )
        print(workflow.run().output(Summarize))


if __name__ == "__main__":
    main()
