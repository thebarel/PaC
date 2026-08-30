from __future__ import annotations

import asyncio
from tempfile import TemporaryDirectory

from pac import Step, Workflow


class Market(Step):
    async def run(self, ctx):
        await asyncio.sleep(0.02)
        return self.complete(["market finding"])


class Technology(Step):
    async def run(self, ctx):
        await asyncio.sleep(0.02)
        return self.complete(["technology finding"])


class Operations(Step):
    async def run(self, ctx):
        await asyncio.sleep(0.02)
        return self.complete(["operations finding"])


class Synthesis(Step):
    def run(self, ctx):
        findings = (
            ctx.output(Market) + ctx.output(Technology) + ctx.output(Operations)
        )
        return self.complete({"findings": findings})


async def execute(path: str):
    workflow = Workflow("parallel-research", state_path=path, max_concurrency=3)
    workflow.add_step(Market)
    workflow.add_step(Technology)
    workflow.add_step(Operations)
    workflow.add_step(
        Synthesis, depends_on=[Market, Technology, Operations]
    )
    return await workflow.arun()


def main() -> None:
    with TemporaryDirectory() as directory:
        run = asyncio.run(execute(f"{directory}/state.db"))
        print(run.output(Synthesis))


if __name__ == "__main__":
    main()
