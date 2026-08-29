from pac import Step, Workflow


class Calculate(Step):
    max_attempts = 3

    def run(self, ctx):
        feedback = f" Previous output was rejected: {ctx.retry_reason}" if ctx.retry_reason else ""
        result = ctx.codex.run(f"Calculate 6 * 7. Return only the integer.{feedback}")
        return self.complete(result.text)

    def validate_output(self, output, ctx):
        if output != "42":
            return f"Expected exactly '42', received {output!r}"
        return None


workflow = Workflow("validated-calculation", cwd=".")
workflow.add_step(Calculate)
result = workflow.loop()
print(result.output(Calculate))

