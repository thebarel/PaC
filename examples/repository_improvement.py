from pac import Step, Workflow


class AnalyzeRepository(Step):
    def run(self, ctx):
        result = ctx.codex.run(
            """
            Analyze this repository.

            Return:
            - architecture summary
            - important components
            - potential problems
            """
        )
        return self.complete(result.text)

    def validate_output(self, output, ctx):
        if not output or len(output.strip()) < 40:
            return "The repository analysis was empty or too short"
        return None


class ImplementImprovement(Step):
    max_attempts = 2

    def run(self, ctx):
        analysis = ctx.output(AnalyzeRepository)
        retry_feedback = f"\nPrevious attempt failed: {ctx.retry_reason}" if ctx.retry_reason else ""
        result = ctx.codex.run(
            f"""
            Here is the previous analysis:

            {analysis}

            Implement the highest-value improvement.
            Run relevant tests afterward.
            {retry_feedback}
            """
        )
        return self.complete(result.text)

    def validate_output(self, output, ctx):
        if not output or "test" not in output.lower():
            return "The result must report the tests that were run"
        return None


def main():
    workflow = Workflow("repository-improvement", cwd=".")
    workflow.add_step(AnalyzeRepository)
    workflow.add_step(ImplementImprovement, depends_on=[AnalyzeRepository])

    result = workflow.loop()
    print(result.status)
    print(result.outputs)


if __name__ == "__main__":
    main()

