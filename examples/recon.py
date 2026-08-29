import json

import requests

from pac import Step, Workflow


class RootDomain(Step):
    max_attempts = 3
    JSON_OUTPUT = r'{"company": "Google", "domains": ["google.com", "google.net"]}'

    def run(self, ctx):
        company = ctx.input("company")
        feedback = (
            f"Previous output was rejected: {ctx.retry_reason}"
            if ctx.retry_reason
            else ""
        )
        result = ctx.codex.run(
            f"""
            Find all root domains for {company}.
            Return JSON shaped like:
            {self.JSON_OUTPUT}

            {feedback}
            """
        )

        try:
            output = json.loads(result.text)
        except (json.JSONDecodeError, TypeError) as exc:
            return self.retry(f"Expected JSON: {exc}")

        return self.complete(output)

    def validate_output(self, output, ctx):
        if set(output) != {"company", "domains"}:
            return f"Expected company and domains fields, received {output!r}"

        if not isinstance(output["company"], str):
            return "company must be a string"

        if not isinstance(output["domains"], list) or not all(
            isinstance(domain, str) for domain in output["domains"]
        ):
            return "domains must be a list of strings"

        return None


class CertificateLookup(Step):
    max_attempts = 3

    def run(self, ctx):
        data = ctx.output(RootDomain)

        results = {}
        for domain in data["domains"]:
            response = requests.get(
                "https://crt.name/v1/search",
                params={"apex": domain},
                timeout=30,
            )
            response.raise_for_status()
            results[domain] = response.text

        return self.complete(results)


workflow = Workflow("recon", cwd=".")

workflow.add_step(
    RootDomain,
    inputs={"company": "Ribbon Communications"},
)
workflow.add_step(
    CertificateLookup,
    depends_on=[RootDomain],
)
result = workflow.loop()
print(result.output(RootDomain))
print(result.output(CertificateLookup))
