from __future__ import annotations

from tempfile import TemporaryDirectory

from pac import Step, Workflow


class FakePaymentAPI:
    def __init__(self) -> None:
        self.receipts: dict[str, dict] = {}

    def charge(self, order_id: str, *, idempotency_key: str) -> dict:
        return self.receipts.setdefault(
            idempotency_key,
            {"order_id": order_id, "status": "charged", "key": idempotency_key},
        )


payments = FakePaymentAPI()


class Charge(Step):
    max_attempts = 2

    def run(self, ctx):
        key = ctx.idempotency_key_for("charge")
        receipt = payments.charge("order-123", idempotency_key=key)
        if ctx.attempt == 1:
            return self.retry("demonstrate stable external idempotency")
        return self.complete(receipt)


def main() -> None:
    with TemporaryDirectory() as directory:
        workflow = Workflow("idempotent-action", state_path=f"{directory}/state.db")
        workflow.add_step(Charge)
        run = workflow.run()
        print(run.output(Charge))
        print("remote charge count:", len(payments.receipts))


if __name__ == "__main__":
    main()
