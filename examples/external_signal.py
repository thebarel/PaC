from __future__ import annotations

from tempfile import TemporaryDirectory

from pac import Step, Workflow


class AwaitPayment(Step):
    def run(self, ctx):
        if ctx.signal_payload is None:
            return self.wait(signal="payment_received", payload_type=dict)
        return self.complete(ctx.signal_payload)


def webhook_handler(workflow: Workflow, run_id: str, body: dict) -> dict:
    """The core call a Flask/FastAPI/Django webhook handler would make."""

    receipt = workflow.signal(
        run_id,
        "payment_received",
        payload=body,
        event_id=body["event_id"],
        actor={"service": "payment-webhook"},
    )
    return {"duplicate": receipt.duplicate, "consumed": receipt.consumed}


def main() -> None:
    with TemporaryDirectory() as directory:
        workflow = Workflow("external-signal", state_path=f"{directory}/state.db")
        workflow.add_step(AwaitPayment)
        waiting = workflow.run()
        print(webhook_handler(workflow, waiting.id, {"event_id": "evt-1", "amount": 25}))
        print(workflow.resume(waiting.id).output(AwaitPayment))


if __name__ == "__main__":
    main()
