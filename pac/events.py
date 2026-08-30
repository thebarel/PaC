from __future__ import annotations

from enum import StrEnum
from typing import Any

from .models import JsonValue
from .secrets import SecretRef, SecretValue


class EventType(StrEnum):
    WORKFLOW_CREATED = "workflow.created"
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_RESUMED = "workflow.resumed"
    WORKFLOW_WAITING = "workflow.waiting"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_CANCELLED = "workflow.cancelled"
    STEP_REGISTERED = "step.registered"
    STEP_RUNNABLE = "step.runnable"
    STEP_CLAIMED = "step.claimed"
    STEP_STARTED = "step.started"
    STEP_COMPLETED = "step.completed"
    STEP_FAILED = "step.failed"
    STEP_RETRY_REQUESTED = "step.retry_requested"
    STEP_OUTPUT_REJECTED = "step.output_rejected"
    VALIDATION_ACCEPTED = "validation.accepted"
    VALIDATION_REJECTED = "validation.rejected"
    STEP_WAITING = "step.waiting"
    STEP_RESUMED = "step.resumed"
    STEP_RECOVERED = "step.recovered"
    STEP_TIMEOUT = "step.timeout"
    STEP_LEASE_EXPIRED = "step.lease_expired"
    AGENT_REQUEST_STARTED = "agent.request_started"
    AGENT_REQUEST_FINISHED = "agent.request_finished"
    AGENT_REQUEST_FAILED = "agent.request_failed"
    SIGNAL_RECEIVED = "signal.received"
    SIGNAL_CONSUMED = "signal.consumed"
    TIMER_SCHEDULED = "timer.scheduled"
    TIMER_FIRED = "timer.fired"
    HUMAN_APPROVAL_REQUESTED = "human.approval_requested"
    HUMAN_APPROVAL_RECEIVED = "human.approval_received"
    IDEMPOTENCY_CLAIMED = "idempotency.claimed"
    IDEMPOTENCY_COMPLETED = "idempotency.completed"


_SENSITIVE_KEYS = frozenset(
    {"api_key", "authorization", "credential", "password", "prompt", "secret", "token"}
)


def _sensitive_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in _SENSITIVE_KEYS:
        return True
    parts = lowered.replace("-", "_").split("_")
    return any(part in _SENSITIVE_KEYS for part in parts) and not lowered.endswith("_tokens")


def sanitize_event_data(value: Any, *, key: str | None = None) -> JsonValue:
    """Return strict JSON-safe event data without resolved secrets or prompts."""

    if key is not None and _sensitive_key(key):
        return "***"
    if isinstance(value, SecretValue):
        return "***"
    if isinstance(value, SecretRef):
        return {"secret_ref": value.name}
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, dict):
        return {str(item_key): sanitize_event_data(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_event_data(item) for item in value]
    return repr(value)
