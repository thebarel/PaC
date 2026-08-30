from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol, Sequence

from .models import WorkflowEvent
from .state.base import StateStore


class EventExporter(Protocol):
    def export(self, events: Sequence[WorkflowEvent]) -> None: ...


class LoggingExporter:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger("pac.events")

    def export(self, events: Sequence[WorkflowEvent]) -> None:
        for event in events:
            self.logger.info(
                event.type,
                extra={
                    "sequence": event.sequence,
                    "timestamp": event.timestamp,
                    "step_id": event.step_id,
                    "attempt": event.attempt,
                    "iteration": event.iteration,
                    "event_data": event.data,
                },
            )


@dataclass(slots=True)
class EventExportCursor:
    """Durable cursor: advances only after an exporter accepts the batch."""

    store: StateStore
    exporter: EventExporter
    name: str = "default"

    def export_run(self, run_id: str, *, limit: int = 100) -> int:
        events = self.store.pending_export_events(run_id, self.name, limit=limit)
        if not events:
            return 0
        self.exporter.export(events)
        self.store.advance_export_cursor(run_id, self.name, events[-1].sequence)
        return len(events)


class OpenTelemetryExporter:
    """Optional event adapter; requires opentelemetry-api at construction time."""

    def __init__(self, tracer_name: str = "pac") -> None:
        try:
            from opentelemetry import trace
        except ImportError as exc:
            raise RuntimeError("Install the 'otel' extra to use OpenTelemetryExporter") from exc
        self._tracer = trace.get_tracer(tracer_name)

    def export(self, events: Sequence[WorkflowEvent]) -> None:
        for event in events:
            with self._tracer.start_as_current_span(event.type) as span:
                span.set_attribute("pac.event.sequence", event.sequence)
                if event.step_id:
                    span.set_attribute("pac.step.id", event.step_id)
                if event.attempt is not None:
                    span.set_attribute("pac.step.attempt", event.attempt)
