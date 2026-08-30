from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..codecs import schema_spec, validate_schema_spec
from ..errors import ConcurrencyError, StateStoreError, ValidationError
from ..events import sanitize_event_data
from ..models import (
    CycleState,
    CycleStatus,
    HumanTask,
    IdempotencyClaim,
    JsonValue,
    SignalReceipt,
    StepClaim,
    StepState,
    StepStatus,
    WorkflowDefinition,
    WorkflowEvent,
    WorkflowRun,
    WorkflowStatus,
)
from ..runtime.base import AgentInvocation, AgentUsage
from ..usage import UsageSummary
from ..waits import TimeoutAction, WaitKind, WaitRequest
from .base import StateStore
from .encryption import JsonPayloadCodec, PayloadCodec
from .migrations import (
    DEFINITION_FORMAT_VERSION,
    EVENT_SCHEMA_VERSION,
    MIGRATIONS,
    STATE_FORMAT_VERSION,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


class SQLiteStateStore(StateStore):
    """SQLite-backed durable state with transactional events."""

    def __init__(
        self,
        path: str | Path = ".pac/state.db",
        *,
        clock: Callable[[], datetime] = _utc_now,
        payload_codec: PayloadCodec | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self._payload_codec = payload_codec or JsonPayloadCodec()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        try:
            with self._connect() as connection:
                connection.executescript(
                    """
                    PRAGMA journal_mode = WAL;

                    CREATE TABLE IF NOT EXISTS pac_schema_migrations (
                        version INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        checksum TEXT NOT NULL,
                        applied_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS workflow_runs (
                        run_id TEXT PRIMARY KEY,
                        workflow_name TEXT NOT NULL,
                        definition_fingerprint TEXT NOT NULL,
                        definition_json TEXT NOT NULL,
                        definition_format_version INTEGER NOT NULL DEFAULT 1,
                        state_format_version INTEGER NOT NULL DEFAULT 1,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT,
                        error TEXT,
                        cancellation_reason TEXT,
                        next_event_sequence INTEGER NOT NULL DEFAULT 1
                    );

                    CREATE TABLE IF NOT EXISTS step_runs (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        registration_order INTEGER NOT NULL,
                        dependencies_json TEXT NOT NULL,
                        inputs_json TEXT NOT NULL DEFAULT '{}',
                        max_attempts INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        attempt INTEGER NOT NULL DEFAULT 0,
                        started_at TEXT,
                        completed_at TEXT,
                        error TEXT,
                        output_json TEXT,
                        codex_thread_id TEXT,
                        retry_reason TEXT,
                        waiting_reason TEXT,
                        iteration INTEGER NOT NULL DEFAULT 1,
                        claim_owner TEXT,
                        claim_token TEXT,
                        claimed_at TEXT,
                        lease_expires_at TEXT,
                        heartbeat_at TEXT,
                        available_at TEXT,
                        signal_payload_json TEXT,
                        cancellation_requested INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (run_id, step_id),
                        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
                    );

                    CREATE TABLE IF NOT EXISTS step_attempts (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        iteration INTEGER NOT NULL DEFAULT 1,
                        attempt INTEGER NOT NULL,
                        started_at TEXT NOT NULL,
                        completed_at TEXT,
                        outcome TEXT NOT NULL,
                        error TEXT,
                        candidate_output_json TEXT,
                        rejection_reason TEXT,
                        PRIMARY KEY (run_id, step_id, iteration, attempt),
                        FOREIGN KEY (run_id, step_id)
                            REFERENCES step_runs(run_id, step_id)
                    );

                    CREATE TABLE IF NOT EXISTS events (
                        run_id TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        event_type TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        step_id TEXT,
                        attempt INTEGER,
                        iteration INTEGER,
                        schema_version INTEGER NOT NULL DEFAULT 1,
                        data_json TEXT NOT NULL,
                        PRIMARY KEY (run_id, sequence),
                        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
                    );

                    CREATE TABLE IF NOT EXISTS runtime_sessions (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        runtime TEXT NOT NULL,
                        data_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (run_id, step_id, runtime),
                        FOREIGN KEY (run_id, step_id)
                            REFERENCES step_runs(run_id, step_id)
                    );

                    CREATE TABLE IF NOT EXISTS waits (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        iteration INTEGER NOT NULL,
                        attempt INTEGER NOT NULL,
                        kind TEXT NOT NULL,
                        signal_name TEXT,
                        wake_at TEXT,
                        timeout_at TEXT,
                        timeout_action TEXT NOT NULL,
                        payload_schema TEXT,
                        state TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        consumed_at TEXT,
                        PRIMARY KEY (run_id, step_id),
                        FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
                    );

                    CREATE TABLE IF NOT EXISTS signals (
                        run_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        actor_json TEXT,
                        received_at TEXT NOT NULL,
                        consumed_by_step_id TEXT,
                        consumed_at TEXT,
                        PRIMARY KEY (run_id, name, event_id),
                        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
                    );

                    CREATE TABLE IF NOT EXISTS human_tasks (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        requested_at TEXT NOT NULL,
                        responded_at TEXT,
                        actor_json TEXT,
                        comment TEXT,
                        payload_json TEXT,
                        timeout_at TEXT,
                        event_id TEXT,
                        PRIMARY KEY (run_id, step_id),
                        FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
                    );

                    CREATE TABLE IF NOT EXISTS idempotency_records (
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        iteration INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        status TEXT NOT NULL,
                        claim_token TEXT,
                        lease_expires_at TEXT,
                        result_json TEXT,
                        created_at TEXT NOT NULL,
                        completed_at TEXT,
                        PRIMARY KEY (run_id, step_id, iteration, action),
                        UNIQUE (idempotency_key),
                        FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
                    );

                    CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency_claim_token
                    ON idempotency_records(claim_token) WHERE claim_token IS NOT NULL;

                    CREATE INDEX IF NOT EXISTS idx_waits_due
                    ON waits(state, wake_at, timeout_at);
                    CREATE INDEX IF NOT EXISTS idx_waits_signal
                    ON waits(run_id, signal_name, state);
                    CREATE INDEX IF NOT EXISTS idx_signals_unconsumed
                    ON signals(run_id, name, received_at) WHERE consumed_at IS NULL;

                    CREATE TABLE IF NOT EXISTS agent_invocations (
                        invocation_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        iteration INTEGER NOT NULL,
                        runtime TEXT NOT NULL,
                        provider TEXT,
                        model TEXT,
                        status TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        completed_at TEXT,
                        input_tokens INTEGER,
                        output_tokens INTEGER,
                        cached_tokens INTEGER,
                        total_tokens INTEGER,
                        cost TEXT,
                        currency TEXT,
                        latency_seconds REAL,
                        error TEXT,
                        FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_agent_invocations_run
                    ON agent_invocations(run_id, step_id, started_at);

                    CREATE TABLE IF NOT EXISTS event_export_cursors (
                        run_id TEXT NOT NULL,
                        exporter TEXT NOT NULL,
                        last_sequence INTEGER NOT NULL DEFAULT 0,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (run_id, exporter),
                        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
                    );

                    CREATE TABLE IF NOT EXISTS workers (
                        worker_id TEXT PRIMARY KEY,
                        started_at TEXT NOT NULL,
                        heartbeat_at TEXT NOT NULL,
                        metadata_json TEXT NOT NULL DEFAULT '{}'
                    );

                    CREATE TABLE IF NOT EXISTS cycle_runs (
                        run_id TEXT NOT NULL,
                        cycle_name TEXT NOT NULL,
                        members_json TEXT NOT NULL,
                        controller_step_id TEXT NOT NULL,
                        entry_step_id TEXT NOT NULL,
                        max_iterations INTEGER NOT NULL,
                        iteration INTEGER NOT NULL DEFAULT 1,
                        status TEXT NOT NULL,
                        PRIMARY KEY (run_id, cycle_name),
                        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
                    );
                    """
                )
                workflow_columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(workflow_runs)").fetchall()
                }
                if "definition_format_version" not in workflow_columns:
                    connection.execute(
                        "ALTER TABLE workflow_runs ADD COLUMN "
                        "definition_format_version INTEGER NOT NULL DEFAULT 1"
                    )
                if "state_format_version" not in workflow_columns:
                    connection.execute(
                        "ALTER TABLE workflow_runs ADD COLUMN "
                        "state_format_version INTEGER NOT NULL DEFAULT 1"
                    )
                if "cancellation_reason" not in workflow_columns:
                    connection.execute(
                        "ALTER TABLE workflow_runs ADD COLUMN cancellation_reason TEXT"
                    )
                columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(step_runs)").fetchall()
                }
                if "inputs_json" not in columns:
                    connection.execute(
                        "ALTER TABLE step_runs "
                        "ADD COLUMN inputs_json TEXT NOT NULL DEFAULT '{}'"
                    )
                if "iteration" not in columns:
                    connection.execute(
                        "ALTER TABLE step_runs ADD COLUMN iteration INTEGER NOT NULL DEFAULT 1"
                    )
                for claim_column in (
                    "claim_owner TEXT",
                    "claim_token TEXT",
                    "claimed_at TEXT",
                    "lease_expires_at TEXT",
                    "heartbeat_at TEXT",
                    "available_at TEXT",
                    "signal_payload_json TEXT",
                    "cancellation_requested INTEGER NOT NULL DEFAULT 0",
                ):
                    column_name = claim_column.split()[0]
                    if column_name not in columns:
                        connection.execute(f"ALTER TABLE step_runs ADD COLUMN {claim_column}")
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_step_claim_token "
                    "ON step_runs(claim_token) WHERE claim_token IS NOT NULL"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_step_lease_expiry "
                    "ON step_runs(lease_expires_at) WHERE claim_token IS NOT NULL"
                )
                event_columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(events)").fetchall()
                }
                if "iteration" not in event_columns:
                    connection.execute("ALTER TABLE events ADD COLUMN iteration INTEGER")
                if "schema_version" not in event_columns:
                    connection.execute(
                        "ALTER TABLE events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
                    )
                attempt_columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(step_attempts)").fetchall()
                }
                if "iteration" not in attempt_columns:
                    connection.executescript(
                        """
                        ALTER TABLE step_attempts RENAME TO step_attempts_legacy;
                        CREATE TABLE step_attempts (
                            run_id TEXT NOT NULL,
                            step_id TEXT NOT NULL,
                            iteration INTEGER NOT NULL DEFAULT 1,
                            attempt INTEGER NOT NULL,
                            started_at TEXT NOT NULL,
                            completed_at TEXT,
                            outcome TEXT NOT NULL,
                            error TEXT,
                            candidate_output_json TEXT,
                            rejection_reason TEXT,
                            PRIMARY KEY (run_id, step_id, iteration, attempt),
                            FOREIGN KEY (run_id, step_id)
                                REFERENCES step_runs(run_id, step_id)
                        );
                        INSERT INTO step_attempts(
                            run_id, step_id, iteration, attempt, started_at, completed_at,
                            outcome, error, candidate_output_json, rejection_reason
                        )
                        SELECT run_id, step_id, 1, attempt, started_at, completed_at,
                            outcome, error, candidate_output_json, rejection_reason
                        FROM step_attempts_legacy;
                        DROP TABLE step_attempts_legacy;
                        """
                    )
                applied = {
                    int(row["version"]): (str(row["name"]), str(row["checksum"]))
                    for row in connection.execute(
                        "SELECT version, name, checksum FROM pac_schema_migrations"
                    ).fetchall()
                }
                for migration in MIGRATIONS:
                    recorded = applied.get(migration.version)
                    if recorded is not None and recorded != (
                        migration.name,
                        migration.checksum,
                    ):
                        raise StateStoreError(
                            f"SQLite migration {migration.version} does not match installed history"
                        )
                    if recorded is None:
                        connection.execute(
                            """
                            INSERT INTO pac_schema_migrations(version, name, checksum, applied_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                migration.version,
                                migration.name,
                                migration.checksum,
                                self._now(),
                            ),
                        )
        except sqlite3.Error as exc:
            raise StateStoreError(f"Could not initialize SQLite state at {self.path}: {exc}") from exc

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat(timespec="microseconds")

    @staticmethod
    def _json(value: Any) -> str:
        """Encode query-visible structural JSON, which is intentionally plaintext."""

        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)

    def _payload(self, value: JsonValue, *, aad: str) -> str:
        return self._payload_codec.encode(value, aad=aad)

    def _read_payload(self, value: str, *, aad: str) -> JsonValue:
        return self._payload_codec.decode(value, aad=aad)

    def _event_data(self, value: str, *, aad: str) -> dict[str, JsonValue]:
        payload = self._read_payload(value, aad=aad)
        if not isinstance(payload, dict):
            raise StateStoreError(f"Persisted event data at {aad} is not an object")
        return payload

    def _event(
        self,
        connection: Any,
        run_id: str,
        event_type: str,
        *,
        step_id: str | None = None,
        attempt: int | None = None,
        iteration: int | None = None,
        data: dict[str, JsonValue] | None = None,
    ) -> None:
        row = connection.execute(
            "SELECT next_event_sequence FROM workflow_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise StateStoreError(f"Unknown workflow run {run_id!r}")
        sequence = int(row["next_event_sequence"])
        connection.execute(
            "UPDATE workflow_runs SET next_event_sequence = ? WHERE run_id = ?",
            (sequence + 1, run_id),
        )
        connection.execute(
            """
            INSERT INTO events(
                run_id, sequence, event_type, timestamp, step_id, attempt, iteration,
                schema_version, data_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                sequence,
                event_type,
                self._now(),
                step_id,
                attempt,
                iteration,
                EVENT_SCHEMA_VERSION,
                self._payload(
                    sanitize_event_data(data or {}),
                    aad=f"events:data_json:{run_id}:{sequence}",
                ),
            ),
        )

    @contextmanager
    def execution_lock(self, workflow_name: str) -> Iterator[None]:
        """Compatibility no-op; run and step claims provide concurrency control."""

        del workflow_name
        yield

    def active_runs(self, workflow_name: str) -> tuple[WorkflowRun, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT run_id FROM workflow_runs
                WHERE workflow_name = ? AND status IN (?, ?, ?)
                ORDER BY created_at, run_id
                """,
                (
                    workflow_name,
                    WorkflowStatus.PENDING.value,
                    WorkflowStatus.RUNNING.value,
                    WorkflowStatus.WAITING.value,
                ),
            ).fetchall()
        return tuple(self.get_run(str(row["run_id"])) for row in rows)

    def active_run(self, workflow_name: str) -> WorkflowRun | None:
        runs = self.active_runs(workflow_name)
        if len(runs) > 1:
            raise ConcurrencyError(
                f"Workflow {workflow_name!r} has multiple active runs; specify a run ID"
            )
        return runs[0] if runs else None

    def list_runs(self, workflow_name: str | None = None) -> tuple[WorkflowRun, ...]:
        with self._connect() as connection:
            if workflow_name is None:
                rows = connection.execute(
                    "SELECT run_id FROM workflow_runs ORDER BY created_at, run_id"
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT run_id FROM workflow_runs
                    WHERE workflow_name = ? ORDER BY created_at, run_id
                    """,
                    (workflow_name,),
                ).fetchall()
        return tuple(self.get_run(str(row["run_id"])) for row in rows)

    def create_run(
        self, definition: WorkflowDefinition, *, run_id: str | None = None
    ) -> WorkflowRun:
        run_id = run_id or str(uuid4())
        if not isinstance(run_id, str) or not run_id.strip():
            raise StateStoreError("Workflow run ID must be a non-empty string")
        created_at = self._now()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT 1 FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if existing is not None:
                raise StateStoreError(f"Workflow run ID {run_id!r} already exists")
            connection.execute(
                """
                INSERT INTO workflow_runs(
                    run_id, workflow_name, definition_fingerprint, definition_json,
                    definition_format_version, state_format_version, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    definition.name,
                    definition.fingerprint,
                    definition.canonical_json,
                    DEFINITION_FORMAT_VERSION,
                    STATE_FORMAT_VERSION,
                    WorkflowStatus.PENDING.value,
                    created_at,
                ),
            )
            self._event(connection, run_id, "workflow.created")
            for step in definition.steps:
                connection.execute(
                    """
                    INSERT INTO step_runs(
                        run_id, step_id, registration_order, dependencies_json,
                        inputs_json, max_attempts, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        step.id,
                        step.registration_order,
                        self._json(list(step.dependencies)),
                        self._payload(
                            step.inputs, aad=f"step_runs:inputs_json:{run_id}:{step.id}"
                        ),
                        step.max_attempts,
                        StepStatus.PENDING.value,
                    ),
                )
                self._event(
                    connection,
                    run_id,
                    "step.registered",
                    step_id=step.id,
                    data={
                        "registration_order": step.registration_order,
                        "dependencies": list(step.dependencies),
                        "inputs": step.inputs,
                        "max_attempts": step.max_attempts,
                    },
                )
            for cycle in definition.cycles:
                connection.execute(
                    """
                    INSERT INTO cycle_runs(
                        run_id, cycle_name, members_json, controller_step_id,
                        entry_step_id, max_iterations, iteration, status
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        run_id,
                        cycle.name,
                        self._json(list(cycle.members)),
                        cycle.controller,
                        cycle.entry,
                        cycle.max_iterations,
                        CycleStatus.ACTIVE.value,
                    ),
                )
                self._event(
                    connection,
                    run_id,
                    "cycle.registered",
                    iteration=1,
                    data={
                        "name": cycle.name,
                        "members": list(cycle.members),
                        "controller": cycle.controller,
                        "entry": cycle.entry,
                        "max_iterations": cycle.max_iterations,
                    },
                )
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> WorkflowRun:
        with self._connect() as connection:
            run = connection.execute(
                "SELECT * FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise StateStoreError(f"Unknown workflow run {run_id!r}")
            step_rows = connection.execute(
                "SELECT * FROM step_runs WHERE run_id = ? ORDER BY registration_order",
                (run_id,),
            ).fetchall()
            event_rows = connection.execute(
                "SELECT * FROM events WHERE run_id = ? ORDER BY sequence", (run_id,)
            ).fetchall()
            cycle_rows = connection.execute(
                "SELECT * FROM cycle_runs WHERE run_id = ? ORDER BY cycle_name", (run_id,)
            ).fetchall()

        steps: dict[str, StepState] = {}
        outputs: dict[str, JsonValue] = {}
        for row in step_rows:
            output = (
                self._read_payload(
                    row["output_json"],
                    aad=f"step_runs:output_json:{run_id}:{row['step_id']}",
                )
                if row["output_json"] is not None
                else None
            )
            state = StepState(
                id=str(row["step_id"]),
                registration_order=int(row["registration_order"]),
                dependencies=tuple(json.loads(row["dependencies_json"])),
                max_attempts=int(row["max_attempts"]),
                status=StepStatus(row["status"]),
                attempt=int(row["attempt"]),
                inputs=self._read_payload(
                    row["inputs_json"],
                    aad=f"step_runs:inputs_json:{run_id}:{row['step_id']}",
                ),
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                error=row["error"],
                output=output,
                codex_thread_id=row["codex_thread_id"],
                retry_reason=row["retry_reason"],
                waiting_reason=row["waiting_reason"],
                iteration=int(row["iteration"]),
                has_output=row["output_json"] is not None,
                claim_owner=row["claim_owner"],
                claim_token=row["claim_token"],
                claimed_at=row["claimed_at"],
                lease_expires_at=row["lease_expires_at"],
                heartbeat_at=row["heartbeat_at"],
                available_at=row["available_at"],
                signal_payload=(
                    self._read_payload(
                        row["signal_payload_json"],
                        aad=f"step_runs:signal_payload_json:{run_id}:{row['step_id']}",
                    )
                    if row["signal_payload_json"] is not None
                    else None
                ),
                cancellation_requested=bool(row["cancellation_requested"]),
            )
            steps[state.id] = state
            if state.status is StepStatus.COMPLETED:
                outputs[state.id] = output

        events = tuple(
            WorkflowEvent(
                sequence=int(row["sequence"]),
                type=str(row["event_type"]),
                timestamp=str(row["timestamp"]),
                step_id=row["step_id"],
                attempt=row["attempt"],
                iteration=row["iteration"],
                data=self._event_data(
                    row["data_json"], aad=f"events:data_json:{run_id}:{row['sequence']}"
                ),
                schema_version=int(row["schema_version"]),
            )
            for row in event_rows
        )
        cycles = {
            str(row["cycle_name"]): CycleState(
                name=str(row["cycle_name"]),
                members=tuple(json.loads(row["members_json"])),
                controller=str(row["controller_step_id"]),
                entry=str(row["entry_step_id"]),
                max_iterations=int(row["max_iterations"]),
                iteration=int(row["iteration"]),
                status=CycleStatus(row["status"]),
            )
            for row in cycle_rows
        }
        return WorkflowRun(
            id=str(run["run_id"]),
            name=str(run["workflow_name"]),
            status=WorkflowStatus(run["status"]),
            definition_fingerprint=str(run["definition_fingerprint"]),
            created_at=str(run["created_at"]),
            started_at=run["started_at"],
            completed_at=run["completed_at"],
            error=run["error"],
            steps=steps,
            outputs=outputs,
            events=events,
            cycles=cycles,
            cancellation_reason=run["cancellation_reason"],
        )

    def start_workflow(self, run_id: str) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown workflow run {run_id!r}")
            status = WorkflowStatus(row["status"])
            if status is WorkflowStatus.RUNNING:
                return
            if status is not WorkflowStatus.PENDING:
                raise StateStoreError(f"Cannot start workflow {run_id} from {status.value}")
            connection.execute(
                "UPDATE workflow_runs SET status = ?, started_at = ? WHERE run_id = ?",
                (WorkflowStatus.RUNNING.value, self._now(), run_id),
            )
            self._event(connection, run_id, "workflow.started")

    def resume_waiting(self, run_id: str) -> None:
        """Resume only legacy unconditioned waits.

        Signal, timer, and human waits are resumed by their durable condition.
        """
        with self._transaction() as connection:
            run = connection.execute(
                "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or WorkflowStatus(run["status"]) is not WorkflowStatus.WAITING:
                raise StateStoreError(f"Workflow {run_id} is not waiting")
            rows = connection.execute(
                """
                SELECT s.step_id, s.attempt, s.iteration
                FROM step_runs AS s
                LEFT JOIN waits AS w ON w.run_id = s.run_id AND w.step_id = s.step_id
                WHERE s.run_id = ? AND s.status = ?
                  AND (w.kind IS NULL OR w.kind = ?)
                ORDER BY s.registration_order
                """,
                (run_id, StepStatus.WAITING.value, WaitKind.LEGACY.value),
            ).fetchall()
            if not rows:
                return
            for row in rows:
                self._resume_step_tx(connection, run_id, row, payload=None, source="manual")
            connection.execute(
                "UPDATE workflow_runs SET status = ? WHERE run_id = ?",
                (WorkflowStatus.RUNNING.value, run_id),
            )
            self._event(connection, run_id, "workflow.resumed", data={"source": "manual"})

    def _resume_step_tx(
        self,
        connection: Any,
        run_id: str,
        row: sqlite3.Row,
        *,
        payload: JsonValue,
        source: str,
    ) -> None:
        connection.execute(
            """
            UPDATE step_runs SET status = ?, waiting_reason = NULL,
                signal_payload_json = ?, claim_owner = NULL, claim_token = NULL,
                claimed_at = NULL, lease_expires_at = NULL, heartbeat_at = NULL
            WHERE run_id = ? AND step_id = ?
            """,
            (
                StepStatus.PENDING.value,
                (
                    self._payload(
                        payload,
                        aad=f"step_runs:signal_payload_json:{run_id}:{row['step_id']}",
                    )
                    if payload is not None
                    else None
                ),
                run_id,
                row["step_id"],
            ),
        )
        connection.execute(
            """
            UPDATE waits SET state = 'CONSUMED', consumed_at = ?
            WHERE run_id = ? AND step_id = ?
            """,
            (self._now(), run_id, row["step_id"]),
        )
        self._event(
            connection,
            run_id,
            "step.resumed",
            step_id=str(row["step_id"]),
            attempt=int(row["attempt"]),
            iteration=int(row["iteration"]),
            data={"source": source},
        )

    def recover_running(self, run_id: str) -> None:
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT step_id, attempt, max_attempts, iteration FROM step_runs
                WHERE run_id = ? AND status = ? AND claim_token IS NULL
                ORDER BY registration_order
                """,
                (run_id, StepStatus.RUNNING.value),
            ).fetchall()
            workflow_failed = False
            for row in rows:
                step_id = str(row["step_id"])
                attempt = int(row["attempt"])
                iteration = int(row["iteration"])
                reason = "Previous process stopped while the step was RUNNING"
                can_retry = attempt < int(row["max_attempts"])
                target = StepStatus.RETRY if can_retry else StepStatus.FAILED
                connection.execute(
                    """
                    UPDATE step_runs SET status = ?, error = ?, retry_reason = ?,
                        claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                        lease_expires_at = NULL, heartbeat_at = NULL
                    WHERE run_id = ? AND step_id = ?
                    """,
                    (target.value, None if can_retry else reason, reason, run_id, step_id),
                )
                connection.execute(
                    """
                    UPDATE step_attempts SET completed_at = ?, outcome = ?, error = ?
                    WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                    """,
                    (self._now(), "INTERRUPTED", reason, run_id, step_id, iteration, attempt),
                )
                self._event(
                    connection,
                    run_id,
                    "step.recovered",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"reason": reason},
                )
                if can_retry:
                    self._event(
                        connection,
                        run_id,
                        "step.retry_requested",
                        step_id=step_id,
                        attempt=attempt,
                        iteration=iteration,
                        data={"reason": reason},
                    )
                else:
                    workflow_failed = True
                    self._event(
                        connection,
                        run_id,
                        "step.failed",
                        step_id=step_id,
                        attempt=attempt,
                        iteration=iteration,
                        data={"error": reason},
                    )
            if workflow_failed:
                self._fail_workflow_tx(connection, run_id, "Interrupted step exhausted attempts")

    def claim_step(
        self,
        run_id: str,
        step_id: str,
        worker_id: str,
        *,
        lease_duration: timedelta,
    ) -> StepClaim:
        if lease_duration.total_seconds() <= 0:
            raise StateStoreError("Step claim lease duration must be positive")
        claimed_at = self._now()
        lease_expires_at = (
            datetime.fromisoformat(claimed_at) + lease_duration
        ).isoformat(timespec="microseconds")
        token = str(uuid4())
        with self._transaction() as connection:
            run = connection.execute(
                "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or WorkflowStatus(run["status"]) is not WorkflowStatus.RUNNING:
                raise ConcurrencyError(f"Workflow run {run_id!r} is not RUNNING")
            row = connection.execute(
                "SELECT * FROM step_runs WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown step {step_id!r} in run {run_id}")
            previous = StepStatus(row["status"])
            if previous not in (StepStatus.PENDING, StepStatus.RETRY):
                raise ConcurrencyError(f"Cannot claim step {step_id} from {previous.value}")
            old_attempt = int(row["attempt"])
            attempt = old_attempt + 1 if previous is StepStatus.RETRY or old_attempt == 0 else old_attempt
            if attempt > int(row["max_attempts"]):
                raise ConcurrencyError(f"Step {step_id} has exhausted its attempts")
            available_at = row["available_at"]
            if available_at is not None and available_at > claimed_at:
                raise ConcurrencyError(f"Step {step_id} is not available until {available_at}")
            iteration = int(row["iteration"])
            updated = connection.execute(
                """
                UPDATE step_runs SET status = ?, attempt = ?, started_at = ?,
                    completed_at = NULL, error = NULL, waiting_reason = NULL,
                    claim_owner = ?, claim_token = ?, claimed_at = ?,
                    lease_expires_at = ?, heartbeat_at = ?
                WHERE run_id = ? AND step_id = ?
                    AND status = ? AND claim_token IS NULL
                """,
                (
                    StepStatus.RUNNING.value,
                    attempt,
                    claimed_at,
                    worker_id,
                    token,
                    claimed_at,
                    lease_expires_at,
                    claimed_at,
                    run_id,
                    step_id,
                    previous.value,
                ),
            ).rowcount
            if updated != 1:
                raise ConcurrencyError(f"Step {step_id} was claimed by another worker")
            existing = connection.execute(
                """
                SELECT 1 FROM step_attempts
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (run_id, step_id, iteration, attempt),
            ).fetchone()
            if existing:
                connection.execute(
                    """
                    UPDATE step_attempts SET started_at = ?, completed_at = NULL,
                        outcome = ?, error = NULL
                    WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                    """,
                    (claimed_at, StepStatus.RUNNING.value, run_id, step_id, iteration, attempt),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO step_attempts(
                        run_id, step_id, iteration, attempt, started_at, outcome
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, step_id, iteration, attempt, claimed_at, StepStatus.RUNNING.value),
                )
            self._event(
                connection, run_id, "step.runnable", step_id=step_id, attempt=attempt,
                iteration=iteration,
            )
            self._event(
                connection, run_id, "step.claimed", step_id=step_id, attempt=attempt,
                iteration=iteration, data={"worker_id": worker_id, "lease_expires_at": lease_expires_at},
            )
            self._event(
                connection, run_id, "step.started", step_id=step_id, attempt=attempt,
                iteration=iteration, data={"worker_id": worker_id},
            )
        return StepClaim(
            run_id, step_id, worker_id, token, attempt, iteration,
            claimed_at, lease_expires_at,
        )

    def heartbeat_claim(
        self, token: str, *, lease_duration: timedelta
    ) -> StepClaim:
        if lease_duration.total_seconds() <= 0:
            raise StateStoreError("Step claim lease duration must be positive")
        heartbeat_at = self._now()
        lease_expires_at = (
            datetime.fromisoformat(heartbeat_at) + lease_duration
        ).isoformat(timespec="microseconds")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM step_runs WHERE claim_token = ? AND status = ?",
                (token, StepStatus.RUNNING.value),
            ).fetchone()
            if row is None or row["lease_expires_at"] <= heartbeat_at:
                raise ConcurrencyError("Step claim is missing or expired")
            connection.execute(
                "UPDATE step_runs SET heartbeat_at = ?, lease_expires_at = ? WHERE claim_token = ?",
                (heartbeat_at, lease_expires_at, token),
            )
        return StepClaim(
            str(row["run_id"]), str(row["step_id"]), str(row["claim_owner"]), token,
            int(row["attempt"]), int(row["iteration"]), str(row["claimed_at"]),
            lease_expires_at,
        )

    def recover_expired_claims(self) -> tuple[StepClaim, ...]:
        now = self._now()
        recovered: list[StepClaim] = []
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT * FROM step_runs
                WHERE status = ? AND claim_token IS NOT NULL AND lease_expires_at <= ?
                ORDER BY run_id, registration_order
                """,
                (StepStatus.RUNNING.value, now),
            ).fetchall()
            for row in rows:
                claim = StepClaim(
                    str(row["run_id"]), str(row["step_id"]), str(row["claim_owner"]),
                    str(row["claim_token"]), int(row["attempt"]), int(row["iteration"]),
                    str(row["claimed_at"]), str(row["lease_expires_at"]),
                )
                can_retry = claim.attempt < int(row["max_attempts"])
                target = StepStatus.RETRY if can_retry else StepStatus.FAILED
                reason = "Step claim lease expired before completion"
                connection.execute(
                    """
                    UPDATE step_runs SET status = ?, error = ?, retry_reason = ?,
                        claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                        lease_expires_at = NULL, heartbeat_at = NULL
                    WHERE run_id = ? AND step_id = ? AND claim_token = ?
                    """,
                    (target.value, None if can_retry else reason, reason,
                     claim.run_id, claim.step_id, claim.token),
                )
                connection.execute(
                    """
                    UPDATE step_attempts SET completed_at = ?, outcome = ?, error = ?
                    WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                    """,
                    (now, "INTERRUPTED", reason, claim.run_id, claim.step_id,
                     claim.iteration, claim.attempt),
                )
                self._event(
                    connection, claim.run_id, "step.lease_expired", step_id=claim.step_id,
                    attempt=claim.attempt, iteration=claim.iteration,
                    data={"worker_id": claim.worker_id},
                )
                if not can_retry:
                    self._fail_workflow_tx(
                        connection, claim.run_id, f"Step {claim.step_id} failed: {reason}"
                    )
                recovered.append(claim)
        return tuple(recovered)

    def interrupt_claim(self, token: str, reason: str) -> None:
        now = self._now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM step_runs WHERE claim_token = ? AND status = ?",
                (token, StepStatus.RUNNING.value),
            ).fetchone()
            if row is None:
                return
            can_retry = int(row["attempt"]) < int(row["max_attempts"])
            target = StepStatus.RETRY if can_retry else StepStatus.FAILED
            connection.execute(
                """
                UPDATE step_runs SET status = ?, error = ?, retry_reason = ?,
                    claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                    lease_expires_at = NULL, heartbeat_at = NULL
                WHERE claim_token = ?
                """,
                (target.value, None if can_retry else reason, reason, token),
            )
            connection.execute(
                """
                UPDATE step_attempts SET completed_at = ?, outcome = ?, error = ?
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (now, "INTERRUPTED", reason, row["run_id"], row["step_id"],
                 row["iteration"], row["attempt"]),
            )
            self._event(
                connection, str(row["run_id"]), "step.recovered",
                step_id=str(row["step_id"]), attempt=int(row["attempt"]),
                iteration=int(row["iteration"]), data={"reason": reason},
            )
            if can_retry:
                self._event(
                    connection, str(row["run_id"]), "step.retry_requested",
                    step_id=str(row["step_id"]), attempt=int(row["attempt"]),
                    iteration=int(row["iteration"]), data={"reason": reason},
                )
            else:
                self._event(
                    connection, str(row["run_id"]), "step.failed",
                    step_id=str(row["step_id"]), attempt=int(row["attempt"]),
                    iteration=int(row["iteration"]), data={"error": reason},
                )
                self._fail_workflow_tx(
                    connection, str(row["run_id"]),
                    f"Step {row['step_id']} failed: {reason}",
                )

    def start_step(self, run_id: str, step_id: str) -> StepState:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM step_runs WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown step {step_id!r} in run {run_id}")
            previous = StepStatus(row["status"])
            if previous not in (StepStatus.PENDING, StepStatus.RETRY):
                raise StateStoreError(f"Cannot start step {step_id} from {previous.value}")
            old_attempt = int(row["attempt"])
            iteration = int(row["iteration"])
            attempt = old_attempt + 1 if previous is StepStatus.RETRY or old_attempt == 0 else old_attempt
            if attempt > int(row["max_attempts"]) and previous is StepStatus.RETRY:
                raise StateStoreError(f"Step {step_id} has exhausted its attempts")
            started_at = self._now()
            connection.execute(
                """
                UPDATE step_runs SET status = ?, attempt = ?, started_at = ?,
                    completed_at = NULL, error = NULL, waiting_reason = NULL
                WHERE run_id = ? AND step_id = ?
                """,
                (StepStatus.RUNNING.value, attempt, started_at, run_id, step_id),
            )
            existing = connection.execute(
                """
                SELECT 1 FROM step_attempts
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (run_id, step_id, iteration, attempt),
            ).fetchone()
            if existing:
                connection.execute(
                    """
                    UPDATE step_attempts SET started_at = ?, completed_at = NULL,
                        outcome = ?, error = NULL
                    WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                    """,
                    (started_at, StepStatus.RUNNING.value, run_id, step_id, iteration, attempt),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO step_attempts(
                        run_id, step_id, iteration, attempt, started_at, outcome
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, step_id, iteration, attempt, started_at, StepStatus.RUNNING.value),
                )
            self._event(
                connection, run_id, "step.started", step_id=step_id, attempt=attempt,
                iteration=iteration,
            )
        return self.get_run(run_id).steps[step_id]

    def complete_step(
        self, run_id: str, step_id: str, output: JsonValue, *, claim_token: str | None = None
    ) -> None:
        output_json = self._payload(
            output, aad=f"step_runs:output_json:{run_id}:{step_id}"
        )
        completed_at = self._now()
        with self._transaction() as connection:
            iteration, attempt = self._running_attempt(
                connection, run_id, step_id, claim_token=claim_token
            )
            connection.execute(
                """
                UPDATE step_runs SET status = ?, output_json = ?, completed_at = ?,
                    error = NULL, retry_reason = NULL, waiting_reason = NULL,
                    claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                    lease_expires_at = NULL, heartbeat_at = NULL
                WHERE run_id = ? AND step_id = ?
                """,
                (StepStatus.COMPLETED.value, output_json, completed_at, run_id, step_id),
            )
            connection.execute(
                """
                UPDATE step_attempts SET completed_at = ?, outcome = ?,
                    candidate_output_json = ?
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (
                    completed_at,
                    StepStatus.COMPLETED.value,
                    output_json,
                    run_id,
                    step_id,
                    iteration,
                    attempt,
                ),
            )
            self._event(
                connection,
                run_id,
                "validation.accepted",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
            )
            self._event(
                connection, run_id, "step.completed", step_id=step_id, attempt=attempt,
                iteration=iteration,
            )
            cycle = connection.execute(
                """
                SELECT cycle_name FROM cycle_runs
                WHERE run_id = ? AND controller_step_id = ? AND status = ?
                """,
                (run_id, step_id, CycleStatus.ACTIVE.value),
            ).fetchone()
            if cycle is not None:
                connection.execute(
                    "UPDATE cycle_runs SET status = ? WHERE run_id = ? AND cycle_name = ?",
                    (CycleStatus.COMPLETED.value, run_id, cycle["cycle_name"]),
                )
                self._event(
                    connection,
                    run_id,
                    "cycle.completed",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"name": str(cycle["cycle_name"])},
                )

    def repeat_cycle(
        self,
        run_id: str,
        cycle_name: str,
        step_id: str,
        output: JsonValue,
        reason: str | None,
        *,
        claim_token: str | None = None,
    ) -> bool:
        output_json = self._payload(
            output, aad=f"step_runs:output_json:{run_id}:{step_id}"
        )
        completed_at = self._now()
        with self._transaction() as connection:
            cycle = connection.execute(
                "SELECT * FROM cycle_runs WHERE run_id = ? AND cycle_name = ?",
                (run_id, cycle_name),
            ).fetchone()
            if cycle is None:
                raise StateStoreError(f"Unknown cycle {cycle_name!r} in run {run_id}")
            if CycleStatus(cycle["status"]) is not CycleStatus.ACTIVE:
                raise StateStoreError(f"Cycle {cycle_name!r} is not active")
            if str(cycle["controller_step_id"]) != step_id:
                raise StateStoreError(f"Step {step_id} is not the controller of cycle {cycle_name!r}")
            iteration, attempt = self._running_attempt(
                connection, run_id, step_id, claim_token=claim_token
            )
            current_iteration = int(cycle["iteration"])
            if iteration != current_iteration:
                raise StateStoreError(
                    f"Cycle {cycle_name!r} and controller iteration are inconsistent"
                )
            connection.execute(
                """
                UPDATE step_attempts SET completed_at = ?, outcome = ?, error = ?,
                    candidate_output_json = ?
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (
                    completed_at,
                    StepStatus.REPEAT.value,
                    reason,
                    output_json,
                    run_id,
                    step_id,
                    iteration,
                    attempt,
                ),
            )
            connection.execute(
                "UPDATE step_runs SET output_json = ? WHERE run_id = ? AND step_id = ?",
                (output_json, run_id, step_id),
            )
            self._event(
                connection,
                run_id,
                "step.repeated",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
                data={"cycle": cycle_name, "reason": reason, "output": output},
            )
            if current_iteration >= int(cycle["max_iterations"]):
                error = (
                    f"Cycle {cycle_name!r} exceeded its maximum of "
                    f"{int(cycle['max_iterations'])} iterations"
                )
                connection.execute(
                    """
                    UPDATE step_attempts SET outcome = ?, error = ?
                    WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                    """,
                    (StepStatus.FAILED.value, error, run_id, step_id, iteration, attempt),
                )
                connection.execute(
                    """
                    UPDATE step_runs SET status = ?, error = ?, completed_at = ?,
                        claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                        lease_expires_at = NULL, heartbeat_at = NULL
                    WHERE run_id = ? AND step_id = ?
                    """,
                    (StepStatus.FAILED.value, error, completed_at, run_id, step_id),
                )
                self._event(
                    connection,
                    run_id,
                    "step.failed",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"error": error},
                )
                self._event(
                    connection,
                    run_id,
                    "cycle.limit_exceeded",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"name": cycle_name, "max_iterations": int(cycle["max_iterations"])},
                )
                self._fail_workflow_tx(connection, run_id, error)
                return False

            next_iteration = current_iteration + 1
            members = tuple(json.loads(cycle["members_json"]))
            placeholders = ",".join("?" for _ in members)
            connection.execute(
                f"""
                UPDATE step_runs SET status = ?, attempt = 0, iteration = ?,
                    started_at = NULL, completed_at = NULL, error = NULL,
                    retry_reason = NULL, waiting_reason = NULL,
                    claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                    lease_expires_at = NULL, heartbeat_at = NULL
                WHERE run_id = ? AND step_id IN ({placeholders})
                """,
                (StepStatus.PENDING.value, next_iteration, run_id, *members),
            )
            connection.execute(
                "UPDATE cycle_runs SET iteration = ? WHERE run_id = ? AND cycle_name = ?",
                (next_iteration, run_id, cycle_name),
            )
            self._event(
                connection,
                run_id,
                "cycle.repeated",
                step_id=step_id,
                attempt=attempt,
                iteration=next_iteration,
                data={
                    "name": cycle_name,
                    "from_iteration": current_iteration,
                    "to_iteration": next_iteration,
                    "reason": reason,
                },
            )
            return True

    def retry_step(
        self,
        run_id: str,
        step_id: str,
        reason: str,
        *,
        candidate: JsonValue = None,
        rejected: bool = False,
        claim_token: str | None = None,
    ) -> bool:
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT status, attempt, max_attempts, iteration, claim_token FROM step_runs
                WHERE run_id = ? AND step_id = ?
                """,
                (run_id, step_id),
            ).fetchone()
            if row is None or StepStatus(row["status"]) is not StepStatus.RUNNING:
                raise StateStoreError(f"Step {step_id} is not RUNNING")
            if claim_token is not None and row["claim_token"] != claim_token:
                raise ConcurrencyError(f"Claim for step {step_id} is no longer valid")
            attempt = int(row["attempt"])
            iteration = int(row["iteration"])
            can_retry = attempt < int(row["max_attempts"])
            completed_at = self._now()
            candidate_json = (
                self._payload(
                    candidate,
                    aad=(
                        f"step_attempts:candidate_output_json:{run_id}:{step_id}:"
                        f"{iteration}:{attempt}"
                    ),
                )
                if rejected
                else None
            )
            outcome = "REJECTED" if rejected else StepStatus.RETRY.value
            connection.execute(
                """
                UPDATE step_attempts SET completed_at = ?, outcome = ?, error = ?,
                    candidate_output_json = ?, rejection_reason = ?
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (
                    completed_at,
                    outcome,
                    reason,
                    candidate_json,
                    reason if rejected else None,
                    run_id,
                    step_id,
                    iteration,
                    attempt,
                ),
            )
            if rejected:
                self._event(
                    connection,
                    run_id,
                    "validation.rejected",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"reason": reason},
                )
                self._event(
                    connection,
                    run_id,
                    "step.output_rejected",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"reason": reason, "candidate": candidate},
                )
            if can_retry:
                connection.execute(
                    """
                    UPDATE step_runs SET status = ?, retry_reason = ?, error = NULL,
                        claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                        lease_expires_at = NULL, heartbeat_at = NULL
                    WHERE run_id = ? AND step_id = ?
                    """,
                    (StepStatus.RETRY.value, reason, run_id, step_id),
                )
                self._event(
                    connection,
                    run_id,
                    "step.retry_requested",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"reason": reason},
                )
                return True

            connection.execute(
                """
                UPDATE step_runs SET status = ?, error = ?, retry_reason = ?,
                    completed_at = ?, claim_owner = NULL, claim_token = NULL,
                    claimed_at = NULL, lease_expires_at = NULL, heartbeat_at = NULL
                WHERE run_id = ? AND step_id = ?
                """,
                (StepStatus.FAILED.value, reason, reason, completed_at, run_id, step_id),
            )
            self._event(
                connection,
                run_id,
                "step.failed",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
                data={"error": reason},
            )
            self._fail_workflow_tx(connection, run_id, f"Step {step_id} failed: {reason}")
            return False

    def wait_step(
        self,
        run_id: str,
        step_id: str,
        reason: str | None,
        *,
        claim_token: str | None = None,
        request: WaitRequest | None = None,
    ) -> None:
        completed_at = self._now()
        request = request or WaitRequest(WaitKind.LEGACY, reason=reason)
        wake_at = request.wake_at.astimezone(UTC).isoformat(timespec="microseconds") if request.wake_at else None
        timeout_at = request.timeout_at.astimezone(UTC).isoformat(timespec="microseconds") if request.timeout_at else None
        with self._transaction() as connection:
            iteration, attempt = self._running_attempt(
                connection, run_id, step_id, claim_token=claim_token
            )
            connection.execute(
                """
                UPDATE step_runs SET status = ?, waiting_reason = ?, signal_payload_json = NULL,
                    claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                    lease_expires_at = NULL, heartbeat_at = NULL
                WHERE run_id = ? AND step_id = ?
                """,
                (StepStatus.WAITING.value, reason, run_id, step_id),
            )
            connection.execute(
                """
                INSERT INTO waits(
                    run_id, step_id, iteration, attempt, kind, signal_name, wake_at,
                    timeout_at, timeout_action, payload_schema, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?)
                ON CONFLICT(run_id, step_id) DO UPDATE SET
                    iteration=excluded.iteration, attempt=excluded.attempt, kind=excluded.kind,
                    signal_name=excluded.signal_name, wake_at=excluded.wake_at,
                    timeout_at=excluded.timeout_at, timeout_action=excluded.timeout_action,
                    payload_schema=excluded.payload_schema, state='WAITING',
                    created_at=excluded.created_at, consumed_at=NULL
                """,
                (
                    run_id, step_id, iteration, attempt, request.kind.value,
                    request.signal, wake_at, timeout_at, request.timeout_action.value,
                    self._json(schema_spec(request.payload_type)), completed_at,
                ),
            )
            connection.execute(
                """
                UPDATE step_attempts SET completed_at = ?, outcome = ?
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (completed_at, StepStatus.WAITING.value, run_id, step_id, iteration, attempt),
            )
            event_type = "timer.scheduled" if request.kind is WaitKind.TIMER else "step.waiting"
            self._event(
                connection,
                run_id,
                event_type,
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
                data={
                    "reason": reason,
                    "kind": request.kind.value,
                    "signal": request.signal,
                    "wake_at": wake_at,
                    "timeout_at": timeout_at,
                    "timeout_action": request.timeout_action.value,
                },
            )
            if request.kind is WaitKind.HUMAN:
                connection.execute(
                    """
                    INSERT INTO human_tasks(run_id, step_id, status, requested_at, timeout_at)
                    VALUES (?, ?, 'PENDING', ?, ?)
                    ON CONFLICT(run_id, step_id) DO UPDATE SET status='PENDING',
                        requested_at=excluded.requested_at, timeout_at=excluded.timeout_at,
                        responded_at=NULL, actor_json=NULL, comment=NULL,
                        payload_json=NULL, event_id=NULL
                    """,
                    (run_id, step_id, completed_at, timeout_at),
                )
                self._event(
                    connection, run_id, "human.approval_requested", step_id=step_id,
                    attempt=attempt, iteration=iteration, data={"timeout_at": timeout_at},
                )
            if request.signal:
                queued = connection.execute(
                    """
                    SELECT * FROM signals WHERE run_id=? AND name=? AND consumed_at IS NULL
                    ORDER BY received_at, event_id LIMIT 1
                    """,
                    (run_id, request.signal),
                ).fetchone()
                if queued is not None:
                    queued_payload = self._read_payload(
                        queued["payload_json"],
                        aad=(
                            f"signals:payload_json:{run_id}:{request.signal}:"
                            f"{queued['event_id']}"
                        ),
                    )
                    try:
                        validate_schema_spec(
                            queued_payload,
                            schema_spec(request.payload_type),
                            path=f"signal {request.signal!r} payload",
                        )
                    except ValidationError:
                        queued = None
                if queued is not None:
                    connection.execute(
                        """
                        UPDATE signals SET consumed_by_step_id=?, consumed_at=?
                        WHERE run_id=? AND name=? AND event_id=?
                        """,
                        (step_id, completed_at, run_id, request.signal, queued["event_id"]),
                    )
                    resume_row = connection.execute(
                        "SELECT * FROM waits WHERE run_id=? AND step_id=?",
                        (run_id, step_id),
                    ).fetchone()
                    self._resume_step_tx(
                        connection, run_id, resume_row,
                        payload=queued_payload,
                        source=f"signal:{request.signal}",
                    )
                    self._event(
                        connection, run_id, "signal.consumed", step_id=step_id,
                        attempt=attempt, iteration=iteration,
                        data={"name": request.signal, "event_id": queued["event_id"]},
                    )

    def signal(
        self,
        run_id: str,
        name: str,
        payload: JsonValue = None,
        *,
        event_id: str | None = None,
        actor: JsonValue = None,
    ) -> SignalReceipt:
        if not isinstance(name, str) or not name.strip():
            raise StateStoreError("Signal name must be a non-empty string")
        event_id = event_id or str(uuid4())
        received_at = self._now()
        payload_json = self._payload(
            payload, aad=f"signals:payload_json:{run_id}:{name}:{event_id}"
        )
        actor_json = (
            self._payload(actor, aad=f"signals:actor_json:{run_id}:{name}:{event_id}")
            if actor is not None
            else None
        )
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM signals WHERE run_id = ? AND name = ? AND event_id = ?",
                (run_id, name, event_id),
            ).fetchone()
            if existing is not None:
                return SignalReceipt(
                    run_id, name, event_id, True,
                    existing["consumed_at"] is not None,
                    self._read_payload(
                        existing["payload_json"],
                        aad=f"signals:payload_json:{run_id}:{name}:{event_id}",
                    ),
                )
            run = connection.execute(
                "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise StateStoreError(f"Unknown workflow run {run_id!r}")
            if WorkflowStatus(run["status"]) in (
                WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED
            ):
                raise StateStoreError(f"Workflow run {run_id!r} is terminal")
            connection.execute(
                """
                INSERT INTO signals(run_id, name, event_id, payload_json, actor_json, received_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, name, event_id, payload_json, actor_json, received_at),
            )
            self._event(
                connection, run_id, "signal.received",
                data={"name": name, "event_id": event_id, "actor": actor},
            )
            wait = connection.execute(
                """
                SELECT w.*, s.registration_order FROM waits AS w
                JOIN step_runs AS s ON s.run_id=w.run_id AND s.step_id=w.step_id
                WHERE w.run_id = ? AND w.signal_name = ? AND w.state = 'WAITING'
                ORDER BY s.registration_order LIMIT 1
                """,
                (run_id, name),
            ).fetchone()
            consumed = wait is not None
            if wait is not None:
                try:
                    validate_schema_spec(
                        payload,
                        json.loads(wait["payload_schema"] or '{"type":"any"}'),
                        path=f"signal {name!r} payload",
                    )
                except ValidationError as exc:
                    raise StateStoreError(str(exc)) from exc
                connection.execute(
                    """
                    UPDATE signals SET consumed_by_step_id = ?, consumed_at = ?
                    WHERE run_id = ? AND name = ? AND event_id = ?
                    """,
                    (wait["step_id"], received_at, run_id, name, event_id),
                )
                self._resume_step_tx(
                    connection, run_id, wait, payload=payload, source=f"signal:{name}"
                )
                connection.execute(
                    "UPDATE workflow_runs SET status = ? WHERE run_id = ?",
                    (WorkflowStatus.RUNNING.value, run_id),
                )
                self._event(
                    connection, run_id, "signal.consumed", step_id=str(wait["step_id"]),
                    attempt=int(wait["attempt"]), iteration=int(wait["iteration"]),
                    data={"name": name, "event_id": event_id},
                )
        return SignalReceipt(run_id, name, event_id, False, consumed, payload)

    def process_due_waits(self) -> tuple[str, ...]:
        now = self._now()
        resumed: list[str] = []
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT w.*, s.registration_order FROM waits AS w
                JOIN step_runs AS s ON s.run_id=w.run_id AND s.step_id=w.step_id
                WHERE w.state='WAITING' AND (
                    (w.kind = ? AND w.wake_at IS NOT NULL AND w.wake_at <= ?)
                    OR (w.timeout_at IS NOT NULL AND w.timeout_at <= ?)
                ) ORDER BY COALESCE(w.wake_at, w.timeout_at), w.run_id, s.registration_order
                """,
                (WaitKind.TIMER.value, now, now),
            ).fetchall()
            for row in rows:
                is_timer = row["kind"] == WaitKind.TIMER.value and row["wake_at"] and row["wake_at"] <= now
                action = TimeoutAction.RESUME if is_timer else TimeoutAction(row["timeout_action"])
                run_id, step_id = str(row["run_id"]), str(row["step_id"])
                if action is TimeoutAction.RESUME:
                    payload: JsonValue = None
                    if row["kind"] == WaitKind.HUMAN.value:
                        payload = {"decision": "timed_out", "payload": None, "comment": None, "actor": None}
                        connection.execute(
                            """
                            UPDATE human_tasks SET status='TIMED_OUT', responded_at=?
                            WHERE run_id=? AND step_id=?
                            """,
                            (now, run_id, step_id),
                        )
                        self._event(
                            connection,
                            run_id,
                            "human.approval_timed_out",
                            step_id=step_id,
                            attempt=int(row["attempt"]),
                            iteration=int(row["iteration"]),
                        )
                    self._resume_step_tx(connection, run_id, row, payload=payload, source="timer")
                    connection.execute(
                        "UPDATE workflow_runs SET status = ? WHERE run_id = ?",
                        (WorkflowStatus.RUNNING.value, run_id),
                    )
                    self._event(
                        connection, run_id, "timer.fired", step_id=step_id,
                        attempt=int(row["attempt"]), iteration=int(row["iteration"]),
                    )
                elif action is TimeoutAction.RETRY:
                    connection.execute(
                        "UPDATE step_runs SET status=?, retry_reason=?, waiting_reason=NULL WHERE run_id=? AND step_id=?",
                        (StepStatus.RETRY.value, "Wait timed out", run_id, step_id),
                    )
                    connection.execute(
                        "UPDATE waits SET state='TIMED_OUT', consumed_at=? WHERE run_id=? AND step_id=?",
                        (now, run_id, step_id),
                    )
                    connection.execute(
                        "UPDATE workflow_runs SET status=? WHERE run_id=?",
                        (WorkflowStatus.RUNNING.value, run_id),
                    )
                    self._event(connection, run_id, "step.timeout", step_id=step_id, data={"action": "retry"})
                elif action is TimeoutAction.CANCEL:
                    self._cancel_run_tx(connection, run_id, "Wait timed out", actor=None)
                else:
                    connection.execute(
                        "UPDATE step_runs SET status=?, error=?, completed_at=? WHERE run_id=? AND step_id=?",
                        (StepStatus.FAILED.value, "Wait timed out", now, run_id, step_id),
                    )
                    connection.execute(
                        "UPDATE waits SET state='TIMED_OUT', consumed_at=? WHERE run_id=? AND step_id=?",
                        (now, run_id, step_id),
                    )
                    self._event(connection, run_id, "step.timeout", step_id=step_id, data={"action": "fail"})
                    self._fail_workflow_tx(connection, run_id, f"Step {step_id} timed out")
                resumed.append(run_id)
        return tuple(dict.fromkeys(resumed))

    def ready_runs(self) -> tuple[str, ...]:
        self.process_due_waits()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT run_id FROM workflow_runs
                WHERE status IN (?, ?) ORDER BY created_at, run_id
                """,
                (WorkflowStatus.PENDING.value, WorkflowStatus.RUNNING.value),
            ).fetchall()
        return tuple(str(row["run_id"]) for row in rows)

    def next_wakeup_at(self) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT MIN(deadline) AS deadline FROM (
                    SELECT wake_at AS deadline FROM waits WHERE state='WAITING' AND wake_at IS NOT NULL
                    UNION ALL
                    SELECT timeout_at AS deadline FROM waits WHERE state='WAITING' AND timeout_at IS NOT NULL
                )
                """
            ).fetchone()
        return None if row is None else row["deadline"]

    def skip_step(self, run_id: str, step_id: str, *, reason: str) -> None:
        completed_at = self._now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, attempt, iteration FROM step_runs WHERE run_id=? AND step_id=?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown step {step_id!r}")
            if StepStatus(row["status"]) is not StepStatus.PENDING:
                raise StateStoreError(f"Step {step_id!r} cannot be skipped from {row['status']}")
            connection.execute(
                "UPDATE step_runs SET status=?, completed_at=? WHERE run_id=? AND step_id=?",
                (StepStatus.SKIPPED.value, completed_at, run_id, step_id),
            )
            self._event(
                connection,
                run_id,
                "step.skipped",
                step_id=step_id,
                attempt=int(row["attempt"]),
                iteration=int(row["iteration"]),
                data={"reason": reason},
            )

    def fail_step(
        self, run_id: str, step_id: str, error: str, *, claim_token: str | None = None
    ) -> None:
        completed_at = self._now()
        with self._transaction() as connection:
            iteration, attempt = self._running_attempt(
                connection, run_id, step_id, claim_token=claim_token
            )
            connection.execute(
                """
                UPDATE step_runs SET status = ?, error = ?, completed_at = ?,
                    claim_owner = NULL, claim_token = NULL, claimed_at = NULL,
                    lease_expires_at = NULL, heartbeat_at = NULL
                WHERE run_id = ? AND step_id = ?
                """,
                (StepStatus.FAILED.value, error, completed_at, run_id, step_id),
            )
            connection.execute(
                """
                UPDATE step_attempts SET completed_at = ?, outcome = ?, error = ?
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (completed_at, StepStatus.FAILED.value, error, run_id, step_id, iteration, attempt),
            )
            self._event(
                connection,
                run_id,
                "step.failed",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
                data={"error": error},
            )
            self._fail_workflow_tx(connection, run_id, f"Step {step_id} failed: {error}")

    def cancel_run(
        self, run_id: str, *, reason: str | None = None, actor: JsonValue = None
    ) -> WorkflowRun:
        with self._transaction() as connection:
            self._cancel_run_tx(connection, run_id, reason or "Cancelled", actor)
        return self.get_run(run_id)

    def _cancel_run_tx(
        self,
        connection: Any,
        run_id: str,
        reason: str,
        actor: JsonValue,
    ) -> None:
        row = connection.execute(
            "SELECT status FROM workflow_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise StateStoreError(f"Unknown workflow run {run_id!r}")
        status = WorkflowStatus(row["status"])
        if status is WorkflowStatus.CANCELLED:
            return
        if status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED):
            raise StateStoreError(f"Cannot cancel terminal workflow run {run_id!r}")
        now = self._now()
        connection.execute(
            """
            UPDATE workflow_runs SET status=?, completed_at=?, cancellation_reason=?
            WHERE run_id=?
            """,
            (WorkflowStatus.CANCELLED.value, now, reason, run_id),
        )
        connection.execute(
            """
            UPDATE step_runs SET status=?, completed_at=?, waiting_reason=NULL,
                cancellation_requested=1, claim_owner=NULL, claim_token=NULL,
                claimed_at=NULL, lease_expires_at=NULL, heartbeat_at=NULL
            WHERE run_id=? AND status NOT IN (?, ?)
            """,
            (
                StepStatus.CANCELLED.value, now, run_id,
                StepStatus.COMPLETED.value, StepStatus.FAILED.value,
            ),
        )
        connection.execute(
            "UPDATE waits SET state='CANCELLED', consumed_at=? WHERE run_id=? AND state='WAITING'",
            (now, run_id),
        )
        self._event(
            connection, run_id, "workflow.cancelled",
            data={"reason": reason, "actor": actor},
        )

    def human_task(self, run_id: str, step_id: str) -> HumanTask:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM human_tasks WHERE run_id=? AND step_id=?",
                (run_id, step_id),
            ).fetchone()
        if row is None:
            raise StateStoreError(f"No human task for step {step_id!r} in run {run_id}")
        return HumanTask(
            run_id, step_id, str(row["status"]), str(row["requested_at"]),
            row["responded_at"],
            (
                self._read_payload(
                    row["actor_json"],
                    aad=f"human_tasks:actor_json:{run_id}:{step_id}",
                )
                if row["actor_json"]
                else None
            ),
            row["comment"],
            (
                self._read_payload(
                    row["payload_json"],
                    aad=f"human_tasks:payload_json:{run_id}:{step_id}",
                )
                if row["payload_json"]
                else None
            ),
            row["timeout_at"],
        )

    def create_human_task(
        self,
        run_id: str,
        step_id: str,
        *,
        timeout_at: str | None = None,
    ) -> None:
        now = self._now()
        with self._transaction() as connection:
            step = connection.execute(
                "SELECT attempt, iteration FROM step_runs WHERE run_id=? AND step_id=?",
                (run_id, step_id),
            ).fetchone()
            if step is None:
                raise StateStoreError(f"Unknown step {step_id!r}")
            connection.execute(
                """
                INSERT INTO human_tasks(run_id, step_id, status, requested_at, timeout_at)
                VALUES (?, ?, 'PENDING', ?, ?)
                ON CONFLICT(run_id, step_id) DO UPDATE SET status='PENDING',
                    requested_at=excluded.requested_at, timeout_at=excluded.timeout_at,
                    responded_at=NULL, actor_json=NULL, comment=NULL, payload_json=NULL,
                    event_id=NULL
                """,
                (run_id, step_id, now, timeout_at),
            )
            self._event(
                connection, run_id, "human.approval_requested", step_id=step_id,
                attempt=int(step["attempt"]), iteration=int(step["iteration"]),
                data={"timeout_at": timeout_at},
            )

    def respond_human(
        self,
        run_id: str,
        step_id: str,
        decision: str,
        *,
        payload: JsonValue = None,
        comment: str | None = None,
        actor: JsonValue = None,
        event_id: str | None = None,
    ) -> HumanTask:
        decision = decision.upper()
        if decision not in {"APPROVED", "REJECTED"}:
            raise StateStoreError("Human decision must be APPROVED or REJECTED")
        now = self._now()
        with self._transaction() as connection:
            task = connection.execute(
                "SELECT * FROM human_tasks WHERE run_id=? AND step_id=?",
                (run_id, step_id),
            ).fetchone()
            if task is None:
                raise StateStoreError(f"No human task for step {step_id!r} in run {run_id}")
            if task["status"] != "PENDING":
                if event_id and task["event_id"] == event_id:
                    return self.human_task(run_id, step_id)
                raise StateStoreError(f"Human task {step_id!r} is already {task['status']}")
            connection.execute(
                """
                UPDATE human_tasks SET status=?, responded_at=?, actor_json=?, comment=?,
                    payload_json=?, event_id=? WHERE run_id=? AND step_id=?
                """,
                (
                    decision,
                    now,
                    (
                        self._payload(
                            actor, aad=f"human_tasks:actor_json:{run_id}:{step_id}"
                        )
                        if actor is not None
                        else None
                    ),
                    comment,
                    self._payload(
                        payload, aad=f"human_tasks:payload_json:{run_id}:{step_id}"
                    ),
                    event_id,
                    run_id,
                    step_id,
                ),
            )
            wait = connection.execute(
                "SELECT * FROM waits WHERE run_id=? AND step_id=? AND state='WAITING'",
                (run_id, step_id),
            ).fetchone()
            if wait is None:
                raise StateStoreError(f"Human task step {step_id!r} is not waiting")
            response = {"decision": decision.lower(), "payload": payload, "comment": comment, "actor": actor}
            self._resume_step_tx(connection, run_id, wait, payload=response, source="human")
            connection.execute(
                "UPDATE workflow_runs SET status=? WHERE run_id=?",
                (WorkflowStatus.RUNNING.value, run_id),
            )
            self._event(
                connection, run_id, "human.approval_received", step_id=step_id,
                attempt=int(wait["attempt"]), iteration=int(wait["iteration"]),
                data={"decision": decision.lower(), "comment": comment, "actor": actor},
            )
        return self.human_task(run_id, step_id)

    def complete_workflow(self, run_id: str) -> None:
        with self._transaction() as connection:
            incomplete = connection.execute(
                """
                SELECT step_id FROM step_runs
                WHERE run_id = ? AND status NOT IN (?, ?) LIMIT 1
                """,
                (run_id, StepStatus.COMPLETED.value, StepStatus.SKIPPED.value),
            ).fetchone()
            if incomplete is not None:
                raise StateStoreError(f"Cannot complete workflow; step {incomplete['step_id']} is incomplete")
            active_cycle = connection.execute(
                """
                SELECT cycle_name FROM cycle_runs
                WHERE run_id = ? AND status != ? LIMIT 1
                """,
                (run_id, CycleStatus.COMPLETED.value),
            ).fetchone()
            if active_cycle is not None:
                raise StateStoreError(
                    f"Cannot complete workflow; cycle {active_cycle['cycle_name']} is active"
                )
            row = connection.execute(
                "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown workflow run {run_id}")
            if WorkflowStatus(row["status"]) is WorkflowStatus.COMPLETED:
                return
            connection.execute(
                """
                UPDATE workflow_runs SET status = ?, completed_at = ?, error = NULL
                WHERE run_id = ?
                """,
                (WorkflowStatus.COMPLETED.value, self._now(), run_id),
            )
            self._event(connection, run_id, "workflow.completed")

    def wait_workflow(self, run_id: str) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown workflow run {run_id}")
            if WorkflowStatus(row["status"]) is WorkflowStatus.WAITING:
                return
            connection.execute(
                "UPDATE workflow_runs SET status = ? WHERE run_id = ?",
                (WorkflowStatus.WAITING.value, run_id),
            )
            self._event(connection, run_id, "workflow.waiting")

    def fail_workflow(self, run_id: str, error: str) -> None:
        with self._transaction() as connection:
            self._fail_workflow_tx(connection, run_id, error)

    def _fail_workflow_tx(
        self, connection: Any, run_id: str, error: str
    ) -> None:
        row = connection.execute(
            "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise StateStoreError(f"Unknown workflow run {run_id}")
        if WorkflowStatus(row["status"]) is WorkflowStatus.FAILED:
            return
        connection.execute(
            """
            UPDATE workflow_runs SET status = ?, completed_at = ?, error = ?
            WHERE run_id = ?
            """,
            (WorkflowStatus.FAILED.value, self._now(), error, run_id),
        )
        self._event(connection, run_id, "workflow.failed", data={"error": error})

    def get_runtime_session(
        self, run_id: str, step_id: str, runtime: str
    ) -> JsonValue | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT data_json FROM runtime_sessions
                WHERE run_id = ? AND step_id = ? AND runtime = ?
                """,
                (run_id, step_id, runtime),
            ).fetchone()
        return (
            None
            if row is None
            else self._read_payload(
                row["data_json"],
                aad=f"runtime_sessions:data_json:{run_id}:{step_id}:{runtime}",
            )
        )

    def set_runtime_session(
        self, run_id: str, step_id: str, runtime: str, data: JsonValue
    ) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT attempt, iteration FROM step_runs WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown step {step_id!r}")
            connection.execute(
                """
                INSERT INTO runtime_sessions(run_id, step_id, runtime, data_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id, step_id, runtime) DO UPDATE SET
                    data_json = excluded.data_json,
                    updated_at = excluded.updated_at
                """,
                (
                    run_id,
                    step_id,
                    runtime,
                    self._payload(
                        data,
                        aad=f"runtime_sessions:data_json:{run_id}:{step_id}:{runtime}",
                    ),
                    self._now(),
                ),
            )
            self._event(
                connection,
                run_id,
                "agent.session_saved",
                step_id=step_id,
                attempt=int(row["attempt"]),
                iteration=int(row["iteration"]),
                data={"runtime": runtime},
            )

    def record_agent_invocation(
        self,
        run_id: str,
        step_id: str,
        event_type: str,
        invocation_id: str,
        data: dict[str, JsonValue] | None = None,
    ) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT attempt, iteration FROM step_runs WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown step {step_id!r}")
            event_data: dict[str, JsonValue] = {"invocation_id": invocation_id}
            if data:
                event_data.update(data)
            self._event(
                connection,
                run_id,
                event_type,
                step_id=step_id,
                attempt=int(row["attempt"]),
                iteration=int(row["iteration"]),
                data=event_data,
            )

    def start_agent_invocation(
        self,
        run_id: str,
        step_id: str,
        invocation_id: str,
        *,
        runtime: str,
        model: str | None = None,
    ) -> None:
        with self._transaction() as connection:
            step = connection.execute(
                "SELECT attempt, iteration FROM step_runs WHERE run_id=? AND step_id=?",
                (run_id, step_id),
            ).fetchone()
            if step is None:
                raise StateStoreError(f"Unknown step {step_id!r}")
            started_at = self._now()
            connection.execute(
                """
                INSERT INTO agent_invocations(
                    invocation_id, run_id, step_id, attempt, iteration, runtime,
                    model, status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?)
                """,
                (
                    invocation_id,
                    run_id,
                    step_id,
                    int(step["attempt"]),
                    int(step["iteration"]),
                    runtime,
                    model,
                    started_at,
                ),
            )
            self._event(
                connection,
                run_id,
                "agent.request_started",
                step_id=step_id,
                attempt=int(step["attempt"]),
                iteration=int(step["iteration"]),
                data={"invocation_id": invocation_id, "runtime": runtime, "model": model},
            )

    def finish_agent_invocation(
        self,
        invocation_id: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        usage: AgentUsage | None = None,
        error: str | None = None,
    ) -> None:
        usage = usage or AgentUsage()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM agent_invocations WHERE invocation_id=?", (invocation_id,)
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown agent invocation {invocation_id!r}")
            completed_at = self._now()
            status = "FAILED" if error is not None else "COMPLETED"
            connection.execute(
                """
                UPDATE agent_invocations SET provider=?, model=COALESCE(?, model),
                    status=?, completed_at=?, input_tokens=?, output_tokens=?,
                    cached_tokens=?, total_tokens=?, cost=?, currency=?,
                    latency_seconds=?, error=? WHERE invocation_id=?
                """,
                (
                    provider,
                    model,
                    status,
                    completed_at,
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cached_tokens,
                    usage.total_tokens,
                    str(usage.cost) if usage.cost is not None else None,
                    usage.currency,
                    usage.latency_seconds,
                    error,
                    invocation_id,
                ),
            )
            data: dict[str, JsonValue] = {
                "invocation_id": invocation_id,
                "runtime": row["runtime"],
                "provider": provider,
                "model": model or row["model"],
            }
            if error is not None:
                data["error"] = error
            else:
                data["usage"] = {
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cached_tokens": usage.cached_tokens,
                    "total_tokens": usage.total_tokens,
                    "cost": str(usage.cost) if usage.cost is not None else None,
                    "currency": usage.currency,
                    "latency_seconds": usage.latency_seconds,
                }
            self._event(
                connection,
                str(row["run_id"]),
                "agent.request_failed" if error is not None else "agent.request_finished",
                step_id=str(row["step_id"]),
                attempt=int(row["attempt"]),
                iteration=int(row["iteration"]),
                data=data,
            )

    def agent_invocations(self, run_id: str) -> tuple[AgentInvocation, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM agent_invocations WHERE run_id=? ORDER BY started_at, invocation_id",
                (run_id,),
            ).fetchall()
        return tuple(
            AgentInvocation(
                invocation_id=str(row["invocation_id"]),
                run_id=str(row["run_id"]),
                step_id=str(row["step_id"]),
                attempt=int(row["attempt"]),
                iteration=int(row["iteration"]),
                runtime=str(row["runtime"]),
                provider=row["provider"],
                model=row["model"],
                status=str(row["status"]),
                started_at=str(row["started_at"]),
                completed_at=row["completed_at"],
                usage=AgentUsage(
                    input_tokens=row["input_tokens"],
                    output_tokens=row["output_tokens"],
                    cached_tokens=row["cached_tokens"],
                    total_tokens=row["total_tokens"],
                    cost=Decimal(row["cost"]) if row["cost"] is not None else None,
                    currency=row["currency"],
                    latency_seconds=row["latency_seconds"],
                ),
                error=row["error"],
            )
            for row in rows
        )

    def usage(self, run_id: str, step_id: str | None = None) -> UsageSummary:
        invocations = self.agent_invocations(run_id)
        if step_id is not None:
            invocations = tuple(item for item in invocations if item.step_id == step_id)
        return UsageSummary.combine([item.usage for item in invocations])

    def pending_export_events(
        self, run_id: str, exporter: str, *, limit: int = 100
    ) -> tuple[WorkflowEvent, ...]:
        if limit < 1:
            raise StateStoreError("Export limit must be positive")
        with self._connect() as connection:
            cursor = connection.execute(
                "SELECT last_sequence FROM event_export_cursors WHERE run_id=? AND exporter=?",
                (run_id, exporter),
            ).fetchone()
            after = int(cursor["last_sequence"]) if cursor is not None else 0
            rows = connection.execute(
                """
                SELECT * FROM events WHERE run_id=? AND sequence>?
                ORDER BY sequence LIMIT ?
                """,
                (run_id, after, limit),
            ).fetchall()
        return tuple(
            WorkflowEvent(
                sequence=int(row["sequence"]),
                type=str(row["event_type"]),
                timestamp=str(row["timestamp"]),
                step_id=row["step_id"],
                attempt=row["attempt"],
                iteration=row["iteration"],
                data=self._event_data(
                    row["data_json"], aad=f"events:data_json:{run_id}:{row['sequence']}"
                ),
                schema_version=int(row["schema_version"]),
            )
            for row in rows
        )

    def advance_export_cursor(self, run_id: str, exporter: str, sequence: int) -> None:
        with self._transaction() as connection:
            latest = connection.execute(
                "SELECT MAX(sequence) AS maximum FROM events WHERE run_id=?", (run_id,)
            ).fetchone()
            if latest is None or latest["maximum"] is None or sequence > int(latest["maximum"]):
                raise StateStoreError("Export cursor cannot advance beyond available events")
            connection.execute(
                """
                INSERT INTO event_export_cursors(run_id, exporter, last_sequence, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id, exporter) DO UPDATE SET
                    last_sequence=MAX(last_sequence, excluded.last_sequence),
                    updated_at=excluded.updated_at
                """,
                (run_id, exporter, sequence, self._now()),
            )

    def register_worker(self, worker_id: str, metadata: JsonValue = None) -> None:
        now = self._now()
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO workers(worker_id, started_at, heartbeat_at, metadata_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET heartbeat_at=excluded.heartbeat_at,
                    metadata_json=excluded.metadata_json
                """,
                (worker_id, now, now, self._json(metadata or {})),
            )

    def rotate_encryption(self) -> int:
        """Re-encrypt all opaque payload columns with the codec's active key."""

        reencrypt = getattr(self._payload_codec, "reencrypt", None)
        if not callable(reencrypt):
            raise StateStoreError("Configured payload codec does not support key rotation")
        specifications = (
            ("step_runs", ("run_id", "step_id"), "inputs_json", lambda row: f"step_runs:inputs_json:{row['run_id']}:{row['step_id']}"),
            ("step_runs", ("run_id", "step_id"), "output_json", lambda row: f"step_runs:output_json:{row['run_id']}:{row['step_id']}"),
            ("step_runs", ("run_id", "step_id"), "signal_payload_json", lambda row: f"step_runs:signal_payload_json:{row['run_id']}:{row['step_id']}"),
            ("events", ("run_id", "sequence"), "data_json", lambda row: f"events:data_json:{row['run_id']}:{row['sequence']}"),
            ("runtime_sessions", ("run_id", "step_id", "runtime"), "data_json", lambda row: f"runtime_sessions:data_json:{row['run_id']}:{row['step_id']}:{row['runtime']}"),
            ("signals", ("run_id", "name", "event_id"), "payload_json", lambda row: f"signals:payload_json:{row['run_id']}:{row['name']}:{row['event_id']}"),
            ("signals", ("run_id", "name", "event_id"), "actor_json", lambda row: f"signals:actor_json:{row['run_id']}:{row['name']}:{row['event_id']}"),
            ("human_tasks", ("run_id", "step_id"), "payload_json", lambda row: f"human_tasks:payload_json:{row['run_id']}:{row['step_id']}"),
            ("human_tasks", ("run_id", "step_id"), "actor_json", lambda row: f"human_tasks:actor_json:{row['run_id']}:{row['step_id']}"),
            ("idempotency_records", ("run_id", "step_id", "iteration", "action"), "result_json", lambda row: f"idempotency_records:result_json:{row['run_id']}:{row['step_id']}:{row['iteration']}:{row['action']}"),
            ("step_attempts", ("run_id", "step_id", "iteration", "attempt"), "candidate_output_json", lambda row: f"step_attempts:candidate_output_json:{row['run_id']}:{row['step_id']}:{row['iteration']}:{row['attempt']}"),
        )
        changed = 0
        with self._transaction() as connection:
            for table, keys, column, aad_for in specifications:
                rows = connection.execute(
                    f"SELECT {', '.join(keys)}, {column} FROM {table} WHERE {column} IS NOT NULL"
                ).fetchall()
                for row in rows:
                    aad = aad_for(row)
                    encoded = reencrypt(row[column], aad=aad)
                    where = " AND ".join(f"{key}=?" for key in keys)
                    connection.execute(
                        f"UPDATE {table} SET {column}=? WHERE {where}",
                        (encoded, *(row[key] for key in keys)),
                    )
                    changed += 1
        return changed

    def heartbeat_worker(self, worker_id: str) -> None:
        with self._transaction() as connection:
            updated = connection.execute(
                "UPDATE workers SET heartbeat_at=? WHERE worker_id=?",
                (self._now(), worker_id),
            )
            if updated.rowcount != 1:
                raise StateStoreError(f"Unknown worker {worker_id!r}")

    def list_workers(self) -> tuple[dict[str, JsonValue], ...]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM workers ORDER BY worker_id").fetchall()
        return tuple(
            {
                "worker_id": str(row["worker_id"]),
                "started_at": str(row["started_at"]),
                "heartbeat_at": str(row["heartbeat_at"]),
                "metadata": json.loads(row["metadata_json"]),
            }
            for row in rows
        )

    def claim_idempotency(
        self,
        run_id: str,
        step_id: str,
        iteration: int,
        action: str,
        key: str,
        *,
        lease_duration: timedelta = timedelta(minutes=2),
    ) -> IdempotencyClaim:
        if not isinstance(action, str) or not action.strip():
            raise StateStoreError("Idempotency action must be a non-empty string")
        if lease_duration.total_seconds() <= 0:
            raise StateStoreError("Idempotency lease duration must be positive")
        token = str(uuid4())
        created_at = self._now()
        lease_expires_at = (
            datetime.fromisoformat(created_at) + lease_duration
        ).isoformat(timespec="microseconds")
        with self._transaction() as connection:
            step = connection.execute(
                "SELECT iteration FROM step_runs WHERE run_id=? AND step_id=?",
                (run_id, step_id),
            ).fetchone()
            if step is None:
                raise StateStoreError(f"Unknown step {step_id!r} in run {run_id}")
            if int(step["iteration"]) != iteration:
                raise ConcurrencyError(f"Step {step_id} moved to another iteration")
            existing = connection.execute(
                """
                SELECT * FROM idempotency_records
                WHERE run_id=? AND step_id=? AND iteration=? AND action=?
                """,
                (run_id, step_id, iteration, action),
            ).fetchone()
            if existing is not None:
                if existing["idempotency_key"] != key:
                    raise ConcurrencyError("Idempotency record key does not match its logical action")
                if existing["status"] == "COMPLETED":
                    result = (
                        self._read_payload(
                            existing["result_json"],
                            aad=(
                                f"idempotency_records:result_json:{run_id}:{step_id}:"
                                f"{iteration}:{action}"
                            ),
                        )
                        if existing["result_json"] is not None
                        else None
                    )
                    return IdempotencyClaim(
                        run_id, step_id, iteration, action, key, "", True, result
                    )
                if existing["lease_expires_at"] > created_at:
                    raise ConcurrencyError(
                        f"Idempotent action {action!r} is already in progress"
                    )
                connection.execute(
                    """
                    UPDATE idempotency_records SET claim_token=?, lease_expires_at=?,
                        created_at=? WHERE run_id=? AND step_id=? AND iteration=? AND action=?
                    """,
                    (
                        token, lease_expires_at, created_at, run_id, step_id,
                        iteration, action,
                    ),
                )
                self._event(
                    connection, run_id, "idempotency.recovered", step_id=step_id,
                    iteration=iteration, data={"action": action, "key": key},
                )
                return IdempotencyClaim(run_id, step_id, iteration, action, key, token)
            connection.execute(
                """
                INSERT INTO idempotency_records(
                    run_id, step_id, iteration, action, idempotency_key, status,
                    claim_token, lease_expires_at, created_at
                ) VALUES (?, ?, ?, ?, ?, 'CLAIMED', ?, ?, ?)
                """,
                (
                    run_id, step_id, iteration, action, key, token,
                    lease_expires_at, created_at,
                ),
            )
            self._event(
                connection,
                run_id,
                "idempotency.claimed",
                step_id=step_id,
                iteration=iteration,
                data={"action": action, "key": key},
            )
        return IdempotencyClaim(run_id, step_id, iteration, action, key, token)

    def complete_idempotency(self, token: str, result: JsonValue) -> None:
        completed_at = self._now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM idempotency_records WHERE claim_token=? AND status='CLAIMED'",
                (token,),
            ).fetchone()
            if row is None:
                raise ConcurrencyError("Idempotency claim is missing or no longer valid")
            connection.execute(
                """
                UPDATE idempotency_records SET status='COMPLETED', result_json=?,
                    completed_at=?, claim_token=NULL, lease_expires_at=NULL
                WHERE claim_token=?
                """,
                (
                    self._payload(
                        result,
                        aad=(
                            f"idempotency_records:result_json:{row['run_id']}:"
                            f"{row['step_id']}:{row['iteration']}:{row['action']}"
                        ),
                    ),
                    completed_at,
                    token,
                ),
            )
            self._event(
                connection,
                str(row["run_id"]),
                "idempotency.completed",
                step_id=str(row["step_id"]),
                iteration=int(row["iteration"]),
                data={"action": str(row["action"]), "key": str(row["idempotency_key"])},
            )

    def release_idempotency(self, token: str) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM idempotency_records WHERE claim_token=? AND status='CLAIMED'",
                (token,),
            ).fetchone()
            if row is None:
                return
            connection.execute(
                "DELETE FROM idempotency_records WHERE claim_token=?", (token,)
            )
            self._event(
                connection,
                str(row["run_id"]),
                "idempotency.released",
                step_id=str(row["step_id"]),
                iteration=int(row["iteration"]),
                data={"action": str(row["action"]), "key": str(row["idempotency_key"])},
            )

    def set_codex_thread(self, run_id: str, step_id: str, thread_id: str) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT codex_thread_id, attempt, iteration FROM step_runs
                WHERE run_id = ? AND step_id = ?
                """,
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown step {step_id!r}")
            existing = row["codex_thread_id"]
            if existing is not None and existing != thread_id:
                raise StateStoreError(
                    f"Step {step_id} already owns Codex thread {existing}; refusing {thread_id}"
                )
            if existing == thread_id:
                return
            connection.execute(
                """
                UPDATE step_runs SET codex_thread_id = ?
                WHERE run_id = ? AND step_id = ?
                """,
                (thread_id, run_id, step_id),
            )
            connection.execute(
                """
                INSERT INTO runtime_sessions(run_id, step_id, runtime, data_json, updated_at)
                VALUES (?, ?, 'codex', ?, ?)
                ON CONFLICT(run_id, step_id, runtime) DO UPDATE SET
                    data_json = excluded.data_json,
                    updated_at = excluded.updated_at
                """,
                (
                    run_id,
                    step_id,
                    self._payload(
                        {"thread_id": thread_id},
                        aad=f"runtime_sessions:data_json:{run_id}:{step_id}:codex",
                    ),
                    self._now(),
                ),
            )
            self._event(
                connection,
                run_id,
                "codex.thread_started",
                step_id=step_id,
                attempt=int(row["attempt"]),
                iteration=int(row["iteration"]),
                data={"thread_id": thread_id},
            )

    def record_codex_turn(
        self,
        run_id: str,
        step_id: str,
        event_type: str,
        turn_id: str,
        data: dict[str, JsonValue] | None = None,
    ) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT attempt, iteration FROM step_runs WHERE run_id = ? AND step_id = ?",
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise StateStoreError(f"Unknown step {step_id!r}")
            event_data: dict[str, JsonValue] = {"turn_id": turn_id}
            if data:
                event_data.update(data)
            self._event(
                connection,
                run_id,
                event_type,
                step_id=step_id,
                attempt=int(row["attempt"]),
                iteration=int(row["iteration"]),
                data=event_data,
            )

    def _running_attempt(
        self,
        connection: Any,
        run_id: str,
        step_id: str,
        *,
        claim_token: str | None = None,
    ) -> tuple[int, int]:
        row = connection.execute(
            """
            SELECT s.status, s.attempt, s.iteration, s.claim_token,
                s.lease_expires_at, w.status AS workflow_status
            FROM step_runs AS s
            JOIN workflow_runs AS w ON w.run_id = s.run_id
            WHERE s.run_id = ? AND s.step_id = ?
            """,
            (run_id, step_id),
        ).fetchone()
        if row is None:
            raise StateStoreError(f"Unknown step {step_id!r} in run {run_id}")
        if claim_token is not None and (
            StepStatus(row["status"]) is not StepStatus.RUNNING
            or row["claim_token"] != claim_token
        ):
            raise ConcurrencyError(f"Claim for step {step_id} is no longer valid")
        if StepStatus(row["status"]) is not StepStatus.RUNNING:
            raise StateStoreError(f"Step {step_id} is not RUNNING")
        if WorkflowStatus(row["workflow_status"]) is not WorkflowStatus.RUNNING:
            raise ConcurrencyError(f"Workflow run {run_id!r} no longer accepts step results")
        if claim_token is not None:
            if row["claim_token"] != claim_token:
                raise ConcurrencyError(f"Claim for step {step_id} is no longer valid")
            if row["lease_expires_at"] <= self._now():
                raise ConcurrencyError(f"Claim for step {step_id} has expired")
        return int(row["iteration"]), int(row["attempt"])
