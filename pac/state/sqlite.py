from __future__ import annotations

import hashlib
import importlib
import json
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..errors import StateStoreError, WorkflowExecutionError
from ..models import (
    CycleState,
    CycleStatus,
    JsonValue,
    StepState,
    StepStatus,
    WorkflowDefinition,
    WorkflowEvent,
    WorkflowRun,
    WorkflowStatus,
)
from .base import StateStore


def _utc_now() -> datetime:
    return datetime.now(UTC)


class SQLiteStateStore(StateStore):
    """SQLite-backed durable state with transactional events."""

    def __init__(
        self,
        path: str | Path = ".pac/state.db",
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
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

                    CREATE TABLE IF NOT EXISTS workflow_runs (
                        run_id TEXT PRIMARY KEY,
                        workflow_name TEXT NOT NULL,
                        definition_fingerprint TEXT NOT NULL,
                        definition_json TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT,
                        error TEXT,
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
                        data_json TEXT NOT NULL,
                        PRIMARY KEY (run_id, sequence),
                        FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
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
                event_columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(events)").fetchall()
                }
                if "iteration" not in event_columns:
                    connection.execute("ALTER TABLE events ADD COLUMN iteration INTEGER")
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
        except sqlite3.Error as exc:
            raise StateStoreError(f"Could not initialize SQLite state at {self.path}: {exc}") from exc

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat(timespec="microseconds")

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)

    def _event(
        self,
        connection: sqlite3.Connection,
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
                run_id, sequence, event_type, timestamp, step_id, attempt, iteration, data_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                sequence,
                event_type,
                self._now(),
                step_id,
                attempt,
                iteration,
                self._json(data or {}),
            ),
        )

    @contextmanager
    def execution_lock(self, workflow_name: str) -> Iterator[None]:
        digest = hashlib.sha256(workflow_name.encode("utf-8")).hexdigest()[:20]
        lock_path = self.path.with_name(f"{self.path.name}.{digest}.lock")
        handle = lock_path.open("a+b")
        locked = False
        try:
            try:
                if os.name == "nt":
                    msvcrt: Any = importlib.import_module("msvcrt")

                    handle.seek(0)
                    if handle.tell() == 0:
                        handle.write(b"0")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except (BlockingIOError, OSError) as exc:
                raise WorkflowExecutionError(
                    f"Workflow {workflow_name!r} is already being executed"
                ) from exc
            yield
        finally:
            try:
                if locked and os.name == "nt":
                    msvcrt = importlib.import_module("msvcrt")

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                elif locked:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def active_run(self, workflow_name: str) -> WorkflowRun | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id FROM workflow_runs
                WHERE workflow_name = ? AND status IN (?, ?, ?)
                ORDER BY rowid DESC LIMIT 1
                """,
                (
                    workflow_name,
                    WorkflowStatus.PENDING.value,
                    WorkflowStatus.RUNNING.value,
                    WorkflowStatus.WAITING.value,
                ),
            ).fetchone()
        return None if row is None else self.get_run(str(row["run_id"]))

    def create_run(self, definition: WorkflowDefinition) -> WorkflowRun:
        run_id = str(uuid4())
        created_at = self._now()
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO workflow_runs(
                    run_id, workflow_name, definition_fingerprint, definition_json,
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    definition.name,
                    definition.fingerprint,
                    definition.canonical_json,
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
                        self._json(step.inputs),
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
                "SELECT * FROM cycle_runs WHERE run_id = ? ORDER BY rowid", (run_id,)
            ).fetchall()

        steps: dict[str, StepState] = {}
        outputs: dict[str, JsonValue] = {}
        for row in step_rows:
            output = json.loads(row["output_json"]) if row["output_json"] is not None else None
            state = StepState(
                id=str(row["step_id"]),
                registration_order=int(row["registration_order"]),
                dependencies=tuple(json.loads(row["dependencies_json"])),
                max_attempts=int(row["max_attempts"]),
                status=StepStatus(row["status"]),
                attempt=int(row["attempt"]),
                inputs=json.loads(row["inputs_json"]),
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                error=row["error"],
                output=output,
                codex_thread_id=row["codex_thread_id"],
                retry_reason=row["retry_reason"],
                waiting_reason=row["waiting_reason"],
                iteration=int(row["iteration"]),
                has_output=row["output_json"] is not None,
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
                data=json.loads(row["data_json"]),
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
        with self._transaction() as connection:
            run = connection.execute(
                "SELECT status FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or WorkflowStatus(run["status"]) is not WorkflowStatus.WAITING:
                raise StateStoreError(f"Workflow {run_id} is not waiting")
            rows = connection.execute(
                """
                SELECT step_id, attempt, iteration FROM step_runs
                WHERE run_id = ? AND status = ? ORDER BY registration_order
                """,
                (run_id, StepStatus.WAITING.value),
            ).fetchall()
            for row in rows:
                connection.execute(
                    """
                    UPDATE step_runs SET status = ?, waiting_reason = NULL
                    WHERE run_id = ? AND step_id = ?
                    """,
                    (StepStatus.PENDING.value, run_id, row["step_id"]),
                )
                self._event(
                    connection,
                    run_id,
                    "step.resumed",
                    step_id=row["step_id"],
                    attempt=int(row["attempt"]),
                    iteration=int(row["iteration"]),
                )
            connection.execute(
                "UPDATE workflow_runs SET status = ? WHERE run_id = ?",
                (WorkflowStatus.RUNNING.value, run_id),
            )
            self._event(connection, run_id, "workflow.resumed")

    def recover_running(self, run_id: str) -> None:
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT step_id, attempt, max_attempts, iteration FROM step_runs
                WHERE run_id = ? AND status = ? ORDER BY registration_order
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
                    UPDATE step_runs SET status = ?, error = ?, retry_reason = ?
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

    def complete_step(self, run_id: str, step_id: str, output: JsonValue) -> None:
        output_json = self._json(output)
        completed_at = self._now()
        with self._transaction() as connection:
            iteration, attempt = self._running_attempt(connection, run_id, step_id)
            connection.execute(
                """
                UPDATE step_runs SET status = ?, output_json = ?, completed_at = ?,
                    error = NULL, retry_reason = NULL, waiting_reason = NULL
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
    ) -> bool:
        output_json = self._json(output)
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
            iteration, attempt = self._running_attempt(connection, run_id, step_id)
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
                    UPDATE step_runs SET status = ?, error = ?, completed_at = ?
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
                    retry_reason = NULL, waiting_reason = NULL
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
    ) -> bool:
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT status, attempt, max_attempts, iteration FROM step_runs
                WHERE run_id = ? AND step_id = ?
                """,
                (run_id, step_id),
            ).fetchone()
            if row is None or StepStatus(row["status"]) is not StepStatus.RUNNING:
                raise StateStoreError(f"Step {step_id} is not RUNNING")
            attempt = int(row["attempt"])
            iteration = int(row["iteration"])
            can_retry = attempt < int(row["max_attempts"])
            completed_at = self._now()
            candidate_json = self._json(candidate) if rejected else None
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
                    "step.output_rejected",
                    step_id=step_id,
                    attempt=attempt,
                    iteration=iteration,
                    data={"reason": reason, "candidate": candidate},
                )
            if can_retry:
                connection.execute(
                    """
                    UPDATE step_runs SET status = ?, retry_reason = ?, error = NULL
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
                    completed_at = ? WHERE run_id = ? AND step_id = ?
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

    def wait_step(self, run_id: str, step_id: str, reason: str | None) -> None:
        completed_at = self._now()
        with self._transaction() as connection:
            iteration, attempt = self._running_attempt(connection, run_id, step_id)
            connection.execute(
                """
                UPDATE step_runs SET status = ?, waiting_reason = ?
                WHERE run_id = ? AND step_id = ?
                """,
                (StepStatus.WAITING.value, reason, run_id, step_id),
            )
            connection.execute(
                """
                UPDATE step_attempts SET completed_at = ?, outcome = ?
                WHERE run_id = ? AND step_id = ? AND iteration = ? AND attempt = ?
                """,
                (completed_at, StepStatus.WAITING.value, run_id, step_id, iteration, attempt),
            )
            self._event(
                connection,
                run_id,
                "step.waiting",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
                data={"reason": reason},
            )

    def fail_step(self, run_id: str, step_id: str, error: str) -> None:
        completed_at = self._now()
        with self._transaction() as connection:
            iteration, attempt = self._running_attempt(connection, run_id, step_id)
            connection.execute(
                """
                UPDATE step_runs SET status = ?, error = ?, completed_at = ?
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

    def complete_workflow(self, run_id: str) -> None:
        with self._transaction() as connection:
            incomplete = connection.execute(
                """
                SELECT step_id FROM step_runs
                WHERE run_id = ? AND status != ? LIMIT 1
                """,
                (run_id, StepStatus.COMPLETED.value),
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
        self, connection: sqlite3.Connection, run_id: str, error: str
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

    @staticmethod
    def _running_attempt(
        connection: sqlite3.Connection, run_id: str, step_id: str
    ) -> tuple[int, int]:
        row = connection.execute(
            "SELECT status, attempt, iteration FROM step_runs WHERE run_id = ? AND step_id = ?",
            (run_id, step_id),
        ).fetchone()
        if row is None or StepStatus(row["status"]) is not StepStatus.RUNNING:
            raise StateStoreError(f"Step {step_id} is not RUNNING")
        return int(row["iteration"]), int(row["attempt"])
