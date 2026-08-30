from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from ..errors import ConcurrencyError, ConfigurationError, StateStoreError
from ..events import sanitize_event_data
from ..models import JsonValue, StepClaim, StepStatus, WorkflowStatus
from .encryption import JsonPayloadCodec, PayloadCodec
from .migrations import EVENT_SCHEMA_VERSION, MIGRATIONS
from .sqlite import SQLiteStateStore, _utc_now


class _PostgresConnection:
    """Translate the store's portable qmark SQL to psycopg placeholders."""

    def __init__(self, connection: Any) -> None:
        self.raw = connection

    @staticmethod
    def _sql(statement: str) -> str:
        return statement.replace("?", "%s")

    def execute(self, statement: str, parameters: tuple[Any, ...] = ()) -> Any:
        return self.raw.execute(self._sql(statement), parameters)

    def commit(self) -> None:
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def close(self) -> None:
        self.raw.close()

    def __enter__(self) -> "_PostgresConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.raw.commit()
        else:
            self.raw.rollback()
        self.raw.close()


class PostgreSQLStateStore(SQLiteStateStore):
    """PostgreSQL state store with row locking and skip-locked step claims.

    The transition implementation is shared with SQLite, while transaction start,
    event sequencing, and work claiming use PostgreSQL-specific locking semantics.
    psycopg is optional; install ``process-as-code[postgres]`` to use this backend.
    """

    def __init__(
        self,
        dsn: str,
        *,
        clock: Callable[[], datetime] = _utc_now,
        payload_codec: PayloadCodec | None = None,
    ) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise ConfigurationError(
                "PostgreSQL persistence requires the 'postgres' extra (psycopg)"
            ) from exc
        self.dsn = dsn
        self._psycopg = psycopg
        self._dict_row = dict_row
        self._clock = clock
        self._payload_codec = payload_codec or JsonPayloadCodec()
        self._initialize()

    def _connect(self) -> Any:
        try:
            return _PostgresConnection(
                self._psycopg.connect(self.dsn, row_factory=self._dict_row)
            )
        except self._psycopg.Error as exc:
            raise StateStoreError(f"Could not connect to PostgreSQL: {exc}") from exc

    @contextmanager
    def _transaction(self) -> Iterator[Any]:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS pac_schema_migrations (
            version INTEGER PRIMARY KEY, name TEXT NOT NULL,
            checksum TEXT NOT NULL, applied_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workflow_runs (
            run_id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL,
            definition_fingerprint TEXT NOT NULL, definition_json TEXT NOT NULL,
            definition_format_version INTEGER NOT NULL DEFAULT 1,
            state_format_version INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL,
            created_at TEXT NOT NULL, started_at TEXT, completed_at TEXT, error TEXT,
            cancellation_reason TEXT, next_event_sequence INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS step_runs (
            run_id TEXT NOT NULL, step_id TEXT NOT NULL,
            registration_order INTEGER NOT NULL, dependencies_json TEXT NOT NULL,
            inputs_json TEXT NOT NULL DEFAULT '{}', max_attempts INTEGER NOT NULL,
            status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
            started_at TEXT, completed_at TEXT, error TEXT, output_json TEXT,
            codex_thread_id TEXT, retry_reason TEXT, waiting_reason TEXT,
            iteration INTEGER NOT NULL DEFAULT 1, claim_owner TEXT, claim_token TEXT,
            claimed_at TEXT, lease_expires_at TEXT, heartbeat_at TEXT,
            available_at TEXT, signal_payload_json TEXT,
            cancellation_requested INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (run_id, step_id),
            FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS step_attempts (
            run_id TEXT NOT NULL, step_id TEXT NOT NULL,
            iteration INTEGER NOT NULL DEFAULT 1, attempt INTEGER NOT NULL,
            started_at TEXT NOT NULL, completed_at TEXT, outcome TEXT NOT NULL,
            error TEXT, candidate_output_json TEXT, rejection_reason TEXT,
            PRIMARY KEY (run_id, step_id, iteration, attempt),
            FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
        );
        CREATE TABLE IF NOT EXISTS events (
            run_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_type TEXT NOT NULL,
            timestamp TEXT NOT NULL, step_id TEXT, attempt INTEGER, iteration INTEGER,
            schema_version INTEGER NOT NULL DEFAULT 1, data_json TEXT NOT NULL,
            PRIMARY KEY (run_id, sequence),
            FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS runtime_sessions (
            run_id TEXT NOT NULL, step_id TEXT NOT NULL, runtime TEXT NOT NULL,
            data_json TEXT NOT NULL, updated_at TEXT NOT NULL,
            PRIMARY KEY (run_id, step_id, runtime),
            FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
        );
        CREATE TABLE IF NOT EXISTS waits (
            run_id TEXT NOT NULL, step_id TEXT NOT NULL, iteration INTEGER NOT NULL,
            attempt INTEGER NOT NULL, kind TEXT NOT NULL, signal_name TEXT,
            wake_at TEXT, timeout_at TEXT, timeout_action TEXT NOT NULL,
            payload_schema TEXT, state TEXT NOT NULL, created_at TEXT NOT NULL,
            consumed_at TEXT, PRIMARY KEY (run_id, step_id),
            FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
        );
        CREATE TABLE IF NOT EXISTS signals (
            run_id TEXT NOT NULL, name TEXT NOT NULL, event_id TEXT NOT NULL,
            payload_json TEXT NOT NULL, actor_json TEXT, received_at TEXT NOT NULL,
            consumed_by_step_id TEXT, consumed_at TEXT,
            PRIMARY KEY (run_id, name, event_id),
            FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS human_tasks (
            run_id TEXT NOT NULL, step_id TEXT NOT NULL, status TEXT NOT NULL,
            requested_at TEXT NOT NULL, responded_at TEXT, actor_json TEXT,
            comment TEXT, payload_json TEXT, timeout_at TEXT, event_id TEXT,
            PRIMARY KEY (run_id, step_id),
            FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
        );
        CREATE TABLE IF NOT EXISTS idempotency_records (
            run_id TEXT NOT NULL, step_id TEXT NOT NULL, iteration INTEGER NOT NULL,
            action TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL, claim_token TEXT, lease_expires_at TEXT,
            result_json TEXT, created_at TEXT NOT NULL, completed_at TEXT,
            PRIMARY KEY (run_id, step_id, iteration, action),
            FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
        );
        CREATE TABLE IF NOT EXISTS agent_invocations (
            invocation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, step_id TEXT NOT NULL,
            attempt INTEGER NOT NULL, iteration INTEGER NOT NULL, runtime TEXT NOT NULL,
            provider TEXT, model TEXT, status TEXT NOT NULL, started_at TEXT NOT NULL,
            completed_at TEXT, input_tokens INTEGER, output_tokens INTEGER,
            cached_tokens INTEGER, total_tokens INTEGER, cost TEXT, currency TEXT,
            latency_seconds DOUBLE PRECISION, error TEXT,
            FOREIGN KEY (run_id, step_id) REFERENCES step_runs(run_id, step_id)
        );
        CREATE TABLE IF NOT EXISTS event_export_cursors (
            run_id TEXT NOT NULL, exporter TEXT NOT NULL,
            last_sequence INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL,
            PRIMARY KEY (run_id, exporter),
            FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
        );
        CREATE TABLE IF NOT EXISTS workers (
            worker_id TEXT PRIMARY KEY, started_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS cycle_runs (
            run_id TEXT NOT NULL, cycle_name TEXT NOT NULL, members_json TEXT NOT NULL,
            controller_step_id TEXT NOT NULL, entry_step_id TEXT NOT NULL,
            max_iterations INTEGER NOT NULL, iteration INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL, PRIMARY KEY (run_id, cycle_name),
            FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_step_claim_token
            ON step_runs(claim_token) WHERE claim_token IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_step_lease_expiry
            ON step_runs(lease_expires_at) WHERE claim_token IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency_claim_token
            ON idempotency_records(claim_token) WHERE claim_token IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_waits_due ON waits(state, wake_at, timeout_at);
        CREATE INDEX IF NOT EXISTS idx_waits_signal ON waits(run_id, signal_name, state);
        CREATE INDEX IF NOT EXISTS idx_signals_unconsumed
            ON signals(run_id, name, received_at) WHERE consumed_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_agent_invocations_run
            ON agent_invocations(run_id, step_id, started_at);
        """
        try:
            with self._connect() as connection:
                for statement in ddl.split(";"):
                    if statement.strip():
                        connection.execute(statement)
                applied = {
                    int(row["version"]): (str(row["name"]), str(row["checksum"]))
                    for row in connection.execute(
                        "SELECT version, name, checksum FROM pac_schema_migrations"
                    ).fetchall()
                }
                for migration in MIGRATIONS:
                    recorded = applied.get(migration.version)
                    identity = (migration.name, migration.checksum)
                    if recorded is not None and recorded != identity:
                        raise StateStoreError(
                            f"PostgreSQL migration {migration.version} does not match installed history"
                        )
                    if recorded is None:
                        connection.execute(
                            "INSERT INTO pac_schema_migrations(version,name,checksum,applied_at) "
                            "VALUES (?,?,?,?)",
                            (
                                migration.version,
                                migration.name,
                                migration.checksum,
                                self._now(),
                            ),
                        )
        except self._psycopg.Error as exc:
            raise StateStoreError(f"Could not initialize PostgreSQL state: {exc}") from exc

    def advance_export_cursor(
        self, run_id: str, exporter: str, sequence: int
    ) -> None:
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO event_export_cursors(run_id, exporter, last_sequence, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id, exporter) DO UPDATE SET
                    last_sequence=GREATEST(
                        event_export_cursors.last_sequence,
                        excluded.last_sequence
                    ),
                    updated_at=excluded.updated_at
                """,
                (run_id, exporter, sequence, self._now()),
            )

    def _event(
        self,
        connection: _PostgresConnection,
        run_id: str,
        event_type: str,
        *,
        step_id: str | None = None,
        attempt: int | None = None,
        iteration: int | None = None,
        data: dict[str, JsonValue] | None = None,
    ) -> None:
        row = connection.execute(
            "SELECT next_event_sequence FROM workflow_runs WHERE run_id = ? FOR UPDATE",
            (run_id,),
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
            INSERT INTO events(run_id,sequence,event_type,timestamp,step_id,attempt,
                iteration,schema_version,data_json) VALUES (?,?,?,?,?,?,?,?,?)
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
                "SELECT status FROM workflow_runs WHERE run_id = ? FOR UPDATE", (run_id,)
            ).fetchone()
            if run is None or WorkflowStatus(run["status"]) is not WorkflowStatus.RUNNING:
                raise ConcurrencyError(f"Workflow run {run_id!r} is not RUNNING")
            row = connection.execute(
                """
                SELECT * FROM step_runs WHERE run_id = ? AND step_id = ?
                FOR UPDATE SKIP LOCKED
                """,
                (run_id, step_id),
            ).fetchone()
            if row is None:
                raise ConcurrencyError(f"Step {step_id!r} is unavailable or already claimed")
            previous = StepStatus(row["status"])
            if previous not in (StepStatus.PENDING, StepStatus.RETRY) or row["claim_token"]:
                raise ConcurrencyError(f"Cannot claim step {step_id} from {previous.value}")
            old_attempt = int(row["attempt"])
            attempt = old_attempt + 1 if previous is StepStatus.RETRY or old_attempt == 0 else old_attempt
            if attempt > int(row["max_attempts"]):
                raise ConcurrencyError(f"Step {step_id} has exhausted its attempts")
            if row["available_at"] is not None and row["available_at"] > claimed_at:
                raise ConcurrencyError(f"Step {step_id} is not available until {row['available_at']}")
            iteration = int(row["iteration"])
            connection.execute(
                """
                UPDATE step_runs SET status=?,attempt=?,started_at=?,completed_at=NULL,
                    error=NULL,waiting_reason=NULL,claim_owner=?,claim_token=?,claimed_at=?,
                    lease_expires_at=?,heartbeat_at=? WHERE run_id=? AND step_id=?
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
                ),
            )
            existing = connection.execute(
                "SELECT 1 FROM step_attempts WHERE run_id=? AND step_id=? "
                "AND iteration=? AND attempt=?",
                (run_id, step_id, iteration, attempt),
            ).fetchone()
            if existing:
                connection.execute(
                    "UPDATE step_attempts SET started_at=?,completed_at=NULL,outcome=?,error=NULL "
                    "WHERE run_id=? AND step_id=? AND iteration=? AND attempt=?",
                    (
                        claimed_at,
                        StepStatus.RUNNING.value,
                        run_id,
                        step_id,
                        iteration,
                        attempt,
                    ),
                )
            else:
                connection.execute(
                    "INSERT INTO step_attempts(run_id,step_id,iteration,attempt,started_at,outcome) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        run_id,
                        step_id,
                        iteration,
                        attempt,
                        claimed_at,
                        StepStatus.RUNNING.value,
                    ),
                )
            self._event(
                connection,
                run_id,
                "step.runnable",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
            )
            self._event(
                connection,
                run_id,
                "step.claimed",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
                data={"worker_id": worker_id, "lease_expires_at": lease_expires_at},
            )
            self._event(
                connection,
                run_id,
                "step.started",
                step_id=step_id,
                attempt=attempt,
                iteration=iteration,
                data={"worker_id": worker_id},
            )
        return StepClaim(
            run_id,
            step_id,
            worker_id,
            token,
            attempt,
            iteration,
            claimed_at,
            lease_expires_at,
        )
