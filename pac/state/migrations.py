from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Migration:
    """A durable schema migration identity recorded by relational stores."""

    version: int
    name: str

    @property
    def checksum(self) -> str:
        value = f"{self.version}:{self.name}".encode("utf-8")
        return hashlib.sha256(value).hexdigest()


# Migration bodies live in the backend because SQLite and PostgreSQL use
# different transactional DDL. These stable identities make partial upgrades
# and edited migration history detectable.
MIGRATIONS = (
    Migration(1, "legacy-core-schema"),
    Migration(2, "step-inputs-and-cycle-iterations"),
    Migration(3, "persisted-format-versions"),
    Migration(4, "provider-neutral-runtime-sessions"),
    Migration(5, "step-claims-and-leases"),
    Migration(6, "durable-waits-signals-humans-cancellation"),
    Migration(7, "idempotency-records-and-behavioral-fingerprints"),
    Migration(8, "agent-usage-event-export-and-workers"),
)

SCHEMA_VERSION = MIGRATIONS[-1].version
DEFINITION_FORMAT_VERSION = 2
STATE_FORMAT_VERSION = 1
EVENT_SCHEMA_VERSION = 1
