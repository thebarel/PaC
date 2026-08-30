from __future__ import annotations

import sqlite3

import pytest

from pac import (
    AESGCMEncryptionCodec,
    EncryptionError,
    SQLiteStateStore,
    StaticEncryptionKeyProvider,
    Step,
    Workflow,
)

cryptography = pytest.importorskip("cryptography")


class SensitiveStep(Step):
    def run(self, ctx):
        return self.complete({"token": ctx.input("token")})


def _codec(key_id: str = "key-1", key: bytes = b"a" * 32):
    return AESGCMEncryptionCodec(StaticEncryptionKeyProvider({key_id: key}, key_id))


def test_sqlite_encrypts_payloads_but_not_scheduler_metadata(tmp_path):
    path = tmp_path / "encrypted.db"
    store = SQLiteStateStore(path, payload_codec=_codec())
    workflow = Workflow("encrypted", state_store=store)
    workflow.add_step(SensitiveStep, inputs={"token": "top-secret-value"})

    run = workflow.run()
    with sqlite3.connect(path) as connection:
        step = connection.execute(
            "SELECT step_id, status, inputs_json, output_json FROM step_runs WHERE run_id=?",
            (run.id,),
        ).fetchone()
        event_payloads = connection.execute(
            "SELECT data_json FROM events WHERE run_id=?", (run.id,)
        ).fetchall()

    assert step[0].endswith("SensitiveStep")
    assert step[1] == "COMPLETED"
    assert "top-secret-value" not in step[2]
    assert "top-secret-value" not in step[3]
    assert all("top-secret-value" not in row[0] for row in event_payloads)
    assert run.output(SensitiveStep) == {"token": "top-secret-value"}
    assert SQLiteStateStore(path, payload_codec=_codec()).get_run(run.id).output(
        SensitiveStep
    ) == {"token": "top-secret-value"}


def test_encrypted_payload_authentication_and_key_rotation(tmp_path):
    path = tmp_path / "rotation.db"
    old = _codec("old", b"o" * 32)
    store = SQLiteStateStore(path, payload_codec=old)
    workflow = Workflow("rotation", state_store=store)
    workflow.add_step(SensitiveStep, inputs={"token": "secret"})
    run = workflow.run()

    rotated = AESGCMEncryptionCodec(
        StaticEncryptionKeyProvider({"old": b"o" * 32, "new": b"n" * 32}, "new")
    )
    rotated_store = SQLiteStateStore(path, payload_codec=rotated)
    assert rotated_store.get_run(run.id).output(SensitiveStep) == {"token": "secret"}
    assert rotated_store.rotate_encryption() > 0

    wrong = AESGCMEncryptionCodec(StaticEncryptionKeyProvider({"new": b"n" * 32}, "new"))
    assert SQLiteStateStore(path, payload_codec=wrong).get_run(run.id).output(
        SensitiveStep
    ) == {"token": "secret"}

    missing = AESGCMEncryptionCodec(StaticEncryptionKeyProvider({"other": b"x" * 32}, "other"))
    with pytest.raises(EncryptionError, match="not available"):
        SQLiteStateStore(path, payload_codec=missing).get_run(run.id)


def test_aad_prevents_ciphertext_relocation():
    codec = _codec()
    envelope = codec.encode({"secret": 1}, aad="table:field:run-a")

    with pytest.raises(EncryptionError, match="authentication failed"):
        codec.decode(envelope, aad="table:field:run-b")
