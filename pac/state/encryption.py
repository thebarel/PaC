from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Protocol

from ..errors import ConfigurationError, EncryptionError
from ..models import JsonValue


class EncryptionKeyProvider(Protocol):
    """Supplies encryption keys without storing key material in workflow state."""

    @property
    def active_key_id(self) -> str: ...

    def get_key(self, key_id: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class StaticEncryptionKeyProvider:
    """Small key provider useful for deployment wiring and tests.

    Applications should normally obtain these bytes from a KMS or secret manager.
    """

    keys: dict[str, bytes]
    active_key_id: str

    def get_key(self, key_id: str) -> bytes:
        try:
            key = self.keys[key_id]
        except KeyError as exc:
            raise EncryptionError(f"Encryption key {key_id!r} is not available") from exc
        if len(key) != 32:
            raise ConfigurationError("AES-256-GCM keys must contain exactly 32 bytes")
        return key


@dataclass(frozen=True, slots=True)
class EnvironmentEncryptionKeyProvider:
    """Reads base64-encoded AES keys from environment variables on demand."""

    active_key_id: str
    prefix: str = "PAC_ENCRYPTION_KEY_"

    def get_key(self, key_id: str) -> bytes:
        variable = f"{self.prefix}{key_id}"
        encoded = os.environ.get(variable)
        if encoded is None:
            raise EncryptionError(f"Encryption key environment variable {variable!r} is missing")
        try:
            key = base64.b64decode(encoded, validate=True)
        except ValueError as exc:
            raise EncryptionError(f"Encryption key {variable!r} is not valid base64") from exc
        if len(key) != 32:
            raise ConfigurationError("AES-256-GCM keys must contain exactly 32 bytes")
        return key


class PayloadCodec(Protocol):
    def encode(self, value: JsonValue, *, aad: str) -> str: ...

    def decode(self, value: str, *, aad: str) -> JsonValue: ...


class JsonPayloadCodec:
    """Canonical plaintext JSON codec used by default and for legacy databases."""

    def encode(self, value: JsonValue, *, aad: str) -> str:
        del aad
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)

    def decode(self, value: str, *, aad: str) -> JsonValue:
        del aad
        return json.loads(value)


class AESGCMEncryptionCodec:
    """Versioned AES-256-GCM envelopes with externally supplied key material.

    Plain JSON written before encryption was enabled remains readable. New writes use
    the provider's active key. Keeping old keys available permits gradual rotation;
    :meth:`reencrypt` rewrites a decoded value under the current active key.
    """

    marker = "pac.aesgcm.v1"

    def __init__(self, keys: EncryptionKeyProvider) -> None:
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
        except ImportError as exc:
            raise ConfigurationError(
                "Encrypted persistence requires the 'encryption' extra (cryptography)"
            ) from exc
        self.keys = keys

    @staticmethod
    def _canonical(value: JsonValue) -> bytes:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")

    def encode(self, value: JsonValue, *, aad: str) -> str:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        key_id = self.keys.active_key_id
        nonce = os.urandom(12)
        ciphertext = AESGCM(self.keys.get_key(key_id)).encrypt(
            nonce, self._canonical(value), aad.encode("utf-8")
        )
        return json.dumps(
            {
                "$pac": self.marker,
                "key_id": key_id,
                "nonce": base64.b64encode(nonce).decode("ascii"),
                "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def decode(self, value: str, *, aad: str) -> JsonValue:
        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        try:
            envelope = json.loads(value)
        except json.JSONDecodeError as exc:
            raise EncryptionError("Persisted payload is not valid JSON") from exc
        if not isinstance(envelope, dict) or envelope.get("$pac") != self.marker:
            return envelope
        try:
            key_id = str(envelope["key_id"])
            nonce = base64.b64decode(envelope["nonce"], validate=True)
            ciphertext = base64.b64decode(envelope["ciphertext"], validate=True)
            plaintext = AESGCM(self.keys.get_key(key_id)).decrypt(
                nonce, ciphertext, aad.encode("utf-8")
            )
            return json.loads(plaintext)
        except (KeyError, ValueError, InvalidTag, json.JSONDecodeError) as exc:
            raise EncryptionError(
                "Encrypted payload authentication failed or its envelope is invalid"
            ) from exc

    def reencrypt(self, value: str, *, aad: str) -> str:
        return self.encode(self.decode(value, aad=aad), aad=aad)
