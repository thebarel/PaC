# Security

## Secrets

Use `SecretRef` and a `SecretProvider`; never place a resolved credential in ordinary workflow state.

```python
workflow.add_step(CallAPI, inputs={"api_key": SecretRef("SERVICE_API_KEY")})

value = ctx.secrets.get(ctx.input("api_key"))
client = Client(token=value.reveal())
```

The environment provider is a convenience, not a vault. Custom providers can integrate AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault, Kubernetes Secrets, or an application service. `SecretValue.__str__` and `repr` redact the value, but `.reveal()` necessarily creates plaintext for execution.

Avoid secrets in:

- workflow, step, signal, idempotency-action, or actor names;
- prompts and model output unless the provider is explicitly trusted for them;
- exception text and manual log messages;
- external command arguments visible to the OS;
- explicit semantic version strings.

PaC sanitizes documented structured event paths and excludes prompts by default. It cannot sanitize arbitrary logging performed by workflow/provider code.

## Payload encryption threat model

Optional `AESGCMEncryptionCodec` uses `cryptography`'s AES-256-GCM. A random 96-bit nonce is generated for every write. Authenticated associated data binds ciphertext to its logical storage location. Envelopes record format and key ID, never key bytes.

Encrypted payload categories include inputs, outputs, rejected candidates, event data, signal and human payloads/actors, runtime sessions, and idempotency results. Metadata required to find work remains plaintext: workflow/run/step identity, statuses, event types and sequence, timestamps, attempts, leases, deadlines, and usage counters. `SQLiteStateStore.rotate_encryption()` rewrites all opaque payload columns under the active key; `pac rotate-key KEY_ID` exposes that operation for environment-backed keys.

Encryption is useful for lost database files/backups and database readers who do not possess the external key. It does not protect against:

- application code or a worker that can ask the key provider;
- plaintext while a step/provider is executing;
- memory inspection;
- sensitive metadata embedded in names/statuses/timing;
- an attacker who has both database and key-provider access;
- data copied into an unencrypted log or external system.

## Key management and rotation

Keys must be exactly 32 random bytes and should come from a KMS/secret manager. Do not hard-code production keys or store them next to the database.

`EnvironmentEncryptionKeyProvider` expects base64 under `PAC_ENCRYPTION_KEY_<key_id>`. New writes use `active_key_id`. Decryption selects the envelope's old key ID, so retain old keys until every old row and backup has aged out or been explicitly re-encrypted. `AESGCMEncryptionCodec.reencrypt()` is the primitive for rotation tooling; schedule rotation with backups and verification.

Losing an old key makes its payloads unrecoverable. Reusing a key across unrelated trust domains increases blast radius.

## Signals and approvals

PaC stores actor metadata but does not authenticate it. The surrounding CLI/server/UI must authenticate and authorize the caller before invoking `signal`, `approve`, `reject`, `cancel`, or operational retry/resume APIs.

Use provider event IDs as idempotency keys and verify webhook signatures before calling PaC. Validate payload types and size in the integration layer as well as the workflow contract.

## Workers and databases

Protect database credentials and network transport. Restrict workers to definitions they should execute. A `WorkflowRegistry` prevents reconstruction of executable code from database data, but loading `module:attribute` through the CLI executes trusted local Python.

For PostgreSQL, use TLS, least-privilege roles, supported isolation/locking behavior, and test the integration suite against the real deployment topology. For SQLite, protect database and WAL files together and use a filesystem with reliable locking.

## External effects

PaC cannot provide universal exactly-once effects. Prefer remote APIs that accept idempotency keys. A local `once()` record reduces duplicates but cannot atomically commit with an unrelated service. Design reconciliation for ambiguous outcomes after crashes and timeouts.

Cancellation and timeout do not prove that a remote operation stopped. Treat late responses and stale claim rejection as state safety, not external rollback.
