from __future__ import annotations

import argparse
import importlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .errors import ConfigurationError
from .state import AESGCMEncryptionCodec, EnvironmentEncryptionKeyProvider, SQLiteStateStore
from .workflow import Workflow


def _load(spec: str) -> Any:
    try:
        module_name, attribute = spec.split(":", 1)
        value = getattr(importlib.import_module(module_name), attribute)
    except (ValueError, ImportError, AttributeError) as exc:
        raise ConfigurationError(f"Could not load {spec!r}; expected module:attribute") from exc
    return value() if callable(value) and not isinstance(value, Workflow) else value


def _workflow(spec: str) -> Workflow:
    value = _load(spec)
    if not isinstance(value, Workflow):
        raise ConfigurationError(f"{spec!r} did not produce a Workflow")
    return value


def _json(value: Any) -> None:
    print(json.dumps(value, default=str, sort_keys=True, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pac", description="Operate durable PaC workflow runs")
    parser.add_argument("--db", default=".pac/state.db", help="SQLite state database")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("runs").add_argument("--workflow")
    for name in ("inspect", "events", "cancel"):
        command = commands.add_parser(name)
        command.add_argument("run_id")
        if name == "cancel":
            command.add_argument("--reason")
    signal = commands.add_parser("signal")
    signal.add_argument("run_id")
    signal.add_argument("name")
    signal.add_argument("--payload", default="null")
    signal.add_argument("--event-id")
    commands.add_parser("workers")
    rotate = commands.add_parser("rotate-key")
    rotate.add_argument("key_id", help="active key ID in PAC_ENCRYPTION_KEY_<id>")
    rotate.add_argument("--prefix", default="PAC_ENCRYPTION_KEY_")
    for name in ("validate", "resume", "retry", "worker"):
        command = commands.add_parser(name)
        command.add_argument("workflow", help="module:attribute yielding a Workflow")
        if name in ("resume", "retry", "worker"):
            command.add_argument("run_id")
        if name == "retry":
            command.add_argument("step")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = SQLiteStateStore(Path(args.db))
    if args.command == "rotate-key":
        provider = EnvironmentEncryptionKeyProvider(args.key_id, prefix=args.prefix)
        store = SQLiteStateStore(Path(args.db), payload_codec=AESGCMEncryptionCodec(provider))
    try:
        if args.command == "runs":
            _json([
                {"id": run.id, "workflow": run.name, "status": run.status.value, "created_at": run.created_at}
                for run in store.list_runs(args.workflow)
            ])
        elif args.command == "inspect":
            run = store.get_run(args.run_id)
            _json({
                "id": run.id,
                "workflow": run.name,
                "status": run.status.value,
                "fingerprint": run.definition_fingerprint,
                "steps": {key: value.status.value for key, value in run.steps.items()},
                "usage": asdict(store.usage(run.id)),
            })
        elif args.command == "events":
            _json([asdict(event) for event in store.get_run(args.run_id).events])
        elif args.command == "signal":
            receipt = store.signal(
                args.run_id, args.name, json.loads(args.payload), event_id=args.event_id
            )
            _json(asdict(receipt))
        elif args.command == "cancel":
            _json({"status": store.cancel_run(args.run_id, reason=args.reason).status.value})
        elif args.command == "workers":
            _json(store.list_workers())
        elif args.command == "rotate-key":
            _json({"rotated_payloads": store.rotate_encryption(), "key_id": args.key_id})
        elif args.command == "validate":
            workflow = _workflow(args.workflow)
            definition = workflow._definition()
            _json({"workflow": definition.name, "fingerprint": definition.fingerprint, "valid": True})
        elif args.command in ("resume", "worker"):
            workflow = _workflow(args.workflow)
            workflow.state_store = store
            run = workflow.resume(args.run_id)
            _json({"id": run.id, "status": run.status.value})
        elif args.command == "retry":
            workflow = _workflow(args.workflow)
            workflow.state_store = store
            run = store.get_run(args.run_id)
            state = run.steps.get(args.step)
            if state is None:
                raise ConfigurationError(f"Unknown step {args.step!r}")
            if state.status.value != "RETRY":
                raise ConfigurationError("Only a step already in RETRY state can be resumed safely")
            result = workflow.resume(args.run_id)
            _json({"id": result.id, "status": result.status.value})
        return 0
    except (ConfigurationError, ValueError, json.JSONDecodeError) as exc:
        print(f"pac: {exc}", file=sys.stderr)
        return 2
