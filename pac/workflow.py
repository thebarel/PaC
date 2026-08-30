from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, get_type_hints

from .codecs import canonical_json_value, from_json_value, schema_identity, validate_and_encode
from .context import StepContext
from .errors import (
    ConcurrencyError,
    StepExecutionError,
    StepOutputSerializationError,
    ValidationError,
    WorkflowCycleError,
    WorkflowDeadlockError,
    WorkflowDefinitionChanged,
    WorkflowDefinitionError,
    WorkflowFailed,
)
from .fingerprint import FINGERPRINT_FORMAT_VERSION, implementation_identity, validator_identity
from .human import HumanApproval
from .idempotency import IdempotencyManager
from .models import (
    ConditionalDependency,
    CycleDefinition,
    CycleStatus,
    HumanTask,
    JsonValue,
    SignalReceipt,
    StepDefinition,
    StepStatus,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
    step_identity,
)
from .results import StepResult
from .runtime import AgentExecutionContext, AgentRuntime, BoundAgent, CodexRuntime
from .scheduler import runnable_steps
from .secrets import EnvironmentSecretProvider, SecretContext, SecretProvider, SecretResolver
from .state.base import StateStore
from .state.sqlite import SQLiteStateStore
from .step import Step

if TYPE_CHECKING:
    from openai_codex import Sandbox

logger = logging.getLogger(__name__)

RuntimeFactory = Callable[..., AbstractContextManager[Any]]


class Workflow:
    """A declarative process reconciled against durable runtime state."""

    def __init__(
        self,
        name: str,
        *,
        cwd: str | Path = ".",
        model: str | None = None,
        sandbox: Sandbox | None = None,
        state_path: str | Path | None = None,
        state_store: StateStore | None = None,
        agent_runtime: AgentRuntime | None = None,
        secret_provider: SecretProvider | None = None,
        max_concurrency: int = 1,
        worker_id: str | None = None,
        lease_duration: timedelta = timedelta(minutes=2),
        step_timeout: timedelta | None = None,
        workflow_timeout: timedelta | None = None,
        codex_runtime_factory: RuntimeFactory = CodexRuntime,
        version: str | None = None,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise WorkflowDefinitionError("Workflow name must be a non-empty string")
        if state_store is not None and state_path is not None:
            raise WorkflowDefinitionError("Pass either state_store or state_path, not both")
        if version is not None and (not isinstance(version, str) or not version.strip()):
            raise WorkflowDefinitionError("Workflow version must be a non-empty string")
        self.name = name
        self.version = version
        self.cwd = Path(cwd).resolve()
        self.model = model
        self.sandbox = sandbox
        self.state_store = state_store or SQLiteStateStore(
            state_path or self.cwd / ".pac" / "state.db"
        )
        self._runtime_factory = codex_runtime_factory
        self._agent_runtime = agent_runtime
        self._secret_provider = secret_provider or EnvironmentSecretProvider()
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int) or max_concurrency < 1:
            raise WorkflowDefinitionError("max_concurrency must be an integer >= 1")
        if lease_duration.total_seconds() <= 0:
            raise WorkflowDefinitionError("lease_duration must be positive")
        self.max_concurrency = max_concurrency
        self.worker_id = worker_id or f"local-{id(self):x}"
        if step_timeout is not None and step_timeout.total_seconds() <= 0:
            raise WorkflowDefinitionError("step_timeout must be positive")
        if workflow_timeout is not None and workflow_timeout.total_seconds() <= 0:
            raise WorkflowDefinitionError("workflow_timeout must be positive")
        self.lease_duration = lease_duration
        self.step_timeout = step_timeout
        self.workflow_timeout = workflow_timeout
        self._run_id: str | None = None
        self._registrations: list[
            tuple[type[Step], tuple[type[Step] | ConditionalDependency, ...], JsonValue]
        ] = []
        self._cycles: list[
            tuple[str, tuple[type[Step], ...], tuple[type[Step], type[Step]], int]
        ] = []

    def add_step(
        self,
        step: type[Step],
        *,
        depends_on: (
            list[type[Step] | ConditionalDependency]
            | tuple[type[Step] | ConditionalDependency, ...]
            | None
        ) = None,
        inputs: Any = None,
    ) -> Workflow:
        """Register a Step class, dependencies, and durable JSON inputs."""

        if not isinstance(step, type) or not issubclass(step, Step):
            raise WorkflowDefinitionError("Workflow.add_step() requires a Step class")
        canonical_inputs = self._canonical_inputs(inputs, step)
        self._registrations.append((step, tuple(depends_on or ()), canonical_inputs))
        return self

    def add_cycle(
        self,
        name: str,
        *,
        steps: list[type[Step]] | tuple[type[Step], ...],
        back_edge: tuple[type[Step], type[Step]],
        max_iterations: int,
    ) -> Workflow:
        """Declare an opt-in cycle whose controller may request another pass."""

        if not isinstance(name, str) or not name.strip():
            raise WorkflowDefinitionError("Cycle name must be a non-empty string")
        if not isinstance(max_iterations, int) or isinstance(max_iterations, bool) or max_iterations < 1:
            raise WorkflowDefinitionError("Cycle max_iterations must be an integer >= 1")
        if not isinstance(steps, (list, tuple)) or not steps:
            raise WorkflowDefinitionError("Cycle steps must be a non-empty list of Step classes")
        if not isinstance(back_edge, tuple) or len(back_edge) != 2:
            raise WorkflowDefinitionError("Cycle back_edge must be a (Controller, Entry) tuple")
        if any(not isinstance(endpoint, type) or not issubclass(endpoint, Step) for endpoint in back_edge):
            raise WorkflowDefinitionError("Cycle back_edge endpoints must be Step classes")
        self._cycles.append((name.strip(), tuple(steps), back_edge, max_iterations))
        return self

    @staticmethod
    def _canonical_inputs(
        inputs: Any,
        step: type[Step],
    ) -> JsonValue:
        if inputs is None:
            return {}
        input_type = getattr(step, "input_type", Any)
        if input_type in (Any, object) and not isinstance(inputs, Mapping):
            raise WorkflowDefinitionError(
                f"Inputs for {step_identity(step)} must be a mapping with string keys"
            )
        Workflow._validate_input_keys(inputs, step_identity(step))
        try:
            _, canonical = validate_and_encode(
                inputs,
                input_type,
                path=f"inputs for {step_identity(step)}",
            )
        except ValidationError as exc:
            raise WorkflowDefinitionError(f"Inputs for {step_identity(step)} are invalid: {exc}") from exc
        if input_type in (Any, object) and not isinstance(canonical, dict):
            raise WorkflowDefinitionError(
                f"Inputs for {step_identity(step)} must be a JSON object"
            )
        return canonical

    @staticmethod
    def _validate_input_keys(value: Any, step_id: str, path: str = "inputs") -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if not isinstance(key, str):
                    raise WorkflowDefinitionError(
                        f"Inputs for {step_id} must use string keys; "
                        f"found {key!r} at {path}"
                    )
                Workflow._validate_input_keys(child, step_id, f"{path}.{key}")
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                Workflow._validate_input_keys(child, step_id, f"{path}[{index}]")

    def _definition(self) -> WorkflowDefinition:
        if any(
            not isinstance(step, type) or not issubclass(step, Step)
            for step, _, _ in self._registrations
        ):
            raise WorkflowDefinitionError("Every registered step must be a Step class")

        ids = [step_identity(step) for step, _, _ in self._registrations]
        duplicates = sorted({step_id for step_id in ids if ids.count(step_id) > 1})
        if duplicates:
            raise WorkflowDefinitionError(f"Duplicate step IDs: {', '.join(duplicates)}")
        order = {step_id: index for index, step_id in enumerate(ids)}

        definitions: list[StepDefinition] = []
        for index, (step_class, dependencies, inputs) in enumerate(self._registrations):
            max_attempts = getattr(step_class, "max_attempts", 1)
            if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
                raise WorkflowDefinitionError(
                    f"Step {step_identity(step_class)} max_attempts must be an integer >= 1"
                )
            dependency_ids: list[str] = []
            dependency_conditions: dict[str, str] = {}
            for dependency in dependencies:
                condition: str | None = None
                dependency_class: Any = dependency
                if isinstance(dependency, ConditionalDependency):
                    dependency_class = dependency.step
                    condition = dependency.outcome
                    if condition not in {"approved", "rejected", "timed_out"}:
                        raise WorkflowDefinitionError(
                            f"Unsupported human dependency outcome {condition!r}"
                        )
                if not isinstance(dependency_class, type) or not issubclass(dependency_class, Step):
                    raise WorkflowDefinitionError(
                        f"Dependency of {step_identity(step_class)} is not a Step class"
                    )
                if condition is not None and not issubclass(dependency_class, HumanApproval):
                    raise WorkflowDefinitionError(
                        "Conditional dependencies require a HumanApproval step"
                    )
                dependency_id = step_identity(dependency_class)
                if dependency_id not in order:
                    raise WorkflowDefinitionError(
                        f"Step {step_identity(step_class)} depends on unregistered step "
                        f"{dependency_id}"
                    )
                previous = dependency_conditions.get(dependency_id)
                if previous is not None and previous != condition:
                    raise WorkflowDefinitionError(
                        f"Step {step_identity(step_class)} has conflicting conditions for {dependency_id}"
                    )
                if dependency_id not in dependency_ids:
                    dependency_ids.append(dependency_id)
                if condition is not None:
                    dependency_conditions[dependency_id] = condition
            dependency_ids.sort(key=order.__getitem__)
            signature = inspect.signature(step_class.run)
            positional = [
                parameter
                for parameter in signature.parameters.values()
                if parameter.kind
                in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
            ]
            if len(positional) not in (2, 3):
                raise WorkflowDefinitionError(
                    f"Step {step_identity(step_class)} run() must accept (self, ctx) "
                    "or (self, ctx, inputs)"
                )
            accepts_typed_input = len(positional) == 3
            input_type = getattr(step_class, "input_type", Any)
            output_type = getattr(step_class, "output_type", Any)
            if accepts_typed_input and input_type in (Any, object):
                try:
                    annotation = get_type_hints(step_class.run).get(
                        positional[2].name, positional[2].annotation
                    )
                except (NameError, TypeError):
                    annotation = positional[2].annotation
                if annotation is not inspect.Parameter.empty:
                    input_type = annotation
            definitions.append(
                StepDefinition(
                    id=step_identity(step_class),
                    step_class=step_class,
                    registration_order=index,
                    dependencies=tuple(dependency_ids),
                    max_attempts=max_attempts,
                    inputs=deepcopy(inputs),
                    input_type=input_type,
                    output_type=output_type,
                    accepts_typed_input=accepts_typed_input,
                    dependency_conditions=tuple(
                        (dependency_id, dependency_conditions[dependency_id])
                        for dependency_id in dependency_ids
                        if dependency_id in dependency_conditions
                    ),
                )
            )

        self._validate_acyclic(tuple(definitions))
        cycle_definitions = self._cycle_definitions(tuple(definitions))
        sandbox_value = getattr(self.sandbox, "value", self.sandbox)
        canonical = {
            "fingerprint_format": FINGERPRINT_FORMAT_VERSION,
            "workflow_name": self.name,
            "workflow_version": self.version,
            "cwd": str(self.cwd),
            "model": self.model,
            "sandbox": sandbox_value,
            "timeouts": {
                "step_seconds": self.step_timeout.total_seconds() if self.step_timeout else None,
                "workflow_seconds": self.workflow_timeout.total_seconds() if self.workflow_timeout else None,
            },
            "agent_runtime": (
                f"{type(self._agent_runtime).__module__}.{type(self._agent_runtime).__qualname__}"
                if self._agent_runtime is not None
                else f"{self._runtime_factory.__module__}.{self._runtime_factory.__qualname__}"
            ),
            "steps": [
                {
                    "id": step.id,
                    "registration_order": step.registration_order,
                    "dependencies": list(step.dependencies),
                    "dependency_conditions": [list(item) for item in step.dependency_conditions],
                    "max_attempts": step.max_attempts,
                    "implementation": implementation_identity(step.step_class),
                    "input_schema": schema_identity(step.input_type),
                    "output_schema": schema_identity(step.output_type),
                    "human": (
                        {
                            "timeout_seconds": step.step_class.timeout.total_seconds()
                            if step.step_class.timeout
                            else None,
                            "timeout_action": step.step_class.timeout_action.value,
                            "route_timeout": step.step_class.route_timeout,
                            "payload_schema": schema_identity(step.step_class.payload_type),
                        }
                        if issubclass(step.step_class, HumanApproval)
                        else None
                    ),
                    "validation": {
                        "validate_output": implementation_identity(
                            step.step_class.validate_output,
                            explicit_version=getattr(step.step_class, "validator_version", None),
                        ),
                        "validators": [
                            validator_identity(validator)
                            for validator in getattr(step.step_class, "validators", ())
                        ],
                    },
                    **({"inputs": step.inputs} if step.inputs else {}),
                }
                for step in definitions
            ],
            **(
                {
                    "cycles": [
                        {
                            "name": cycle.name,
                            "members": list(cycle.members),
                            "controller": cycle.controller,
                            "entry": cycle.entry,
                            "max_iterations": cycle.max_iterations,
                        }
                        for cycle in cycle_definitions
                    ]
                }
                if cycle_definitions
                else {}
            ),
        }
        try:
            canonical_json = json.dumps(
                canonical, sort_keys=True, separators=(",", ":"), allow_nan=False
            )
        except (TypeError, ValueError) as exc:
            raise WorkflowDefinitionError(f"Workflow configuration is not serializable: {exc}") from exc
        return WorkflowDefinition(
            name=self.name,
            steps=tuple(definitions),
            fingerprint=hashlib.sha256(canonical_json.encode("utf-8")).hexdigest(),
            canonical_json=canonical_json,
            cycles=cycle_definitions,
        )

    def _cycle_definitions(
        self, steps: tuple[StepDefinition, ...]
    ) -> tuple[CycleDefinition, ...]:
        registered = {step.id for step in steps}
        definitions = {step.id: step for step in steps}
        order = {step.id: step.registration_order for step in steps}
        names: set[str] = set()
        claimed: set[str] = set()
        cycles: list[CycleDefinition] = []

        for name, member_classes, back_edge, max_iterations in self._cycles:
            if name in names:
                raise WorkflowDefinitionError(f"Duplicate cycle name {name!r}")
            names.add(name)
            if any(not isinstance(member, type) or not issubclass(member, Step) for member in member_classes):
                raise WorkflowDefinitionError(f"Cycle {name!r} members must be Step classes")
            members = tuple(dict.fromkeys(step_identity(member) for member in member_classes))
            if len(members) != len(member_classes):
                raise WorkflowDefinitionError(f"Cycle {name!r} contains duplicate members")
            missing = [member for member in members if member not in registered]
            if missing:
                raise WorkflowDefinitionError(
                    f"Cycle {name!r} contains unregistered steps: {', '.join(missing)}"
                )
            overlap = sorted(set(members) & claimed)
            if overlap:
                raise WorkflowDefinitionError(
                    f"Cycle {name!r} overlaps another cycle: {', '.join(overlap)}"
                )
            controller, entry = (step_identity(back_edge[0]), step_identity(back_edge[1]))
            if controller not in members or entry not in members:
                raise WorkflowDefinitionError(
                    f"Cycle {name!r} back edge endpoints must both be cycle members"
                )
            if controller in definitions[entry].dependencies:
                raise WorkflowDefinitionError(
                    f"Cycle {name!r} feedback edge must be declared only with add_cycle(), not depends_on"
                )

            member_set = set(members)
            outgoing: dict[str, list[str]] = {member: [] for member in members}
            for member in members:
                for dependency in definitions[member].dependencies:
                    if dependency in member_set:
                        outgoing[dependency].append(member)

            def reachable(start: str) -> set[str]:
                seen: set[str] = set()
                pending = [start]
                while pending:
                    current = pending.pop()
                    if current in seen:
                        continue
                    seen.add(current)
                    pending.extend(outgoing[current])
                return seen

            entry_reachable = reachable(entry)
            cannot_reach_controller = [
                member for member in members if controller not in reachable(member)
            ]
            if entry_reachable != member_set or cannot_reach_controller:
                raise WorkflowDefinitionError(
                    f"Cycle {name!r} must have an entry that reaches every member and a controller reachable from every member"
                )
            canonical_members = tuple(sorted(members, key=order.__getitem__))
            claimed.update(members)
            cycles.append(
                CycleDefinition(name, canonical_members, controller, entry, max_iterations)
            )
        return tuple(cycles)

    @staticmethod
    def _validate_acyclic(steps: tuple[StepDefinition, ...]) -> None:
        dependencies = {step.id: step.dependencies for step in steps}
        visiting: list[str] = []
        visited: set[str] = set()

        def visit(step_id: str) -> None:
            if step_id in visiting:
                start = visiting.index(step_id)
                cycle = visiting[start:] + [step_id]
                raise WorkflowCycleError(f"Workflow dependency cycle: {' -> '.join(cycle)}")
            if step_id in visited:
                return
            visiting.append(step_id)
            for dependency in dependencies[step_id]:
                visit(dependency)
            visiting.pop()
            visited.add(step_id)

        for step in steps:
            visit(step.id)

    @staticmethod
    def _canonical_output(value: Any, step_id: str) -> JsonValue:
        try:
            encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
            return json.loads(encoded)
        except (TypeError, ValueError) as exc:
            raise StepOutputSerializationError(
                f"Step {step_id} returned a non-JSON-serializable output: {exc}"
            ) from exc

    def start(self, *, run_id: str | None = None) -> WorkflowRun:
        """Create an isolated persisted run without executing it."""

        definition = self._definition()
        run = (
            self.state_store.create_run(definition)
            if run_id is None
            else self.state_store.create_run(definition, run_id=run_id)
        )
        self._run_id = run.id
        return run

    def signal(
        self,
        run_id: str,
        name: str,
        payload: Any = None,
        *,
        event_id: str | None = None,
        actor: Any = None,
    ) -> SignalReceipt:
        canonical_payload = canonical_json_value(payload, path="signal payload")
        canonical_actor = canonical_json_value(actor, path="signal actor") if actor is not None else None
        return self.state_store.signal(
            run_id, name, canonical_payload, event_id=event_id, actor=canonical_actor
        )

    def cancel(
        self, run_id: str, *, reason: str | None = None, actor: Any = None
    ) -> WorkflowRun:
        canonical_actor = canonical_json_value(actor, path="cancellation actor") if actor is not None else None
        return self.state_store.cancel_run(run_id, reason=reason, actor=canonical_actor)

    def approve(
        self,
        run_id: str,
        step: type[Step] | str,
        *,
        payload: Any = None,
        comment: str | None = None,
        actor: Any = None,
        event_id: str | None = None,
    ) -> HumanTask:
        return self._human_response(
            run_id, step, "APPROVED", payload, comment, actor, event_id
        )

    def reject(
        self,
        run_id: str,
        step: type[Step] | str,
        *,
        reason: str | None = None,
        payload: Any = None,
        actor: Any = None,
        event_id: str | None = None,
    ) -> HumanTask:
        return self._human_response(
            run_id, step, "REJECTED", payload, reason, actor, event_id
        )

    def _human_response(
        self,
        run_id: str,
        step: type[Step] | str,
        decision: str,
        payload: Any,
        comment: str | None,
        actor: Any,
        event_id: str | None,
    ) -> HumanTask:
        step_id = step_identity(step)
        definition = self._definition()
        step_definition = next((item for item in definition.steps if item.id == step_id), None)
        if step_definition is None or not issubclass(step_definition.step_class, HumanApproval):
            raise WorkflowDefinitionError(f"Step {step_id!r} is not a HumanApproval")
        payload_type = getattr(step_definition.step_class, "payload_type", Any)
        if payload_type in (Any, object):
            canonical_payload = canonical_json_value(payload, path="human response payload")
        else:
            _, canonical_payload = validate_and_encode(
                payload, payload_type, path="human response payload"
            )
        canonical_actor = canonical_json_value(actor, path="human response actor") if actor is not None else None
        return self.state_store.respond_human(
            run_id, step_id, decision, payload=canonical_payload, comment=comment,
            actor=canonical_actor, event_id=event_id,
        )

    def run(self, *, run_id: str | None = None) -> WorkflowRun:
        """Create and execute a new run, independent of other active runs."""

        return self._run_sync(self.arun(run_id=run_id))

    async def arun(self, *, run_id: str | None = None) -> WorkflowRun:
        """Create and execute a new run using native async scheduling."""

        return await self.aloop(run_id=self.start(run_id=run_id).id)

    def resume(self, run_id: str) -> WorkflowRun:
        """Resume a specific persisted run."""

        return self._run_sync(self.aresume(run_id))

    async def aresume(self, run_id: str) -> WorkflowRun:
        """Resume a specific persisted run using native async scheduling."""

        return await self.aloop(run_id=run_id)

    @staticmethod
    def _run_sync(awaitable: Any) -> WorkflowRun:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(awaitable)
        if inspect.iscoroutine(awaitable):
            awaitable.close()
        raise RuntimeError("Synchronous workflow methods cannot run inside an active event loop; use arun(), aresume(), or aloop()")

    def loop(self, run_id: str | None = None) -> WorkflowRun:
        return self._run_sync(self.aloop(run_id=run_id))

    async def aloop(self, run_id: str | None = None) -> WorkflowRun:
        """Reconcile one run until it becomes terminal or durably waiting.

        ``run_id`` is the unambiguous restart-safe API. For compatibility, a
        workflow instance remembers the run it created; a newly constructed
        instance resumes only when the store contains exactly one active run
        with this workflow name.
        """

        definition = self._definition()
        definitions_by_id = {step.id: step for step in definition.steps}

        with self.state_store.execution_lock(self.name):
            selected_id = run_id or self._run_id
            if selected_id is not None:
                run = self.state_store.get_run(selected_id)
                if (
                    run.status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED)
                    and run_id is None
                ):
                    run = self.state_store.create_run(definition)
            else:
                active_run = self.state_store.active_run(self.name)
                run = (
                    active_run
                    if active_run is not None
                    else self.state_store.create_run(definition)
                )
            if run.name != self.name:
                raise WorkflowDefinitionChanged(
                    f"Run {run.id} belongs to workflow {run.name!r}, not {self.name!r}"
                )
            if run.definition_fingerprint != definition.fingerprint:
                raise WorkflowDefinitionChanged(
                    f"Workflow {self.name!r} definition changed for unfinished run {run.id}: "
                    f"stored={run.definition_fingerprint}, current={definition.fingerprint}"
                )
            self._run_id = run.id

            if run.status is WorkflowStatus.WAITING:
                self.state_store.process_due_waits()
                current = self.state_store.get_run(run.id)
                if current.status is WorkflowStatus.WAITING:
                    self.state_store.resume_waiting(run.id)
            else:
                self.state_store.recover_expired_claims()
                self.state_store.recover_running(run.id)

            run = self.state_store.get_run(run.id)
            if run.status is WorkflowStatus.FAILED:
                raise WorkflowFailed(run.error or f"Workflow {self.name!r} failed", run)
            if run.status is WorkflowStatus.CANCELLED:
                return run
            if run.status is WorkflowStatus.WAITING:
                return run
            if run.status is WorkflowStatus.PENDING:
                self.state_store.start_workflow(run.id)

            runtime_manager = (
                nullcontext(self._agent_runtime)
                if self._agent_runtime is not None
                else self._runtime_factory(
                    store=self.state_store,
                    run_id=run.id,
                    cwd=self.cwd,
                    model=self.model,
                    sandbox=self.sandbox,
                )
            )
            with runtime_manager as runtime:
                while True:
                    run = self.state_store.get_run(run.id)
                    if self.workflow_timeout is not None and run.started_at is not None:
                        from datetime import datetime

                        deadline = datetime.fromisoformat(run.started_at) + self.workflow_timeout
                        if datetime.now(deadline.tzinfo) >= deadline:
                            self.state_store.fail_workflow(
                                run.id,
                                f"Workflow timed out after {self.workflow_timeout.total_seconds()} seconds",
                            )
                            timed_out = self.state_store.get_run(run.id)
                            raise WorkflowFailed(timed_out.error or "Workflow timed out", timed_out)
                    failed = next(
                        (state for state in run.steps.values() if state.status is StepStatus.FAILED),
                        None,
                    )
                    if run.status is WorkflowStatus.CANCELLED:
                        return run
                    if failed is not None or run.status is WorkflowStatus.FAILED:
                        if run.status is not WorkflowStatus.FAILED:
                            if failed is None:
                                raise AssertionError("A non-failed workflow must have a failed step")
                            self.state_store.fail_workflow(
                                run.id, f"Step {failed.id} failed: {failed.error}"
                            )
                            run = self.state_store.get_run(run.id)
                        raise WorkflowFailed(run.error or f"Workflow {self.name!r} failed", run)

                    skipped = self._skip_unselected_human_routes(definition, run)
                    if skipped:
                        continue

                    if all(
                        state.status in (StepStatus.COMPLETED, StepStatus.SKIPPED)
                        for state in run.steps.values()
                    ) and all(cycle.status is CycleStatus.COMPLETED for cycle in run.cycles.values()):
                        self.state_store.complete_workflow(run.id)
                        return self.state_store.get_run(run.id)

                    ready = runnable_steps(definition, run.steps, run.cycles)
                    if ready:
                        claims = []
                        for step_id in ready[: self.max_concurrency]:
                            try:
                                claims.append(
                                    self.state_store.claim_step(
                                        run.id,
                                        step_id,
                                        self.worker_id,
                                        lease_duration=self.lease_duration,
                                    )
                                )
                            except ConcurrencyError:
                                continue
                        if claims:
                            results = await asyncio.gather(
                                *(
                                    self._execute_claim(
                                        definition,
                                        definitions_by_id[claim.step_id],
                                        claim,
                                        runtime,
                                    )
                                    for claim in claims
                                ),
                                return_exceptions=True,
                            )
                            failures = [
                                result
                                for result in results
                                if isinstance(result, BaseException)
                            ]
                            current = self.state_store.get_run(run.id)
                            if current.status is WorkflowStatus.CANCELLED:
                                return current
                            if failures:
                                raise failures[0]
                            if current.status is WorkflowStatus.FAILED:
                                raise WorkflowFailed(
                                    current.error or f"Workflow {self.name!r} failed", current
                                )
                            continue
                        # Claims may have been won by another worker after this
                        # scheduler snapshot was read. Reload before deciding that
                        # the persisted graph is deadlocked.
                        run = self.state_store.get_run(run.id)

                    if any(
                        state.status is StepStatus.RUNNING for state in run.steps.values()
                    ):
                        # Another worker owns progress. Returning avoids polling and,
                        # importantly, does not misclassify active work as deadlock.
                        return run

                    waiting = [
                        state.id
                        for state in run.steps.values()
                        if state.status is StepStatus.WAITING
                    ]
                    if waiting:
                        self.state_store.wait_workflow(run.id)
                        return self.state_store.get_run(run.id)

                    blocked = [
                        state.id
                        for state in run.steps.values()
                        if state.status is not StepStatus.COMPLETED
                    ]
                    error = f"Workflow deadlock; blocked steps: {', '.join(blocked)}"
                    self.state_store.fail_workflow(run.id, error)
                    raise WorkflowDeadlockError(error)

    def _skip_unselected_human_routes(
        self, definition: WorkflowDefinition, run: WorkflowRun
    ) -> bool:
        changed = False
        for step in definition.steps:
            state = run.steps[step.id]
            if state.status is not StepStatus.PENDING or not step.dependency_conditions:
                continue
            for dependency_id, expected in step.dependency_conditions:
                dependency = run.steps[dependency_id]
                if dependency.status is not StepStatus.COMPLETED:
                    continue
                output = dependency.output
                actual = output.get("decision") if isinstance(output, dict) else None
                if actual != expected:
                    self.state_store.skip_step(
                        run.id,
                        step.id,
                        reason=(
                            f"Human route {dependency_id} selected {actual!r}, "
                            f"not {expected!r}"
                        ),
                    )
                    changed = True
                    break
        return changed

    async def _execute_claim(
        self,
        definition: WorkflowDefinition,
        step_definition: StepDefinition,
        claim: Any,
        runtime: AgentRuntime,
    ) -> None:
        step_id = claim.step_id
        current_run_id = claim.run_id

        def output_for_step(step: type[Any] | str) -> JsonValue:
            return self._context_output(definition, current_run_id, step)

        def current_state() -> WorkflowRun:
            return self.state_store.get_run(current_run_id)

        def latest_output_for_step(step: type[Any] | str) -> tuple[bool, JsonValue]:
            return self._context_latest_output(definition, current_run_id, step)

        secrets = SecretResolver(
            self._secret_provider,
            SecretContext(self.name, current_run_id, step_id),
        )
        agent_context = AgentExecutionContext(
            workflow_id=self.name,
            run_id=current_run_id,
            step_id=step_id,
            attempt=claim.attempt,
            iteration=claim.iteration,
            secrets=secrets,
        )
        agent = BoundAgent(runtime, agent_context, self.state_store)
        codex = runtime.bind_step(step_id) if hasattr(runtime, "bind_step") else agent
        decoded_inputs = from_json_value(
            deepcopy(step_definition.inputs),
            step_definition.input_type,
            path=f"inputs for {step_id}",
        )
        public_inputs = (
            MappingProxyType(deepcopy(decoded_inputs))
            if isinstance(decoded_inputs, dict)
            else decoded_inputs
        )
        context = StepContext(
            workflow_id=self.name,
            run_id=current_run_id,
            step_id=step_id,
            attempt=claim.attempt,
            iteration=claim.iteration,
            codex=codex,
            agent=agent,
            secrets=secrets,
            idempotency=IdempotencyManager(
                self.state_store,
                current_run_id,
                step_id,
                claim.iteration,
                claim.attempt,
            ),
            retry_reason=self.state_store.get_run(current_run_id).steps[step_id].retry_reason,
            inputs=public_inputs,
            _output=output_for_step,
            _latest_output=latest_output_for_step,
            _state=current_state,
            signal_payload=self.state_store.get_run(current_run_id).steps[step_id].signal_payload,
        )
        logger.info(
            "Step started",
            extra={
                "workflow_name": self.name,
                "run_id": current_run_id,
                "step_id": step_id,
                "attempt": claim.attempt,
                "state_transition": "RUNNING",
            },
        )
        step_instance = step_definition.step_class()
        if isinstance(step_instance, HumanApproval):
            routed = {
                condition
                for candidate in definition.steps
                for dependency_id, condition in candidate.dependency_conditions
                if dependency_id == step_id
            }
            step_instance._pac_routed_outcomes = frozenset(routed)
        try:
            def call() -> Any:
                if step_definition.accepts_typed_input:
                    return step_instance.run(context, decoded_inputs)
                return step_instance.run(context)

            async def invoke() -> Any:
                if inspect.iscoroutinefunction(step_instance.run):
                    return (
                        await step_instance.run(context, decoded_inputs)
                        if step_definition.accepts_typed_input
                        else await step_instance.run(context)
                    )
                return await asyncio.to_thread(call)

            async def invoke_with_heartbeat() -> Any:
                task = asyncio.create_task(invoke())
                interval = min(
                    1.0, max(0.05, self.lease_duration.total_seconds() / 3)
                )
                try:
                    while True:
                        done, _ = await asyncio.wait({task}, timeout=interval)
                        if done:
                            return await task
                        if current_state().status is WorkflowStatus.CANCELLED:
                            task.cancel()
                            return await task
                        self.state_store.heartbeat_claim(
                            claim.token, lease_duration=self.lease_duration
                        )
                finally:
                    if not task.done():
                        task.cancel()

            if self.step_timeout is None:
                result = await invoke_with_heartbeat()
            else:
                result = await asyncio.wait_for(
                    invoke_with_heartbeat(), timeout=self.step_timeout.total_seconds()
                )
        except asyncio.TimeoutError as exc:
            timeout = self.step_timeout
            assert timeout is not None
            error = f"Step execution timed out after {timeout.total_seconds()} seconds"
            self.state_store.fail_step(
                current_run_id, step_id, error, claim_token=claim.token
            )
            failed_run = self.state_store.get_run(current_run_id)
            raise WorkflowFailed(f"Step {step_id} failed: {error}", failed_run) from exc
        except BaseException as exc:
            if not isinstance(exc, Exception):
                self.state_store.interrupt_claim(
                    claim.token, "Previous process stopped while the step was RUNNING"
                )
                raise
            error = f"{type(exc).__name__}: {exc}"
            logger.exception(
                "Step failed",
                extra={"run_id": current_run_id, "step_id": step_id, "attempt": claim.attempt},
            )
            self.state_store.fail_step(
                current_run_id, step_id, error, claim_token=claim.token
            )
            failed_run = self.state_store.get_run(current_run_id)
            raise WorkflowFailed(f"Step {step_id} failed: {error}", failed_run) from exc
        if not isinstance(result, StepResult):
            error = (
                f"Step {step_id} returned {type(result).__name__}; "
                "Step.run() must return StepResult"
            )
            self.state_store.fail_step(
                current_run_id, step_id, error, claim_token=claim.token
            )
            raise StepExecutionError(error)
        self._persist_result(
            current_run_id,
            step_id,
            step_instance,
            context,
            result,
            claim_token=claim.token,
        )

    def _context_output(
        self,
        definition: WorkflowDefinition,
        run_id: str,
        step: type[Any] | str,
    ) -> JsonValue:
        step_id = step_identity(step)
        if step_id not in definition.step_ids:
            raise KeyError(f"Step {step_id!r} is not registered in workflow {self.name!r}")
        value = self.state_store.get_run(run_id).output(step_id)
        step_definition = next(item for item in definition.steps if item.id == step_id)
        return from_json_value(value, step_definition.output_type, path=f"output for {step_id}")

    def _context_latest_output(
        self,
        definition: WorkflowDefinition,
        run_id: str,
        step: type[Any] | str,
    ) -> tuple[bool, JsonValue]:
        step_id = step_identity(step)
        if step_id not in definition.step_ids:
            raise KeyError(f"Step {step_id!r} is not registered in workflow {self.name!r}")
        state = self.state_store.get_run(run_id).steps[step_id]
        if not state.has_output:
            return False, state.output
        step_definition = next(item for item in definition.steps if item.id == step_id)
        return True, from_json_value(
            state.output, step_definition.output_type, path=f"output for {step_id}"
        )

    def _persist_result(
        self,
        run_id: str,
        step_id: str,
        step_instance: Step,
        context: StepContext,
        result: StepResult,
        *,
        claim_token: str | None = None,
    ) -> None:
        if result.status in (StepStatus.COMPLETED, StepStatus.REPEAT):
            step_definition = next(step for step in self._definition().steps if step.id == step_id)
            try:
                if step_definition.output_type in (Any, object):
                    output = self._canonical_output(result.output, step_id)
                else:
                    typed_output, output = validate_and_encode(
                        result.output,
                        step_definition.output_type,
                        path=f"output for {step_id}",
                    )
            except StepOutputSerializationError as exc:
                self.state_store.fail_step(run_id, step_id, str(exc), claim_token=claim_token)
                raise
            except ValidationError as exc:
                error = f"Output validation for {step_id} failed: {exc}"
                self.state_store.fail_step(run_id, step_id, error, claim_token=claim_token)
                failed_run = self.state_store.get_run(run_id)
                raise WorkflowFailed(error, failed_run) from exc
            try:
                validation_value = (
                    typed_output
                    if step_definition.output_type not in (Any, object)
                    else output
                )
                verdict = step_instance.validate_output(validation_value, context)
                if verdict is None:
                    for validator in getattr(step_instance, "validators", ()):
                        verdict = validator.validate(validation_value, context)
                        if verdict is not None:
                            break
            except Exception as exc:
                error = f"Output validator for {step_id} raised {type(exc).__name__}: {exc}"
                self.state_store.fail_step(run_id, step_id, error, claim_token=claim_token)
                failed_run = self.state_store.get_run(run_id)
                raise WorkflowFailed(error, failed_run) from exc
            if verdict is None:
                if result.status is StepStatus.COMPLETED:
                    self.state_store.complete_step(
                        run_id, step_id, output, claim_token=claim_token
                    )
                    return
                cycle = next(
                    (cycle for cycle in self._definition().cycles if cycle.controller == step_id),
                    None,
                )
                if cycle is None:
                    error = f"Step {step_id} returned repeat() but is not a cycle controller"
                    self.state_store.fail_step(run_id, step_id, error, claim_token=claim_token)
                    raise StepExecutionError(error)
                repeated = self.state_store.repeat_cycle(
                    run_id,
                    cycle.name,
                    step_id,
                    output,
                    result.reason,
                    claim_token=claim_token,
                )
                if not repeated:
                    failed_run = self.state_store.get_run(run_id)
                    raise WorkflowFailed(failed_run.error or "Cycle iteration limit exceeded", failed_run)
                return
            if not isinstance(verdict, str) or not verdict.strip():
                error = (
                    f"Output validator for {step_id} must return None or a non-empty reason string"
                )
                self.state_store.fail_step(run_id, step_id, error, claim_token=claim_token)
                raise StepExecutionError(error)
            self.state_store.retry_step(
                run_id,
                step_id,
                verdict,
                candidate=output,
                rejected=True,
                claim_token=claim_token,
            )
            return

        if result.status is StepStatus.RETRY:
            self.state_store.retry_step(
                run_id,
                step_id,
                result.reason or "Step requested a retry",
                claim_token=claim_token,
            )
            return
        if result.status is StepStatus.WAITING:
            self.state_store.wait_step(
                run_id, step_id, result.reason, claim_token=claim_token,
                request=result.wait,
            )
            return
        if result.status is StepStatus.FAILED:
            self.state_store.fail_step(
                run_id,
                step_id,
                result.reason or "Step reported failure",
                claim_token=claim_token,
            )
            return

        error = f"Step {step_id} returned invalid invocation status {result.status.value}"
        self.state_store.fail_step(run_id, step_id, error, claim_token=claim_token)
        raise StepExecutionError(error)
