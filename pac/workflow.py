from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from copy import deepcopy
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from .context import StepContext
from .errors import (
    StepExecutionError,
    StepOutputSerializationError,
    WorkflowCycleError,
    WorkflowDeadlockError,
    WorkflowDefinitionChanged,
    WorkflowDefinitionError,
    WorkflowFailed,
)
from .models import (
    CycleDefinition,
    CycleStatus,
    JsonValue,
    StepDefinition,
    StepStatus,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
    step_identity,
)
from .results import StepResult
from .runtime.codex import CodexRuntime
from .scheduler import next_runnable_step
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
        codex_runtime_factory: RuntimeFactory = CodexRuntime,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise WorkflowDefinitionError("Workflow name must be a non-empty string")
        if state_store is not None and state_path is not None:
            raise WorkflowDefinitionError("Pass either state_store or state_path, not both")
        self.name = name
        self.cwd = Path(cwd).resolve()
        self.model = model
        self.sandbox = sandbox
        self.state_store = state_store or SQLiteStateStore(
            state_path or self.cwd / ".pac" / "state.db"
        )
        self._runtime_factory = codex_runtime_factory
        self._registrations: list[
            tuple[type[Step], tuple[type[Step], ...], dict[str, JsonValue]]
        ] = []
        self._cycles: list[
            tuple[str, tuple[type[Step], ...], tuple[type[Step], type[Step]], int]
        ] = []

    def add_step(
        self,
        step: type[Step],
        *,
        depends_on: list[type[Step]] | tuple[type[Step], ...] | None = None,
        inputs: Mapping[str, Any] | None = None,
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
        inputs: Mapping[str, Any] | None,
        step: type[Step],
    ) -> dict[str, JsonValue]:
        if inputs is None:
            return {}
        if not isinstance(inputs, Mapping):
            raise WorkflowDefinitionError(
                f"Inputs for {step_identity(step)} must be a mapping with string keys"
            )
        if any(not isinstance(key, str) for key in inputs):
            raise WorkflowDefinitionError(
                f"Inputs for {step_identity(step)} must use string keys"
            )
        Workflow._validate_input_keys(inputs, step_identity(step))
        try:
            encoded = json.dumps(
                dict(inputs), sort_keys=True, separators=(",", ":"), allow_nan=False
            )
            canonical = json.loads(encoded)
        except (TypeError, ValueError) as exc:
            raise WorkflowDefinitionError(
                f"Inputs for {step_identity(step)} must be JSON-serializable: {exc}"
            ) from exc
        if not isinstance(canonical, dict):
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
            for dependency in dependencies:
                if not isinstance(dependency, type) or not issubclass(dependency, Step):
                    raise WorkflowDefinitionError(
                        f"Dependency of {step_identity(step_class)} is not a Step class"
                    )
                dependency_id = step_identity(dependency)
                if dependency_id not in order:
                    raise WorkflowDefinitionError(
                        f"Step {step_identity(step_class)} depends on unregistered step "
                        f"{dependency_id}"
                    )
                if dependency_id not in dependency_ids:
                    dependency_ids.append(dependency_id)
            dependency_ids.sort(key=order.__getitem__)
            definitions.append(
                StepDefinition(
                    id=step_identity(step_class),
                    step_class=step_class,
                    registration_order=index,
                    dependencies=tuple(dependency_ids),
                    max_attempts=max_attempts,
                    inputs=deepcopy(inputs),
                )
            )

        self._validate_acyclic(tuple(definitions))
        cycle_definitions = self._cycle_definitions(tuple(definitions))
        sandbox_value = getattr(self.sandbox, "value", self.sandbox)
        canonical = {
            "workflow_name": self.name,
            "cwd": str(self.cwd),
            "model": self.model,
            "sandbox": sandbox_value,
            "steps": [
                {
                    "id": step.id,
                    "registration_order": step.registration_order,
                    "dependencies": list(step.dependencies),
                    "max_attempts": step.max_attempts,
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
            outgoing = {member: [] for member in members}
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

    def loop(self) -> WorkflowRun:
        definition = self._definition()
        definitions_by_id = {step.id: step for step in definition.steps}

        with self.state_store.execution_lock(self.name):
            run = self.state_store.active_run(self.name)
            if run is None:
                run = self.state_store.create_run(definition)
            elif run.definition_fingerprint != definition.fingerprint:
                raise WorkflowDefinitionChanged(
                    f"Workflow {self.name!r} definition changed for unfinished run {run.id}: "
                    f"stored={run.definition_fingerprint}, current={definition.fingerprint}"
                )

            if run.status is WorkflowStatus.WAITING:
                self.state_store.resume_waiting(run.id)
            else:
                self.state_store.recover_running(run.id)

            run = self.state_store.get_run(run.id)
            if run.status is WorkflowStatus.FAILED:
                raise WorkflowFailed(run.error or f"Workflow {self.name!r} failed", run)
            if run.status is WorkflowStatus.PENDING:
                self.state_store.start_workflow(run.id)

            with self._runtime_factory(
                store=self.state_store,
                run_id=run.id,
                cwd=self.cwd,
                model=self.model,
                sandbox=self.sandbox,
            ) as runtime:
                while True:
                    run = self.state_store.get_run(run.id)
                    failed = next(
                        (state for state in run.steps.values() if state.status is StepStatus.FAILED),
                        None,
                    )
                    if failed is not None or run.status is WorkflowStatus.FAILED:
                        if run.status is not WorkflowStatus.FAILED:
                            if failed is None:
                                raise AssertionError("A non-failed workflow must have a failed step")
                            self.state_store.fail_workflow(
                                run.id, f"Step {failed.id} failed: {failed.error}"
                            )
                            run = self.state_store.get_run(run.id)
                        raise WorkflowFailed(run.error or f"Workflow {self.name!r} failed", run)

                    if all(
                        state.status is StepStatus.COMPLETED for state in run.steps.values()
                    ) and all(cycle.status is CycleStatus.COMPLETED for cycle in run.cycles.values()):
                        self.state_store.complete_workflow(run.id)
                        return self.state_store.get_run(run.id)

                    step_id = next_runnable_step(definition, run.steps, run.cycles)
                    if step_id is not None:
                        state = self.state_store.start_step(run.id, step_id)
                        step_definition = definitions_by_id[step_id]
                        current_run_id = run.id

                        def output_for_step(
                            step: type[Any] | str,
                            run_id: str = current_run_id,
                        ) -> JsonValue:
                            return self._context_output(definition, run_id, step)

                        def current_state(run_id: str = current_run_id) -> WorkflowRun:
                            return self.state_store.get_run(run_id)

                        def latest_output_for_step(
                            step: type[Any] | str,
                            run_id: str = current_run_id,
                        ) -> tuple[bool, JsonValue]:
                            return self._context_latest_output(definition, run_id, step)

                        context = StepContext(
                            workflow_id=self.name,
                            run_id=run.id,
                            step_id=step_id,
                            attempt=state.attempt,
                            iteration=state.iteration,
                            codex=runtime.bind_step(step_id),
                            retry_reason=state.retry_reason,
                            inputs=MappingProxyType(deepcopy(step_definition.inputs)),
                            _output=output_for_step,
                            _latest_output=latest_output_for_step,
                            _state=current_state,
                        )
                        logger.info(
                            "Step started",
                            extra={
                                "workflow_name": self.name,
                                "run_id": run.id,
                                "step_id": step_id,
                                "attempt": state.attempt,
                                "state_transition": "RUNNING",
                            },
                        )
                        try:
                            step_instance = step_definition.step_class()
                            result = step_instance.run(context)
                        except Exception as exc:
                            error = f"{type(exc).__name__}: {exc}"
                            logger.exception(
                                "Step failed",
                                extra={"run_id": run.id, "step_id": step_id, "attempt": state.attempt},
                            )
                            self.state_store.fail_step(run.id, step_id, error)
                            failed_run = self.state_store.get_run(run.id)
                            raise WorkflowFailed(f"Step {step_id} failed: {error}", failed_run) from exc

                        if not isinstance(result, StepResult):
                            error = (
                                f"Step {step_id} returned {type(result).__name__}; "
                                "Step.run() must return StepResult"
                            )
                            self.state_store.fail_step(run.id, step_id, error)
                            raise StepExecutionError(error)

                        self._persist_result(run.id, step_id, step_instance, context, result)
                        continue

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

    def _context_output(
        self,
        definition: WorkflowDefinition,
        run_id: str,
        step: type[Any] | str,
    ) -> JsonValue:
        step_id = step_identity(step)
        if step_id not in definition.step_ids:
            raise KeyError(f"Step {step_id!r} is not registered in workflow {self.name!r}")
        return self.state_store.get_run(run_id).output(step_id)

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
        return state.has_output, state.output

    def _persist_result(
        self,
        run_id: str,
        step_id: str,
        step_instance: Step,
        context: StepContext,
        result: StepResult,
    ) -> None:
        if result.status in (StepStatus.COMPLETED, StepStatus.REPEAT):
            try:
                output = self._canonical_output(result.output, step_id)
            except StepOutputSerializationError as exc:
                self.state_store.fail_step(run_id, step_id, str(exc))
                raise
            try:
                verdict = step_instance.validate_output(output, context)
            except Exception as exc:
                error = f"Output validator for {step_id} raised {type(exc).__name__}: {exc}"
                self.state_store.fail_step(run_id, step_id, error)
                failed_run = self.state_store.get_run(run_id)
                raise WorkflowFailed(error, failed_run) from exc
            if verdict is None:
                if result.status is StepStatus.COMPLETED:
                    self.state_store.complete_step(run_id, step_id, output)
                    return
                cycle = next(
                    (cycle for cycle in self._definition().cycles if cycle.controller == step_id),
                    None,
                )
                if cycle is None:
                    error = f"Step {step_id} returned repeat() but is not a cycle controller"
                    self.state_store.fail_step(run_id, step_id, error)
                    raise StepExecutionError(error)
                repeated = self.state_store.repeat_cycle(
                    run_id, cycle.name, step_id, output, result.reason
                )
                if not repeated:
                    failed_run = self.state_store.get_run(run_id)
                    raise WorkflowFailed(failed_run.error or "Cycle iteration limit exceeded", failed_run)
                return
            if not isinstance(verdict, str) or not verdict.strip():
                error = (
                    f"Output validator for {step_id} must return None or a non-empty reason string"
                )
                self.state_store.fail_step(run_id, step_id, error)
                raise StepExecutionError(error)
            self.state_store.retry_step(
                run_id, step_id, verdict, candidate=output, rejected=True
            )
            return

        if result.status is StepStatus.RETRY:
            self.state_store.retry_step(
                run_id, step_id, result.reason or "Step requested a retry"
            )
            return
        if result.status is StepStatus.WAITING:
            self.state_store.wait_step(run_id, step_id, result.reason)
            return
        if result.status is StepStatus.FAILED:
            self.state_store.fail_step(
                run_id, step_id, result.reason or "Step reported failure"
            )
            return

        error = f"Step {step_id} returned invalid invocation status {result.status.value}"
        self.state_store.fail_step(run_id, step_id, error)
        raise StepExecutionError(error)
