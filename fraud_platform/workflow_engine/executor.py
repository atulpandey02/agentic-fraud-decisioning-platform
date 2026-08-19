# =============================================================
# EXECUTOR — run a validated plan, step by step, with checkpoints
# =============================================================
# The plan is fixed (plan-and-execute); the executor is deliberately
# dumb. For each step it: resolves '$ref' args from prior results /
# the trigger payload, dispatches through the registry's ONE guarded
# execute path, and CHECKPOINTS the outcome before the next step
# begins.
#
# It stops on the FIRST failure and records the error — no silent
# fail-open. That is a direct lesson from this platform's own audit:
# the feature pipeline's Redis reader once failed OPEN (an error read
# as "no data", which downstream treated as "nothing suspicious").
# A workflow that half-runs and reports success is the same bug in a
# new place, so a failed step ends the run in FAILED with the error
# on the record, not a shrug.
# =============================================================

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .conditions import compare, parse_condition
from .feasibility import _REF_RE
from .planner import WorkflowPlan, PlanStep
from .registry import ToolRegistry
from .state import WorkflowState, WorkflowStore

logger = logging.getLogger(__name__)


@dataclass
class StepOutcome:
    step_number: int
    tool_name: str
    status: str            # "ok" | "error" | "skipped"
    result: object = None
    error: Optional[str] = None
    reason: Optional[str] = None   # why a step was skipped
    latency_ms: int = 0


@dataclass
class RunResult:
    run_id: str
    status: str            # "COMPLETED" | "FAILED"
    steps: list[StepOutcome] = field(default_factory=list)
    error: Optional[str] = None


class Executor:
    def __init__(self, registry: ToolRegistry, store: WorkflowStore) -> None:
        self._registry = registry
        self._store = store

    # ---------------------------------------------------------- ref resolution
    @staticmethod
    def _resolve_value(value, trigger: dict, results: dict):
        """Resolve one arg value. A '$trigger.f' pulls from the event payload;
        a '$step_N.f' pulls field f from step N's result (or the whole result
        if no field). Non-ref values pass through unchanged."""
        if not (isinstance(value, str) and value.startswith("$")):
            return value
        m = _REF_RE.match(value)
        if not m:
            raise ValueError(f"unresolvable reference {value!r}")
        field_name = m.group(3)[1:] if m.group(3) else None
        if m.group(1) == "trigger":
            src = trigger
        else:
            src = results.get(int(m.group(2)))
            if src is None:
                raise ValueError(f"reference {value!r} to a step that has not run")
        if field_name is None:
            return src
        if isinstance(src, dict):
            if field_name not in src:
                raise ValueError(f"reference {value!r}: field {field_name!r} not in result")
            return src[field_name]
        raise ValueError(f"reference {value!r}: step result is not addressable by field")

    def _resolve_args(self, args: dict, trigger: dict, results: dict) -> dict:
        return {k: self._resolve_value(v, trigger, results) for k, v in args.items()}

    @staticmethod
    def _referenced_steps(step: PlanStep) -> set[int]:
        """The step numbers this step depends on — every $step_N referenced in
        its args OR its `when` guard. Used for cascade-skip: a step whose input
        or guard points at a SKIPPED step cannot itself run."""
        refs: set[int] = set()

        def _add(token: str) -> None:
            m = _REF_RE.match(token)
            if m and m.group(1).startswith("step_"):
                refs.add(int(m.group(2)))

        for v in step.args.values():
            if isinstance(v, str) and v.startswith("$"):
                _add(v)
        if step.when:
            try:
                _add(parse_condition(step.when).ref)
            except ValueError:
                pass  # a malformed `when` is a feasibility failure, not our concern here
        return refs

    # ---------------------------------------------------------- execution
    def execute(self, workflow_id: str, plan: WorkflowPlan,
                trigger_payload: Optional[dict] = None,
                scheduled_run_key: Optional[str] = None) -> RunResult:
        """Run a workflow's plan as a fresh run. The workflow must be in a
        state from which RUNNING is legal (READY). Transitions it to RUNNING,
        then to COMPLETED or FAILED.

        `scheduled_run_key` stamps the run with its (workflow, fire-time)
        identity. It is reserved FIRST — before any state change — so a
        duplicate scheduled fire is rejected by the unique index (raising
        sqlite3.IntegrityError) without half-running or double-sending."""
        trigger = trigger_payload or {}
        run_id = self._store.start_run(workflow_id, scheduled_run_key)
        self._store.transition(workflow_id, WorkflowState.RUNNING)
        results: dict[int, object] = {}
        skipped: set[int] = set()
        outcomes: list[StepOutcome] = []

        def _skip(s: PlanStep, reason: str) -> None:
            self._store.checkpoint_step(run_id, s.step_number, s.tool_name,
                                        s.args, {"skipped": reason}, "skipped")
            outcomes.append(StepOutcome(s.step_number, s.tool_name, "skipped", reason=reason))
            skipped.add(s.step_number)
            logger.info("workflow %s step %d (%s) SKIPPED: %s",
                        workflow_id, s.step_number, s.tool_name, reason)

        for step in sorted(plan.steps, key=lambda s: s.step_number):
            started = time.time()

            # (a) cascade skip — an input or guard points at a SKIPPED step, so
            # this step's data does not exist by design. Skip, don't fail.
            blocked = self._referenced_steps(step) & skipped
            if blocked:
                _skip(step, f"depends on skipped step(s) {sorted(blocked)}")
                continue

            # (b) guard — evaluate `when` in CODE (never the LLM). False -> SKIP.
            # This is the fix for straight-line plans running every step: the
            # 'if' becomes a deterministic gate, not the mere presence of a step.
            if step.when:
                try:
                    cond = parse_condition(step.when)
                    passed = compare(self._resolve_value(cond.ref, trigger, results),
                                     cond.op, cond.literal)
                except Exception as e:  # noqa: BLE001 — a broken guard IS a real error
                    latency = int((time.time() - started) * 1000)
                    err = f"guard {step.when!r}: {type(e).__name__}: {e}"
                    self._store.checkpoint_step(run_id, step.step_number, step.tool_name,
                                                step.args, {"error": err}, "error")
                    outcomes.append(StepOutcome(step.step_number, step.tool_name,
                                                "error", error=err, latency_ms=latency))
                    self._store.finish_run(run_id, "FAILED")
                    self._store.transition(workflow_id, WorkflowState.FAILED)
                    return RunResult(run_id, "FAILED", outcomes, error=err)
                if not passed:
                    _skip(step, f"guard false: {step.when}")
                    continue

            # (c) execute
            try:
                resolved = self._resolve_args(step.args, trigger, results)
                result = self._registry.execute(step.tool_name, resolved)
            except Exception as e:  # noqa: BLE001 — stop, record, do NOT fail open
                latency = int((time.time() - started) * 1000)
                err = f"{type(e).__name__}: {e}"
                self._store.checkpoint_step(run_id, step.step_number, step.tool_name,
                                            step.args, {"error": err}, "error")
                outcomes.append(StepOutcome(step.step_number, step.tool_name,
                                            "error", error=err, latency_ms=latency))
                self._store.finish_run(run_id, "FAILED")
                self._store.transition(workflow_id, WorkflowState.FAILED)
                logger.error("workflow %s step %d (%s) FAILED: %s — run aborted",
                             workflow_id, step.step_number, step.tool_name, err)
                return RunResult(run_id, "FAILED", outcomes, error=err)

            latency = int((time.time() - started) * 1000)
            results[step.step_number] = result
            self._store.checkpoint_step(run_id, step.step_number, step.tool_name,
                                        resolved, result, "ok")
            outcomes.append(StepOutcome(step.step_number, step.tool_name, "ok",
                                        result=result, latency_ms=latency))
            logger.info("workflow %s step %d (%s) ok in %dms",
                        workflow_id, step.step_number, step.tool_name, latency)

        self._store.finish_run(run_id, "COMPLETED")
        self._store.transition(workflow_id, WorkflowState.COMPLETED)
        return RunResult(run_id, "COMPLETED", outcomes)
