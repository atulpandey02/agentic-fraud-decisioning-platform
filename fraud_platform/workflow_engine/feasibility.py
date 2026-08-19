# =============================================================
# FEASIBILITY — deterministic, code-level validation of a plan
# =============================================================
# The planner is judgment (a model); feasibility is the invariant
# (code) — the same split as governance/policy_framework.py. NO LLM
# runs here. A plan the planner produced is only allowed to execute
# if EVERY check below passes, so the model's preferences ("don't
# invent a tool") become guarantees the executor can rely on.
#
# Checks:
#   1. every step's tool exists in the registry (unknown tool =
#      failure, never a runtime guess)
#   2. every step's args validate against that tool's schema
#      (with '$ref' placeholders allowed to stand in)
#   3. '$step_N' references point BACKWARD only (no forward/cyclic)
#   4. step count <= MAX_PLAN_STEPS
#   5. tool category is in the allow-list (no write/destructive
#      tools can slip in — adding one later fails HERE until the
#      policy is consciously widened: fail safe, not fail open)
#   6. requires_approval is surfaced so the state machine routes the
#      workflow through AWAITING_APPROVAL
# =============================================================

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pydantic import ValidationError

from . import config
from .conditions import parse_condition
from .planner import WorkflowPlan
from .registry import ToolRegistry

_REF_RE = re.compile(r"^\$(step_(\d+)|trigger)(\.[A-Za-z_][A-Za-z0-9_]*)?$")


@dataclass
class FeasibilityReport:
    ok: bool
    errors: list[str] = field(default_factory=list)
    requires_approval: bool = False


def _placeholder(annotation) -> object:
    """A type-appropriate stand-in for a '$ref' arg, so the concrete
    fields of a step can still be schema-validated."""
    return {str: "x", int: 0, float: 0.0, bool: False}.get(annotation, "x")


def _check_ref(value: str, step_number: int, valid_steps: set[int], errors: list[str]) -> None:
    m = _REF_RE.match(value)
    if not m:
        errors.append(f"step {step_number}: malformed reference {value!r} "
                      f"(expected $step_N.field or $trigger.field)")
        return
    if m.group(1).startswith("step_"):
        ref_n = int(m.group(2))
        if ref_n >= step_number:
            errors.append(f"step {step_number}: reference {value!r} does not point "
                          f"backward (only earlier steps may be referenced)")
        elif ref_n not in valid_steps:
            errors.append(f"step {step_number}: reference {value!r} points at a "
                          f"step that is not in the plan")


def check_plan(plan: WorkflowPlan, registry: ToolRegistry) -> FeasibilityReport:
    errors: list[str] = []
    requires_approval = False

    # A planner that asked for clarification is not infeasible — it is
    # simply not executable; surface that as a (non-ok) report with no
    # spurious errors so the API can show the clarification question.
    if plan.needs_clarification:
        return FeasibilityReport(
            ok=False,
            errors=[f"needs clarification: {plan.clarification_question or 'unspecified'}"],
        )

    if not plan.steps:
        return FeasibilityReport(ok=False, errors=["plan has no steps"])

    if len(plan.steps) > config.MAX_PLAN_STEPS:
        errors.append(f"plan has {len(plan.steps)} steps; max is {config.MAX_PLAN_STEPS}")

    step_numbers = {s.step_number for s in plan.steps}

    for step in plan.steps:
        spec = registry.get(step.tool_name)
        if spec is None:
            errors.append(f"step {step.step_number}: unknown tool "
                          f"{step.tool_name!r} — not in registry")
            continue

        if spec.category not in config.ALLOWED_TOOL_CATEGORIES:
            errors.append(f"step {step.step_number}: tool {spec.name!r} category "
                          f"{spec.category!r} is not permitted")

        if spec.requires_approval:
            requires_approval = True

        # Validate args: refs are checked for backward-ness and replaced with a
        # type placeholder; concrete values are type/required-checked by pydantic.
        fields = spec.args_schema.model_fields
        candidate: dict = {}
        for key, val in step.args.items():
            if key not in fields:
                errors.append(f"step {step.step_number}: unknown arg {key!r} "
                              f"for {spec.name}")
                continue
            if isinstance(val, str) and val.startswith("$"):
                _check_ref(val, step.step_number, step_numbers, errors)
                candidate[key] = _placeholder(fields[key].annotation)
            else:
                candidate[key] = val
        try:
            spec.args_schema(**candidate)
        except ValidationError as e:
            first = e.errors()[0]
            errors.append(f"step {step.step_number}: args invalid for {spec.name} "
                          f"({'.'.join(str(x) for x in first['loc'])}: {first['msg']})")

        # Validate the optional `when` guard: it must parse into a single
        # '$ref OP literal' comparison, and its reference must point BACKWARD
        # (same rule as arg refs) — a condition on a future step is undecidable.
        if step.when:
            try:
                cond = parse_condition(step.when)
            except ValueError as e:
                errors.append(f"step {step.step_number}: {e}")
            else:
                _check_ref(cond.ref, step.step_number, step_numbers, errors)

    return FeasibilityReport(ok=not errors, errors=errors, requires_approval=requires_approval)
