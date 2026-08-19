# =============================================================
# RUN DEMO — end-to-end workflow engine, from CLI
# =============================================================
# The interview moment. Two runs:
#   1. A valid automation: "after every payment capture, if the user
#      has 2+ BLOCK decisions in 24h, send a Slack message with their
#      recent history." -> plan (3 steps) -> feasibility PASS ->
#      simulated payment.captured event -> step results + outbox row.
#   2. The refusal: "delete all BLOCK decisions from last week" ->
#      the planner finds no destructive tool -> needs_clarification /
#      REJECTED. That refusal IS the guardrail demo.
#
# Run modes:
#   python -m fraud_platform.workflow_engine.run_demo          (real stack)
#   python -m fraud_platform.workflow_engine.run_demo --mock   (offline, canned
#       plan + fake tools + in-memory store — always presentable, no infra)
# =============================================================

from __future__ import annotations

import argparse
import json
import logging

from pydantic import BaseModel, Field

from . import config
from .executor import Executor
from .feasibility import check_plan
from .planner import PlanStep, Planner, WorkflowPlan
from .registry import ANALYZE, NOTIFY, READ_DATA, ToolRegistry, ToolSpec
from .state import WorkflowState, WorkflowStore
from .tools_bridge import build_default_registry
from .connectors import make_slack_send

logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
logger = logging.getLogger("workflow.demo")

VALID_INSTRUCTION = (
    "After every payment capture, if the user has 2 or more BLOCK decisions in "
    "the last 24 hours, send a Slack message with their recent history."
)
REFUSAL_INSTRUCTION = "Delete all BLOCK decisions from last week."


# ---------------------------------------------------------------- mock stack
class _CountArgs(BaseModel):
    user_id: str = Field(description="x")
    decision: str = Field(description="x")
    hours: int = Field(default=24, description="x")


class _UserArgs(BaseModel):
    user_id: str = Field(description="x")


class _SlackArgs(BaseModel):
    channel: str = Field(description="x")
    text: str = Field(description="x")


def _mock_registry(store: WorkflowStore) -> ToolRegistry:
    r = ToolRegistry()
    # Count is user-dependent so the demo can show BOTH branches: a user_id
    # containing "safe" has 1 BLOCK (guard false -> skip), anyone else has 3.
    r.register(ToolSpec("count_recent_decisions", "deterministically count decisions",
                        _CountArgs, ANALYZE,
                        execute=lambda user_id, decision, hours=24:
                            {"count": 1 if "safe" in user_id.lower() else 3, "user_id": user_id}))
    r.register(ToolSpec("get_user_history", "user baseline history", _UserArgs, READ_DATA,
                        execute=lambda user_id: {"summary": f"user {user_id}: 3 BLOCKs, home NYC, HIGH tier"}))
    r.register(ToolSpec("slack_send_message", "send slack", _SlackArgs, NOTIFY,
                        execute=make_slack_send(store.outbox_writer()), read_only=False))
    return r


class _CannedLLM:
    """Stand-in planner LLM for --mock: returns a fixed plan for the valid
    request and a clarification for the destructive one, keyed off the text."""

    def invoke(self, messages):
        text = messages[-1].content.lower()
        if "delete" in text:
            return WorkflowPlan(goal="delete decisions", steps=[], needs_clarification=True,
                                clarification_question="No tool can delete or modify decisions — "
                                "all registered tools are read-only or notify-only.")
        return WorkflowPlan(
            goal="notify fraud-ops when a user has repeat BLOCKs",
            trigger="payment.captured",
            steps=[
                PlanStep(step_number=1, tool_name="count_recent_decisions",
                         args={"user_id": "$trigger.user_id", "decision": "BLOCK", "hours": 24},
                         rationale="deterministic countable condition"),
                PlanStep(step_number=2, tool_name="get_user_history",
                         args={"user_id": "$trigger.user_id"},
                         when="$step_1.count >= 2",
                         rationale="gather context only if the condition holds"),
                PlanStep(step_number=3, tool_name="slack_send_message",
                         args={"channel": "#fraud-ops", "text": "$step_2.summary"},
                         when="$step_1.count >= 2",
                         rationale="notify only if the condition holds"),
            ],
        )


# ---------------------------------------------------------------- demo flow
def _print_plan(plan: WorkflowPlan) -> None:
    print(f"\n  goal: {plan.goal}")
    print(f"  trigger: {plan.trigger}")
    if plan.needs_clarification:
        print(f"  needs_clarification: {plan.clarification_question}")
    for s in plan.steps:
        print(f"    {s.step_number}. {s.tool_name}({json.dumps(s.args)})  — {s.rationale}")


def run(mock: bool = True, event_user_id: str = "demo-user") -> None:
    store = WorkflowStore(db_path=":memory:" if mock else config.WORKFLOW_DB_PATH)
    if mock:
        registry = _mock_registry(store)
        planner = Planner(registry, llm=_CannedLLM())
    else:
        registry = build_default_registry(store.outbox_writer())
        planner = Planner(registry)
    executor = Executor(registry, store)

    print("=" * 70)
    print("WORKFLOW ENGINE DEMO  (plan-and-execute)" + ("  [MOCK]" if mock else ""))
    print("=" * 70)

    # ---- 1. valid automation ----
    print(f"\n[1] Instruction: {VALID_INSTRUCTION}")
    wid = store.create_workflow(event_user_id, VALID_INSTRUCTION)
    store.transition(wid, WorkflowState.PLANNED)
    plan = planner.plan(VALID_INSTRUCTION)
    store.set_plan(wid, plan.model_dump_json())
    _print_plan(plan)

    report = check_plan(plan, registry)
    print(f"\n  FEASIBILITY: {'PASS' if report.ok else 'FAIL'}"
          f"{'  (requires approval)' if report.requires_approval else ''}")
    for e in report.errors:
        print(f"    - {e}")

    if report.ok:
        store.transition(wid, WorkflowState.FEASIBLE)
        if report.requires_approval:
            store.transition(wid, WorkflowState.AWAITING_APPROVAL)
            store.transition(wid, WorkflowState.READY)   # demo auto-approves
        else:
            store.transition(wid, WorkflowState.READY)

        def _show(res):
            print(f"\n  RUN {res.status}")
            for o in res.steps:
                if o.status == "skipped":
                    body = f"SKIPPED — {o.reason}"
                elif o.status == "error":
                    body = o.error
                else:
                    body = json.dumps(o.result, default=str)[:80]
                print(f"    step {o.step_number} {o.tool_name}: {o.status} ({o.latency_ms}ms) {body}")

        # positive branch: a user AT/ABOVE the threshold -> message sent
        print(f"\n[2] Firing simulated event (count >= 2): payment.captured {{user_id: {event_user_id!r}}}")
        _show(executor.execute(wid, plan, trigger_payload={"user_id": event_user_id}))
        print("\n  OUTBOX (the exact payload that WOULD be delivered):")
        for row in store.outbox():
            print(f"    [{row['connector']}] {row['payload_json']}")

        # negative branch: SAME workflow, a BELOW-threshold user -> steps SKIPPED
        print("\n[2b] Firing the SAME workflow for a below-threshold user (count < 2):")
        store.transition(wid, WorkflowState.READY)   # re-arm COMPLETED -> READY
        before = len(store.outbox())
        _show(executor.execute(wid, plan, trigger_payload={"user_id": "safe-" + event_user_id}))
        print(f"  OUTBOX unchanged: {len(store.outbox())} row(s) (was {before}) — the guard held, nothing sent")

    # ---- 2. the refusal (guardrail demo) ----
    print(f"\n[3] Instruction: {REFUSAL_INSTRUCTION}")
    wid2 = store.create_workflow(event_user_id, REFUSAL_INSTRUCTION)
    store.transition(wid2, WorkflowState.PLANNED)
    plan2 = planner.plan(REFUSAL_INSTRUCTION)
    _print_plan(plan2)
    report2 = check_plan(plan2, registry)
    store.transition(wid2, WorkflowState.REJECTED if not report2.ok else WorkflowState.FEASIBLE)
    print(f"\n  FEASIBILITY: {'PASS' if report2.ok else 'REJECTED'} — the guardrail held")
    for e in report2.errors:
        print(f"    - {e}")

    print("\n" + "=" * 70)
    store.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Workflow engine end-to-end demo")
    ap.add_argument("--mock", action="store_true",
                    help="offline: canned plan + fake tools + in-memory store")
    ap.add_argument("--user", default="demo-user", help="user_id for the simulated event")
    args = ap.parse_args()
    run(mock=args.mock, event_user_id=args.user)


if __name__ == "__main__":
    main()
