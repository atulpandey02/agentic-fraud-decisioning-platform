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

SCHEDULED_INSTRUCTION = (
    "Every day at 10 PM, send #fraud-ops a report showing total reviewed "
    "transactions, BLOCK count, ESCALATE count, fraud rate, and top fraud pattern."
)
SCHEDULED_REFUSAL = (
    "Every day at 10 PM delete yesterday's BLOCK decisions and email me the result."
)


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


# ---------------------------------------------------------------- scheduled mock stack
class _FakeReportConn:
    """Stands in for the BI Snowflake connection so the scheduled demo runs
    offline while still exercising the REAL catalog pipeline (validate ->
    substitute window -> re-validate). It records the SQL it was asked to run
    and returns a canned single-row fraud report."""
    _COLS = ["transactions_reviewed", "block_count", "escalate_count",
             "allow_count", "fraud_rate", "top_fraud_pattern"]
    _ROW = [4820, 102, 25, 4693, 0.0263, "VELOCITY_SPIKE"]

    def cursor(self):
        return self

    def execute(self, sql, *a, **k):
        self.description = [(c,) for c in self._COLS]
        self._last_sql = sql

    def fetchall(self):
        return [self._ROW]

    def close(self):
        pass


def _mock_report_registry(store: WorkflowStore) -> ToolRegistry:
    """run_report_query (real catalog pipeline + fake conn) -> format_report
    (real) -> slack (real, outbox). No Snowflake, no Groq."""
    from .report import format_report
    from .reports_catalog import REPORT_CATALOG, run_validated_report
    from .tools_bridge import FormatReportArgs, ReportRunArgs

    def _run(report, window_start=None, window_end=None):
        from fraud_platform.bi_dashboard import config as bi_config
        from fraud_platform.bi_dashboard.sql_guard import SQLValidator
        sql = REPORT_CATALOG.get(report, (None, report))[1]
        validator = SQLValidator(bi_config.BI_ALLOWED_TABLES, bi_config.BI_MAX_ROWS)
        return run_validated_report(sql, window_start, window_end,
                                    validator=validator, connect=lambda: _FakeReportConn())

    r = ToolRegistry()
    r.register(ToolSpec("run_report_query", "run a named parameterized report",
                        ReportRunArgs, READ_DATA, execute=_run))
    r.register(ToolSpec("format_report", "format a report", FormatReportArgs, ANALYZE,
                        execute=format_report))
    r.register(ToolSpec("slack_send_message", "send slack", _SlackArgs, NOTIFY,
                        execute=make_slack_send(store.outbox_writer()), read_only=False))
    return r


class _CannedReportLLM:
    """Canned planner for the scheduled demo: a valid scheduled-report plan
    (with a schedule + the run_report_query/format_report/slack chain), or a
    clarification for the destructive request."""

    def invoke(self, messages):
        from .planner import PlannedSchedule
        text = messages[-1].content.lower()
        if "delete" in text:
            return WorkflowPlan(goal="delete BLOCK decisions", steps=[], needs_clarification=True,
                                clarification_question="No tool can delete or modify decisions — "
                                "every registered tool is read-only or notify-only.")
        return WorkflowPlan(
            goal="daily fraud performance report to #fraud-ops", trigger=None,
            schedule=PlannedSchedule(frequency="daily", hour=22, minute=0,
                                     timezone="America/New_York"),
            steps=[
                PlanStep(step_number=1, tool_name="run_report_query",
                         args={"report": "fraud_performance",
                               "window_start": "$trigger.window_start",
                               "window_end": "$trigger.window_end"},
                         rationale="run the validated fraud report for the day's window"),
                PlanStep(step_number=2, tool_name="format_report",
                         args={"title": "Daily Fraud Operations Report", "data": "$step_1"},
                         rationale="turn rows into a readable report (never raw tuples)"),
                PlanStep(step_number=3, tool_name="slack_send_message",
                         args={"channel": "#fraud-ops", "text": "$step_2.slack_text"},
                         rationale="deliver the compact report to Slack"),
            ],
        )


def run_scheduled(mock: bool = True) -> None:
    """The scheduled-report demo: NL -> plan -> validated SQL -> schedule
    interpretation -> feasibility -> SIMULATED scheduler trigger -> execution
    -> formatted report -> outbox -> persisted state; then the destructive
    scheduled request, REJECTED."""
    from datetime import datetime, timezone

    from .reports_catalog import REPORT_CATALOG
    from .schedule import Schedule
    from .scheduler import SchedulerRuntime

    store = WorkflowStore(db_path=":memory:" if mock else config.WORKFLOW_DB_PATH)
    registry = _mock_report_registry(store) if mock else build_default_registry(
        store.outbox_writer(), template_resolver=lambda t: (store.get_template(t) or {}).get("sql"))
    planner = Planner(registry, llm=_CannedReportLLM() if mock else None)
    executor = Executor(registry, store)
    runtime = SchedulerRuntime(store, executor)   # not started: we fire it by hand

    print("=" * 70)
    print("SCHEDULED REPORT DEMO  (plan → schedule → simulated trigger)" + ("  [MOCK]" if mock else ""))
    print("=" * 70)

    # ---- 1. NL request -> plan ----
    print(f"\n[1] Instruction: {SCHEDULED_INSTRUCTION}")
    wid = store.create_workflow("analyst", SCHEDULED_INSTRUCTION)
    store.transition(wid, WorkflowState.PLANNED)
    plan = planner.plan(SCHEDULED_INSTRUCTION)
    store.set_plan(wid, plan.model_dump_json())
    _print_plan(plan)

    # ---- 2. schedule interpretation (validated in code) + the validated SQL ----
    sched = Schedule(**plan.schedule.model_dump())
    print(f"\n[2] Schedule interpreted: {sched.describe()}")
    print("    Validated, parameterized report SQL (NOT regenerated per run):")
    for line in REPORT_CATALOG["fraud_performance"][1].splitlines():
        print(f"      {line}")

    # ---- 3. feasibility ----
    report = check_plan(plan, registry)
    print(f"\n[3] FEASIBILITY: {'PASS' if report.ok else 'FAIL'}")
    for e in report.errors:
        print(f"    - {e}")
    if not report.ok:
        store.transition(wid, WorkflowState.REJECTED)
        store.close()
        return

    store.transition(wid, WorkflowState.FEASIBLE)
    store.set_schedule(wid, sched.model_dump_json(),
                       destination_json='{"connector":"slack","channel":"#fraud-ops"}')
    store.transition(wid, WorkflowState.READY)
    next_run = runtime.schedule_workflow(wid, sched)
    print(f"    Registered. Next run: {next_run}")

    # ---- 4. SIMULATED scheduler trigger (fixed time, no wall clock) ----
    fire = datetime(2026, 8, 20, 22, 0, tzinfo=timezone.utc)
    print(f"\n[4] [SIMULATED SCHEDULER TRIGGER] fire_dt={fire.isoformat()}")
    result = runtime._fire(wid, "daily", fire_dt=fire)
    print(f"    RUN {result.status}")
    for o in result.steps:
        detail = (o.reason if o.status == "skipped" else str(o.result or o.error or ""))[:70]
        print(f"      step {o.step_number} {o.tool_name}: {o.status} ({o.latency_ms}ms) {detail}")

    # the ACTUAL validated SQL that executed (window substituted, re-validated)
    print(f"\n    Executed SQL (window substituted + re-validated):\n      {result.steps[0].result['sql']}")

    # ---- 5. formatted report + outbox artifact ----
    print("\n[5] Formatted report delivered to Slack (the compact form):")
    for line in result.steps[1].result["slack_text"].splitlines():
        print(f"      {line}")
    print("\n    OUTBOX (exact payload delivered):")
    for row in store.outbox():
        print(f"      [{row['connector']}] {row['payload_json'][:110]}")

    # ---- 6. persisted workflow state ----
    wf = store.get_workflow(wid)
    print(f"\n[6] Persisted state: {wf['state']}  ·  last_run={wf['last_run_at']}  ·  "
          f"next_run={wf['next_run_at']}")
    print(f"    last_report={wf['last_report_json']}")

    # re-fire the SAME period -> idempotent no-op (no duplicate report)
    dup = runtime._fire(wid, "daily", fire_dt=fire.replace(minute=3))
    print(f"\n    Re-fire same period -> {'skipped (idempotent)' if dup is None else dup.status}; "
          f"outbox still has {len(store.outbox())} row(s).")

    # ---- 7. the destructive scheduled request -> REJECTED ----
    print(f"\n[7] Instruction: {SCHEDULED_REFUSAL}")
    wid2 = store.create_workflow("analyst", SCHEDULED_REFUSAL)
    store.transition(wid2, WorkflowState.PLANNED)
    plan2 = planner.plan(SCHEDULED_REFUSAL)
    _print_plan(plan2)
    report2 = check_plan(plan2, registry)
    store.transition(wid2, WorkflowState.REJECTED if not report2.ok else WorkflowState.FEASIBLE)
    print(f"\n    FEASIBILITY: {'PASS' if report2.ok else 'REJECTED'} — the guardrail held")
    for e in report2.errors:
        print(f"      - {e}")

    print("\n" + "=" * 70)
    store.close()


# ---------------------------------------------------------------- demo flow
def _print_plan(plan: WorkflowPlan) -> None:
    print(f"\n  goal: {plan.goal}")
    print(f"  trigger: {plan.trigger}")
    if plan.schedule is not None:
        print(f"  schedule: {plan.schedule.model_dump(exclude_none=True)}")
    if plan.needs_clarification:
        print(f"  needs_clarification: {plan.clarification_question}")
    for s in plan.steps:
        guard = f"  when={s.when!r}" if s.when else ""
        print(f"    {s.step_number}. {s.tool_name}({json.dumps(s.args)})  — {s.rationale}{guard}")


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
    ap.add_argument("--scheduled", action="store_true",
                    help="run the SCHEDULED-report demo (NL → schedule → simulated "
                         "trigger → formatted report → outbox) instead of the "
                         "event/conditional demo")
    ap.add_argument("--user", default="demo-user", help="user_id for the simulated event")
    args = ap.parse_args()
    if args.scheduled:
        run_scheduled(mock=args.mock or True)   # scheduled demo is mock-only for now
    else:
        run(mock=args.mock, event_user_id=args.user)


if __name__ == "__main__":
    main()
