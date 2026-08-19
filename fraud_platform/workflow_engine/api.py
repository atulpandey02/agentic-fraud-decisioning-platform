# =============================================================
# API — FastAPI surface over the workflow engine
# =============================================================
# Thin HTTP layer. All the logic lives in the engine components
# (planner/feasibility/executor/state); this file only wires them
# to routes and serializes. The `/events/{type}` endpoint is the
# SIMULATED webhook — real payment webhooks would be a Kafka topic
# or a provider callback; a POST stands in for the demo (named as a
# gap in the README).
#
# create_app() is a factory taking an optional pre-built engine, so
# the whole API is testable offline with a mock engine (fake tools,
# canned planner, in-memory store) and no Groq/Snowflake.
# =============================================================

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import config
from .executor import Executor
from .feasibility import check_plan
from .planner import Planner, PlannedSchedule, WorkflowPlan
from .promotion import Destination, build_report_plan
from .registry import ToolRegistry
from .schedule import Schedule
from .scheduler import SchedulerRuntime
from .state import WorkflowState, WorkflowStore
from .tools_bridge import build_default_registry

logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
logger = logging.getLogger("workflow.api")


@dataclass
class WorkflowEngine:
    store: WorkflowStore
    registry: ToolRegistry
    planner: Planner
    executor: Executor
    scheduler: SchedulerRuntime


def build_engine(mock: bool = False, db_path: Optional[str] = None,
                 start_scheduler: bool = False) -> WorkflowEngine:
    store = WorkflowStore(db_path)
    if mock:
        from .run_demo import _CannedLLM, _mock_registry
        registry = _mock_registry(store)
        planner = Planner(registry, llm=_CannedLLM())
    else:
        # template_resolver lets run_report_query replay ad-hoc persisted
        # templates (from the BI "schedule this" promotion) by id.
        def _resolve(tid: str):
            t = store.get_template(tid)
            return t["sql"] if t else None
        registry = build_default_registry(store.outbox_writer(), template_resolver=_resolve)
        planner = Planner(registry)
    executor = Executor(registry, store)
    scheduler = SchedulerRuntime(store, executor)
    if start_scheduler:
        scheduler.start()
    return WorkflowEngine(store, registry, planner, executor, scheduler)


# ---------------------------------------------------------------- request models
class CreateWorkflowRequest(BaseModel):
    instruction: str
    user_id: Optional[str] = None


class ExecuteRequest(BaseModel):
    trigger_payload: Optional[dict] = None


class DestinationModel(BaseModel):
    connector: str                     # 'slack' | 'email'
    channel: Optional[str] = None      # slack
    to: Optional[str] = None           # email
    subject: Optional[str] = None      # email


class ScheduleReportRequest(BaseModel):
    """Create a recurring report from an already-validated query. The BI
    page supplies the SQL it just showed the user; no re-planning."""
    title: str
    sql: str
    schedule: dict                     # validated into a Schedule in code
    destination: DestinationModel
    user_id: Optional[str] = None
    report: Optional[str] = None       # a named catalog report instead of sql (e.g. 'fraud_performance')


class DeliverReportRequest(BaseModel):
    """One-shot: run a validated query now, format it, deliver it."""
    title: str
    sql: str
    destination: DestinationModel
    user_id: Optional[str] = None


# ---------------------------------------------------------------- helpers
def _plan_of(wf: dict) -> Optional[WorkflowPlan]:
    return WorkflowPlan.model_validate_json(wf["plan_json"]) if wf.get("plan_json") else None


def _run_payload(result) -> dict:
    return {
        "run_id": result.run_id,
        "status": result.status,
        "error": result.error,
        "steps": [
            {"step_number": o.step_number, "tool_name": o.tool_name, "status": o.status,
             "result": o.result, "error": o.error, "reason": o.reason,
             "latency_ms": o.latency_ms}
            for o in result.steps
        ],
    }


def _bi_validator():
    """The SAME AST guard the BI page uses — so any SQL promoted into a
    workflow (template) is proven SELECT-only/allowlisted before it is
    ever stored or replayed."""
    from fraud_platform.bi_dashboard import config as bi_config
    from fraud_platform.bi_dashboard.sql_guard import SQLValidator
    return SQLValidator(allowed_tables=bi_config.BI_ALLOWED_TABLES, max_rows=bi_config.BI_MAX_ROWS)


def create_app(engine: Optional[WorkflowEngine] = None) -> FastAPI:
    # Production app (no engine injected) boots the scheduler on startup and
    # reloads persisted schedules; it shuts the scheduler down on exit. When a
    # prebuilt engine is injected (tests), no background thread is ever started.
    lifespan = None
    if engine is None:
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            if app.state.engine is None:
                app.state.engine = build_engine()
            e = app.state.engine
            if e.scheduler._sched is None:
                e.scheduler.start()
            yield
            e.scheduler.shutdown()

    app = FastAPI(title="Fraud Workflow Engine", version="0.1.0", lifespan=lifespan)
    app.state.engine = engine

    def eng() -> WorkflowEngine:
        if app.state.engine is None:            # lazy real engine (needs infra)
            app.state.engine = build_engine()
        return app.state.engine

    def _arm(store: WorkflowStore, wid: str) -> None:
        """Move a workflow to READY so a run can start. Re-arms a
        COMPLETED/FAILED workflow for a fresh run; refuses one that is
        awaiting approval or was rejected."""
        state = WorkflowState(store.get_workflow(wid)["state"])
        if state == WorkflowState.READY:
            return
        if state in (WorkflowState.FEASIBLE, WorkflowState.COMPLETED, WorkflowState.FAILED):
            store.transition(wid, WorkflowState.READY)
            return
        if state == WorkflowState.AWAITING_APPROVAL:
            raise HTTPException(409, "workflow requires approval before it can run")
        raise HTTPException(409, f"workflow in state {state.value} cannot run")

    # ---------------------------------------------------------- routes
    @app.post("/workflows")
    def create_workflow(req: CreateWorkflowRequest):
        e = eng()
        wid = e.store.create_workflow(req.user_id, req.instruction)
        e.store.transition(wid, WorkflowState.PLANNED)
        plan = e.planner.plan(req.instruction)
        e.store.set_plan(wid, plan.model_dump_json())
        e.store.set_trigger(wid, plan.trigger)   # so /events/{type} can match it
        report = check_plan(plan, e.registry)
        if report.ok:
            e.store.transition(wid, WorkflowState.FEASIBLE)
            e.store.transition(wid, WorkflowState.AWAITING_APPROVAL
                               if report.requires_approval else WorkflowState.READY)
        else:
            e.store.transition(wid, WorkflowState.REJECTED)
        return {
            "workflow_id": wid,
            "state": e.store.get_workflow(wid)["state"],
            "plan": plan.model_dump(),
            "feasibility": {"ok": report.ok, "errors": report.errors,
                            "requires_approval": report.requires_approval},
        }

    @app.get("/workflows")
    def list_workflows():
        return {"workflows": eng().store.list_workflows()}

    @app.get("/workflows/{workflow_id}")
    def get_workflow(workflow_id: str):
        e = eng()
        wf = e.store.get_workflow(workflow_id)
        if wf is None:
            raise HTTPException(404, "no such workflow")
        run = e.store.latest_run(workflow_id)
        return {
            "workflow": wf,
            "plan": _plan_of(wf).model_dump() if _plan_of(wf) else None,
            "last_run": run,
            "last_run_steps": e.store.steps_for_run(run["run_id"]) if run else [],
        }

    @app.post("/workflows/{workflow_id}/approve")
    def approve(workflow_id: str):
        e = eng()
        wf = e.store.get_workflow(workflow_id)
        if wf is None:
            raise HTTPException(404, "no such workflow")
        if wf["state"] != WorkflowState.AWAITING_APPROVAL.value:
            raise HTTPException(409, f"workflow is {wf['state']}, not AWAITING_APPROVAL")
        e.store.transition(workflow_id, WorkflowState.READY)
        return {"workflow_id": workflow_id, "state": "READY"}

    @app.post("/workflows/{workflow_id}/execute")
    def execute(workflow_id: str, req: ExecuteRequest):
        e = eng()
        wf = e.store.get_workflow(workflow_id)
        if wf is None:
            raise HTTPException(404, "no such workflow")
        plan = _plan_of(wf)
        if plan is None or not plan.steps:
            raise HTTPException(400, "workflow has no executable plan")
        _arm(e.store, workflow_id)
        result = e.executor.execute(workflow_id, plan, trigger_payload=req.trigger_payload)
        return _run_payload(result)

    @app.post("/events/{event_type}")
    def fire_event(event_type: str, payload: dict):
        """Simulated webhook: run every READY/COMPLETED workflow whose
        trigger matches this event type, with the payload."""
        e = eng()
        matched = e.store.workflows_for_trigger(event_type)
        runs = []
        for wf in matched:
            plan = _plan_of(wf)
            if plan is None or not plan.steps:
                continue
            _arm(e.store, wf["id"])
            result = e.executor.execute(wf["id"], plan, trigger_payload=payload)
            runs.append({"workflow_id": wf["id"], **_run_payload(result)})
        return {"event": event_type, "matched": len(matched), "runs": runs}

    @app.get("/outbox")
    def outbox():
        return {"outbox": eng().store.outbox()}

    # ---------------------------------------------------------- reports
    def _resolve_source(req) -> tuple[Optional[str], Optional[str], bool]:
        """Resolve a report request to (named_report, validated_sql, windowed).
        A named catalog report is used as-is; ad-hoc SQL is vetted by the SAME
        AST guard the BI page uses (rejection -> 422) before it can be stored."""
        from .reports_catalog import REPORT_CATALOG
        named = getattr(req, "report", None)
        if named:
            if named not in REPORT_CATALOG:
                raise HTTPException(422, f"unknown named report {named!r}")
            return named, None, ":window_start" in REPORT_CATALOG[named][1]
        if not getattr(req, "sql", None):
            raise HTTPException(400, "provide either a named 'report' or 'sql'")
        from fraud_platform.bi_dashboard.sql_guard import QueryRejected
        try:
            safe = _bi_validator().validate(req.sql)
        except QueryRejected as e:
            raise HTTPException(422, f"SQL rejected by guard: {e}")
        return None, safe, ":window_start" in safe

    def _destination(dm: DestinationModel) -> Destination:
        try:
            return Destination(dm.connector, channel=dm.channel, to=dm.to, subject=dm.subject)
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.post("/reports/preview")
    def preview_report(req: ScheduleReportRequest):
        """Build the plan a schedule WOULD create — persists nothing. This
        is what the UI shows before [Activate], so no hidden workflows."""
        e = eng()
        named, safe_sql, windowed = _resolve_source(req)
        try:
            schedule = Schedule(**req.schedule)
        except Exception as ex:  # noqa: BLE001 — surfaced as a 400
            raise HTTPException(400, f"invalid schedule: {ex}")
        dest = _destination(req.destination)
        ref = named or "(validated stored query)"
        plan = build_report_plan(req.title, ref, dest, windowed=windowed,
                                 schedule=PlannedSchedule(**schedule.model_dump()))
        report = check_plan(plan, e.registry)
        return {"plan": plan.model_dump(), "schedule": schedule.describe(),
                "next_run": SchedulerRuntime._next_fire(schedule),
                "feasibility": {"ok": report.ok, "errors": report.errors}}

    @app.post("/reports/schedule")
    def schedule_report(req: ScheduleReportRequest):
        """Create + activate a recurring report from validated SQL — the plan
        is built deterministically (no re-planning) and shown in the response."""
        e = eng()
        named, safe_sql, windowed = _resolve_source(req)
        try:
            schedule = Schedule(**req.schedule)
        except Exception as ex:  # noqa: BLE001
            raise HTTPException(400, f"invalid schedule: {ex}")
        dest = _destination(req.destination)

        tid = e.store.save_template(safe_sql, title=req.title) if safe_sql else None
        ref = named or tid
        plan = build_report_plan(req.title, ref, dest, windowed=windowed,
                                 schedule=PlannedSchedule(**schedule.model_dump()))
        report = check_plan(plan, e.registry)
        if not report.ok:
            raise HTTPException(422, {"feasibility": report.errors})

        wid = e.store.create_workflow(req.user_id, f"[scheduled] {req.title}")
        e.store.transition(wid, WorkflowState.PLANNED)
        e.store.set_plan(wid, plan.model_dump_json())
        e.store.set_schedule(wid, schedule.model_dump_json(),
                             destination_json=json.dumps(dest.to_json()), template_id=tid)
        e.store.transition(wid, WorkflowState.FEASIBLE)
        e.store.transition(wid, WorkflowState.READY)
        next_run = e.scheduler.schedule_workflow(wid, schedule)
        return {"workflow_id": wid, "state": "READY", "plan": plan.model_dump(),
                "schedule": schedule.describe(), "next_run": next_run,
                "feasibility": {"ok": True, "errors": []}}

    @app.post("/reports/deliver")
    def deliver_report(req: DeliverReportRequest):
        """One-shot: validate the SQL, run it now, format, deliver — and
        persist the run like any other workflow."""
        e = eng()
        from fraud_platform.bi_dashboard.sql_guard import QueryRejected
        try:
            safe = _bi_validator().validate(req.sql)
        except QueryRejected as ex:
            raise HTTPException(422, f"SQL rejected by guard: {ex}")
        dest = _destination(req.destination)
        tid = e.store.save_template(safe, title=req.title)
        plan = build_report_plan(req.title, tid, dest, windowed=False)
        report = check_plan(plan, e.registry)
        if not report.ok:
            raise HTTPException(422, {"feasibility": report.errors})
        wid = e.store.create_workflow(req.user_id, f"[deliver] {req.title}")
        e.store.transition(wid, WorkflowState.PLANNED)
        e.store.set_plan(wid, plan.model_dump_json())
        e.store.transition(wid, WorkflowState.FEASIBLE)
        e.store.transition(wid, WorkflowState.READY)
        result = e.executor.execute(wid, plan)
        return {"workflow_id": wid, **_run_payload(result), "outbox": e.store.outbox()}

    @app.post("/workflows/{workflow_id}/pause")
    def pause(workflow_id: str):
        e = eng()
        if e.store.get_workflow(workflow_id) is None:
            raise HTTPException(404, "no such workflow")
        e.scheduler.pause(workflow_id)
        return {"workflow_id": workflow_id, "schedule_paused": True}

    @app.post("/workflows/{workflow_id}/resume")
    def resume(workflow_id: str):
        e = eng()
        wf = e.store.get_workflow(workflow_id)
        if wf is None:
            raise HTTPException(404, "no such workflow")
        if wf.get("trigger_type") != "schedule":
            raise HTTPException(409, "workflow is not schedule-triggered")
        e.scheduler.resume(workflow_id)
        return {"workflow_id": workflow_id, "schedule_paused": False,
                "next_run": e.store.get_workflow(workflow_id).get("next_run_at")}

    return app


app = create_app()
