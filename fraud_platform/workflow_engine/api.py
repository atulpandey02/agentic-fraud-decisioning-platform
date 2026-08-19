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
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import config
from .executor import Executor
from .feasibility import check_plan
from .planner import Planner, WorkflowPlan
from .registry import ToolRegistry
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


def build_engine(mock: bool = False, db_path: Optional[str] = None) -> WorkflowEngine:
    store = WorkflowStore(db_path)
    if mock:
        from .run_demo import _CannedLLM, _mock_registry
        registry = _mock_registry(store)
        planner = Planner(registry, llm=_CannedLLM())
    else:
        registry = build_default_registry(store.outbox_writer())
        planner = Planner(registry)
    return WorkflowEngine(store, registry, planner, Executor(registry, store))


# ---------------------------------------------------------------- request models
class CreateWorkflowRequest(BaseModel):
    instruction: str
    user_id: Optional[str] = None


class ExecuteRequest(BaseModel):
    trigger_payload: Optional[dict] = None


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
             "result": o.result, "error": o.error, "latency_ms": o.latency_ms}
            for o in result.steps
        ],
    }


def create_app(engine: Optional[WorkflowEngine] = None) -> FastAPI:
    app = FastAPI(title="Fraud Workflow Engine", version="0.1.0")
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

    return app


app = create_app()
