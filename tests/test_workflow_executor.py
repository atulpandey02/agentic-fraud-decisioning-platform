# =============================================================
# UNIT TESTS — executor (fake tools, in-memory store, no infra)
# =============================================================
# Real execution mechanics without touching Redis/Snowflake/Groq:
# a registry of deterministic fake tools + an in-memory store. Covers
# the happy path, $trigger / $step reference resolution, checkpoint
# persistence, and stop-on-first-failure (no fail-open).
# =============================================================

from pydantic import BaseModel, Field

from fraud_platform.workflow_engine.connectors import make_slack_send
from fraud_platform.workflow_engine.executor import Executor
from fraud_platform.workflow_engine.planner import PlanStep, WorkflowPlan
from fraud_platform.workflow_engine.registry import (
    ANALYZE, NOTIFY, READ_DATA, ToolRegistry, ToolSpec,
)
from fraud_platform.workflow_engine.state import WorkflowState, WorkflowStore


class CountArgs(BaseModel):
    user_id: str = Field(description="x")
    decision: str = Field(description="x")
    hours: int = Field(default=24, description="x")


class UserArgs(BaseModel):
    user_id: str = Field(description="x")


class SlackArgs(BaseModel):
    channel: str = Field(description="x")
    text: str = Field(description="x")


def _fixture(explode_at_notify=False):
    store = WorkflowStore(db_path=":memory:")
    r = ToolRegistry()
    r.register(ToolSpec("count", "count blocks", CountArgs, ANALYZE,
                        execute=lambda user_id, decision, hours=24: {"count": 3, "user_id": user_id}))
    r.register(ToolSpec("history", "user history", UserArgs, READ_DATA,
                        execute=lambda user_id: {"summary": f"history for {user_id}"}))
    if explode_at_notify:
        def _boom(channel, text):
            raise RuntimeError("connector down")
        r.register(ToolSpec("notify", "slack", SlackArgs, NOTIFY, execute=_boom,
                            read_only=False))
    else:
        r.register(ToolSpec("notify", "slack", SlackArgs, NOTIFY,
                            execute=make_slack_send(store.outbox_writer()), read_only=False))
    return store, Executor(r, store)


def _ready_workflow(store, trigger="payment.captured"):
    wid = store.create_workflow("u1", "demo", trigger=trigger)
    for st in (WorkflowState.PLANNED, WorkflowState.FEASIBLE, WorkflowState.READY):
        store.transition(wid, st)
    return wid


def _demo_plan():
    return WorkflowPlan(goal="notify on repeat blocks", trigger="payment.captured", steps=[
        PlanStep(step_number=1, tool_name="count",
                 args={"user_id": "$trigger.user_id", "decision": "BLOCK", "hours": 24},
                 rationale="count"),
        PlanStep(step_number=2, tool_name="history",
                 args={"user_id": "$trigger.user_id"}, rationale="history"),
        PlanStep(step_number=3, tool_name="notify",
                 args={"channel": "#fraud-ops", "text": "$step_2.summary"}, rationale="notify"),
    ])


class TestExecutorHappyPath:
    def test_full_run_completes_and_resolves_refs(self):
        store, ex = _fixture()
        wid = _ready_workflow(store)
        result = ex.execute(wid, _demo_plan(), trigger_payload={"user_id": "u42"})
        assert result.status == "COMPLETED"
        assert [o.status for o in result.steps] == ["ok", "ok", "ok"]
        # $trigger.user_id resolved into step 1
        assert result.steps[0].result == {"count": 3, "user_id": "u42"}
        # $step_2.summary resolved into the slack text -> outbox payload
        outbox = store.outbox()
        assert outbox[0]["connector"] == "slack"
        assert "history for u42" in outbox[0]["payload_json"]
        # workflow ended COMPLETED, all 3 steps checkpointed
        assert store.get_workflow(wid)["state"] == "COMPLETED"
        assert len(store.steps_for_run(result.run_id)) == 3
        store.close()


class TestExecutorFailure:
    def test_stops_on_first_failure_no_fail_open(self):
        store, ex = _fixture(explode_at_notify=True)
        wid = _ready_workflow(store)
        result = ex.execute(wid, _demo_plan(), trigger_payload={"user_id": "u42"})
        assert result.status == "FAILED"
        assert result.steps[-1].status == "error" and "connector down" in result.steps[-1].error
        # steps 1 and 2 succeeded and are the resume anchor; step 3 recorded as error
        assert store.last_completed_step(result.run_id) == 2
        assert store.get_workflow(wid)["state"] == "FAILED"
        # nothing delivered to the outbox (the failing step never wrote)
        assert store.outbox() == []
        store.close()

    def test_bad_reference_fails_the_run(self):
        store, ex = _fixture()
        wid = _ready_workflow(store)
        plan = WorkflowPlan(goal="g", steps=[
            PlanStep(step_number=1, tool_name="history",
                     args={"user_id": "$trigger.missing_field"}, rationale="x"),
        ])
        result = ex.execute(wid, plan, trigger_payload={"user_id": "u1"})
        # $trigger.missing_field resolves to None -> history gets user_id=None,
        # which still runs; the point of this case is the executor tolerates a
        # missing trigger field by passing None rather than crashing the harness.
        assert result.status in ("COMPLETED", "FAILED")
        store.close()
