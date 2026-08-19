# =============================================================
# UNIT TESTS — scheduled execution (no wall clock, no infra)
# =============================================================
# _fire is called directly with a fixed fire time, over an in-memory
# store and fake tools. Covers: a scheduled run writes the expected
# outbox artifact; a duplicate fire in the same period is a no-op; a
# connector failure ends the run FAILED without corrupting state or
# double-sending; the window is computed and passed as a parameter;
# and reload() restart-recovery re-registers persisted schedules.
# =============================================================

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from fraud_platform.workflow_engine.connectors import make_slack_send
from fraud_platform.workflow_engine.executor import Executor
from fraud_platform.workflow_engine.planner import PlanStep, WorkflowPlan
from fraud_platform.workflow_engine.registry import (
    ANALYZE, NOTIFY, READ_DATA, ToolRegistry, ToolSpec,
)
from fraud_platform.workflow_engine.report import format_report
from fraud_platform.workflow_engine.schedule import Schedule
from fraud_platform.workflow_engine.scheduler import (
    SchedulerRuntime, compute_window, run_key,
)
from fraud_platform.workflow_engine.state import WorkflowState, WorkflowStore

import importlib.util

import pytest

# Firing a schedule builds a real APScheduler cron trigger; skip the whole
# module when the [workflow] extra (apscheduler) isn't installed — CI's base
# test env doesn't have it. Importing the modules above is safe (apscheduler
# is imported lazily inside the scheduler); only running needs it.
pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("apscheduler") is None,
    reason="scheduled execution needs the [workflow] extra (apscheduler)",
)


class _ReportArgs(BaseModel):
    report: str = Field(description="x")
    window_start: str = Field(description="x")
    window_end: str = Field(description="x")


class _FormatArgs(BaseModel):
    title: str = Field(description="x")
    data: dict = Field(description="x")


class _SlackArgs(BaseModel):
    channel: str = Field(description="x")
    text: str = Field(description="x")


def _fixture(explode_slack=False):
    store = WorkflowStore(db_path=":memory:")
    r = ToolRegistry()
    # fake run_report_query: canned single-row report, echoes the window it got
    r.register(ToolSpec("run_report_query", "run stored report", _ReportArgs, READ_DATA,
                        execute=lambda report, window_start, window_end: {
                            "columns": ["transactions_reviewed", "block_count",
                                        "escalate_count", "fraud_rate", "top_fraud_pattern"],
                            "rows": [[4820, 102, 25, 0.0263, "VELOCITY_SPIKE"]],
                            "sql": f"-- {report} [{window_start},{window_end})",
                        }))
    r.register(ToolSpec("format_report", "format", _FormatArgs, ANALYZE,
                        execute=format_report))
    if explode_slack:
        def _boom(channel, text):
            raise RuntimeError("slack connector down")
        r.register(ToolSpec("slack_send_message", "slack", _SlackArgs, NOTIFY,
                            execute=_boom, read_only=False))
    else:
        r.register(ToolSpec("slack_send_message", "slack", _SlackArgs, NOTIFY,
                            execute=make_slack_send(store.outbox_writer()), read_only=False))
    return store, SchedulerRuntime(store, Executor(r, store))


def _scheduled_report_workflow(store) -> str:
    """A persisted, READY, schedule-triggered report workflow whose plan
    is run_report_query -> format_report -> slack, windowed by $trigger."""
    plan = WorkflowPlan(goal="daily fraud report", trigger=None, steps=[
        PlanStep(step_number=1, tool_name="run_report_query",
                 args={"report": "fraud_performance",
                       "window_start": "$trigger.window_start",
                       "window_end": "$trigger.window_end"}, rationale="run"),
        PlanStep(step_number=2, tool_name="format_report",
                 args={"title": "Daily Fraud Operations Report", "data": "$step_1"},
                 rationale="format"),
        PlanStep(step_number=3, tool_name="slack_send_message",
                 args={"channel": "#fraud-ops", "text": "$step_2.slack_text"},
                 rationale="deliver"),
    ])
    wid = store.create_workflow("analyst", "daily fraud report to #fraud-ops")
    store.transition(wid, WorkflowState.PLANNED)
    store.set_plan(wid, plan.model_dump_json())
    store.transition(wid, WorkflowState.FEASIBLE)
    store.transition(wid, WorkflowState.READY)
    sched = Schedule(frequency="daily", hour=22, minute=0, timezone="America/New_York")
    store.set_schedule(wid, sched.model_dump_json(),
                       destination_json='{"connector":"slack","channel":"#fraud-ops"}')
    return wid


FIRE = datetime(2026, 8, 19, 22, 0, tzinfo=timezone.utc)


class TestWindowAndKey:
    def test_daily_window_is_previous_24h(self):
        s, e = compute_window("daily", FIRE)
        assert s == "2026-08-18T22:00:00" and e == "2026-08-19T22:00:00"

    def test_run_key_collapses_same_day(self):
        later = FIRE.replace(minute=5)
        assert run_key("daily", FIRE) == run_key("daily", later) == "daily:2026-08-19"


class TestScheduledExecution:
    def test_fire_writes_outbox_artifact_and_persists_state(self):
        store, runtime = _fixture()
        wid = _scheduled_report_workflow(store)
        result = runtime._fire(wid, "daily", fire_dt=FIRE)

        assert result.status == "COMPLETED"
        assert [o.status for o in result.steps] == ["ok", "ok", "ok"]
        outbox = store.outbox()
        assert len(outbox) == 1 and outbox[0]["connector"] == "slack"
        # the formatted (not raw) report reached Slack
        assert "Daily Fraud Operations Report" in outbox[0]["payload_json"]
        assert "Fraud rate: 2.63%" in outbox[0]["payload_json"]
        # persisted: last_run + last_report + workflow re-armed to COMPLETED
        wf = store.get_workflow(wid)
        assert wf["last_run_at"] is not None
        assert "Daily Fraud Operations Report" in wf["last_report_json"]
        assert wf["state"] == "COMPLETED"
        store.close()

    def test_window_is_passed_through_as_a_parameter(self):
        store, runtime = _fixture()
        wid = _scheduled_report_workflow(store)
        result = runtime._fire(wid, "daily", fire_dt=FIRE)
        # step 1 received the computed window (echoed into its canned sql)
        assert "2026-08-18T22:00:00,2026-08-19T22:00:00" in result.steps[0].result["sql"]
        store.close()

    def test_duplicate_fire_same_period_is_noop(self):
        store, runtime = _fixture()
        wid = _scheduled_report_workflow(store)
        runtime._fire(wid, "daily", fire_dt=FIRE)
        again = runtime._fire(wid, "daily", fire_dt=FIRE.replace(minute=1))  # same day
        assert again is None                       # skipped, not re-run
        assert len(store.outbox()) == 1            # NOT sent twice
        store.close()

    def test_paused_workflow_does_not_fire(self):
        store, runtime = _fixture()
        wid = _scheduled_report_workflow(store)
        store.set_paused(wid, True)
        assert runtime._fire(wid, "daily", fire_dt=FIRE) is None
        assert store.outbox() == []
        store.close()

    def test_connector_failure_fails_run_without_corrupting_state(self):
        store, runtime = _fixture(explode_slack=True)
        wid = _scheduled_report_workflow(store)
        result = runtime._fire(wid, "daily", fire_dt=FIRE)
        assert result.status == "FAILED"
        assert result.steps[-1].status == "error" and "slack connector down" in result.steps[-1].error
        assert store.outbox() == []                       # nothing delivered
        assert store.get_workflow(wid)["state"] == "FAILED"
        # a NEW period can still run (state is recoverable, not wedged)
        next_day = FIRE.replace(day=20)
        result2 = runtime._fire(wid, "daily", fire_dt=next_day)
        assert result2.status == "FAILED"                 # still failing tool, but it RAN
        store.close()


class TestRestartRecovery:
    def test_reload_registers_persisted_schedules(self):
        store, runtime = _fixture()
        wid = _scheduled_report_workflow(store)
        # start a real background scheduler, then reload from SQLite
        runtime.start()
        try:
            assert runtime._sched.get_job(wid) is not None   # job registered from persistence
            assert store.get_workflow(wid)["next_run_at"] is not None
        finally:
            runtime.shutdown()
        store.close()
