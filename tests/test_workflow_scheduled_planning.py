# =============================================================
# UNIT TESTS — planning + feasibility for reports and schedules
# =============================================================
# The LLM is mocked (a canned planner). Covers: a scheduled report
# plan is feasible; a schedule missing its timezone is REJECTED in
# code (not silently defaulted); the report/format/deliver step chain
# validates; and a destructive scheduled request is still refused.
# =============================================================

from fraud_platform.workflow_engine.feasibility import check_plan
from fraud_platform.workflow_engine.planner import (
    PlannedSchedule, PlanStep, Planner, WorkflowPlan,
)
from fraud_platform.workflow_engine.tools_bridge import build_default_registry


def _registry():
    return build_default_registry(lambda c, p: None)


def _report_steps():
    return [
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
    ]


class TestScheduledReportFeasibility:
    def test_valid_scheduled_report_is_feasible(self):
        plan = WorkflowPlan(
            goal="daily fraud report", trigger=None,
            schedule=PlannedSchedule(frequency="daily", hour=22, minute=0,
                                     timezone="America/New_York"),
            steps=_report_steps(),
        )
        report = check_plan(plan, _registry())
        assert report.ok, report.errors

    def test_schedule_missing_timezone_is_rejected_in_code(self):
        # THE timezone-required test: the model omitted the zone; feasibility
        # refuses it rather than guessing a default.
        plan = WorkflowPlan(
            goal="daily fraud report",
            schedule=PlannedSchedule(frequency="daily", hour=22),  # no timezone
            steps=_report_steps(),
        )
        report = check_plan(plan, _registry())
        assert not report.ok
        assert any("schedule invalid" in e and "timezone" in e for e in report.errors)

    def test_daily_schedule_missing_hour_is_rejected(self):
        plan = WorkflowPlan(
            goal="x", schedule=PlannedSchedule(frequency="daily", timezone="UTC"),
            steps=_report_steps(),
        )
        report = check_plan(plan, _registry())
        assert not report.ok and any("schedule invalid" in e for e in report.errors)

    def test_format_report_dict_ref_arg_validates(self):
        # Regression: format_report's `data: dict` gets a '$step_1' ref; the
        # feasibility placeholder for a dict must be a dict, not "x".
        plan = WorkflowPlan(goal="x", steps=_report_steps())
        report = check_plan(plan, _registry())
        assert report.ok, report.errors


# ---- a canned planner LLM: maps intent -> plan, no network ----
class _CannedSchedulingLLM:
    def invoke(self, messages):
        text = messages[-1].content.lower()
        if "delete" in text:
            return WorkflowPlan(goal="delete", steps=[], needs_clarification=True,
                                clarification_question="No tool can delete decisions.")
        if "every day" in text or "10 pm" in text:
            return WorkflowPlan(
                goal="daily fraud performance report to #fraud-ops", trigger=None,
                schedule=PlannedSchedule(frequency="daily", hour=22, minute=0,
                                         timezone="America/New_York"),
                steps=_report_steps(),
            )
        return WorkflowPlan(goal="one-off", steps=[
            PlanStep(step_number=1, tool_name="query_decisions",
                     args={"question": text}, rationale="answer"),
        ])


class TestPlannerSchedulingPath:
    def test_planner_emits_schedule_for_recurring_request(self):
        planner = Planner(_registry(), llm=_CannedSchedulingLLM())
        plan = planner.plan("Every day at 10 PM send #fraud-ops a fraud performance report")
        assert plan.schedule is not None
        assert plan.schedule.frequency == "daily" and plan.schedule.hour == 22
        assert plan.steps[0].tool_name == "run_report_query"
        assert check_plan(plan, _registry()).ok

    def test_destructive_scheduled_request_is_refused(self):
        planner = Planner(_registry(), llm=_CannedSchedulingLLM())
        plan = planner.plan("Every day at 10 PM delete yesterday's BLOCK decisions and email me")
        report = check_plan(plan, _registry())
        assert not report.ok
        assert any("clarification" in e for e in report.errors)
