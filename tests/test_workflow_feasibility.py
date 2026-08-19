# =============================================================
# UNIT TESTS — feasibility checker (deterministic, no LLM)
# =============================================================

from pydantic import BaseModel, Field

from fraud_platform.workflow_engine.feasibility import check_plan
from fraud_platform.workflow_engine.planner import PlanStep, WorkflowPlan
from fraud_platform.workflow_engine.registry import (
    NOTIFY, ToolRegistry, ToolSpec,
)
from fraud_platform.workflow_engine.tools_bridge import build_default_registry


def _registry():
    return build_default_registry(lambda c, p: None)


def _plan(steps, needs_clarification=False, clarification=None):
    return WorkflowPlan(goal="g", trigger="payment.captured", steps=steps,
                        needs_clarification=needs_clarification,
                        clarification_question=clarification)


class TestFeasibilityHappyPath:
    def test_valid_three_step_plan_passes(self):
        plan = _plan([
            PlanStep(step_number=1, tool_name="count_recent_decisions",
                     args={"user_id": "$trigger.user_id", "decision": "BLOCK", "hours": 24},
                     rationale="count blocks"),
            PlanStep(step_number=2, tool_name="get_user_history",
                     args={"user_id": "$trigger.user_id"}, rationale="pull history"),
            PlanStep(step_number=3, tool_name="slack_send_message",
                     args={"channel": "#fraud-ops", "text": "High-risk user flagged"},
                     rationale="notify"),
        ])
        report = check_plan(plan, _registry())
        assert report.ok, report.errors
        assert report.requires_approval is False


class TestFeasibilityFailures:
    def test_unknown_tool_is_rejected(self):
        plan = _plan([PlanStep(step_number=1, tool_name="delete_decisions",
                               args={}, rationale="destructive")])
        report = check_plan(plan, _registry())
        assert not report.ok
        assert any("unknown tool" in e for e in report.errors)

    def test_bad_arg_type_is_rejected(self):
        plan = _plan([PlanStep(step_number=1, tool_name="count_recent_decisions",
                               args={"user_id": "u1", "decision": "BLOCK", "hours": "not-int"},
                               rationale="x")])
        report = check_plan(plan, _registry())
        assert not report.ok and any("hours" in e for e in report.errors)

    def test_unknown_arg_key_is_rejected(self):
        plan = _plan([PlanStep(step_number=1, tool_name="get_user_history",
                               args={"user_id": "u1", "bogus": 1}, rationale="x")])
        report = check_plan(plan, _registry())
        assert not report.ok and any("bogus" in e for e in report.errors)

    def test_forward_reference_is_rejected(self):
        plan = _plan([
            PlanStep(step_number=1, tool_name="get_user_history",
                     args={"user_id": "$step_2.user_id"}, rationale="forward ref"),
            PlanStep(step_number=2, tool_name="get_user_history",
                     args={"user_id": "u1"}, rationale="x"),
        ])
        report = check_plan(plan, _registry())
        assert not report.ok and any("backward" in e for e in report.errors)

    def test_backward_reference_is_allowed(self):
        plan = _plan([
            PlanStep(step_number=1, tool_name="get_user_history",
                     args={"user_id": "u1"}, rationale="x"),
            PlanStep(step_number=2, tool_name="get_transaction_features",
                     args={"user_id": "$step_1.user_id"}, rationale="backward ref ok"),
        ])
        report = check_plan(plan, _registry())
        assert report.ok, report.errors

    def test_step_cap_enforced(self):
        steps = [PlanStep(step_number=i, tool_name="get_user_history",
                          args={"user_id": "u1"}, rationale="x") for i in range(1, 12)]
        report = check_plan(_plan(steps), _registry())
        assert not report.ok and any("max is" in e for e in report.errors)

    def test_needs_clarification_is_not_ok(self):
        plan = _plan([], needs_clarification=True,
                     clarification="No tool can delete decisions.")
        report = check_plan(plan, _registry())
        assert not report.ok and any("clarification" in e for e in report.errors)


class TestApprovalAndCategory:
    def test_requires_approval_is_surfaced(self):
        class A(BaseModel):
            to: str = Field(description="x")
        r = ToolRegistry()
        r.register(ToolSpec(name="notify_exec", description="notify an exec",
                            args_schema=A, category=NOTIFY, execute=lambda to: None,
                            read_only=False, requires_approval=True))
        plan = _plan([PlanStep(step_number=1, tool_name="notify_exec",
                               args={"to": "ceo@x.com"}, rationale="x")])
        report = check_plan(plan, r)
        assert report.ok and report.requires_approval is True

    def test_disallowed_category_fails_safe(self):
        class A(BaseModel):
            x: int = Field(description="x")
        r = ToolRegistry()
        r.register(ToolSpec(name="wipe", description="destructive write",
                            args_schema=A, category="WRITE", execute=lambda x: None,
                            read_only=False))
        plan = _plan([PlanStep(step_number=1, tool_name="wipe",
                               args={"x": 1}, rationale="x")])
        report = check_plan(plan, r)
        assert not report.ok and any("not permitted" in e for e in report.errors)
