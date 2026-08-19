# =============================================================
# UNIT TESTS — planner (mock LLM; real Groq behind a flag)
# =============================================================
# The planner's job is prompt assembly + returning a structured
# WorkflowPlan. We test it with an injected fake LLM so no Groq call
# happens. A real end-to-end Groq plan is exercised only when
# RUN_LLM_TESTS=1 (and a key is present) — kept out of the default
# suite so CI stays credential-free.
# =============================================================

import os

import pytest

from fraud_platform.workflow_engine.planner import PlanStep, Planner, WorkflowPlan
from fraud_platform.workflow_engine.tools_bridge import build_default_registry


class _FakeLLM:
    """Captures the messages it was invoked with and returns a canned plan."""

    def __init__(self, plan: WorkflowPlan):
        self._plan = plan
        self.last_messages = None

    def invoke(self, messages):
        self.last_messages = messages
        return self._plan


def _registry():
    return build_default_registry(lambda c, p: None)


class TestPlannerWithMock:
    def test_returns_structured_plan_and_injects_tool_listing(self):
        canned = WorkflowPlan(
            goal="notify on repeat blocks", trigger="payment.captured",
            steps=[PlanStep(step_number=1, tool_name="count_recent_decisions",
                            args={"user_id": "$trigger.user_id", "decision": "BLOCK",
                                  "hours": 24}, rationale="count")],
        )
        fake = _FakeLLM(canned)
        planner = Planner(_registry(), llm=fake)
        plan = planner.plan("After every payment capture, count the user's BLOCKs.",
                            trigger_context={"user_id": "u1"})
        assert plan.goal == "notify on repeat blocks"
        assert plan.steps[0].tool_name == "count_recent_decisions"
        # the registry listing (every tool name) must be injected into the prompt
        human_text = fake.last_messages[-1].content
        for name in _registry().names():
            assert name in human_text
        # the trigger payload was passed through so the model can $trigger.ref it
        assert "$trigger" in human_text and "u1" in human_text

    def test_clarification_plan_passes_through(self):
        canned = WorkflowPlan(goal="delete data", steps=[], needs_clarification=True,
                              clarification_question="No tool can delete decisions.")
        planner = Planner(_registry(), llm=_FakeLLM(canned))
        plan = planner.plan("Delete all BLOCK decisions from last week.")
        assert plan.needs_clarification and not plan.steps


@pytest.mark.skipif(
    os.getenv("RUN_LLM_TESTS") != "1" or not os.getenv("GROQ_API_KEY"),
    reason="live Groq plan — set RUN_LLM_TESTS=1 with a GROQ_API_KEY to run",
)
def test_real_groq_plan_smoke():
    planner = Planner(_registry())
    plan = planner.plan(
        "After every payment capture, if the user has 2 or more BLOCK decisions "
        "in the last 24 hours, send a Slack message with their recent history."
    )
    assert isinstance(plan, WorkflowPlan)
    # should route through the deterministic counter, not open-ended query
    assert any(s.tool_name == "count_recent_decisions" for s in plan.steps) or plan.needs_clarification
