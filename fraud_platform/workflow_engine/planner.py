# =============================================================
# PLANNER — natural language -> an ordered, structured Plan
# =============================================================
# This is PLAN-AND-EXECUTE: the WHOLE plan is produced up front in
# one structured LLM call, then executed without re-planning. Con-
# trast the platform's other two patterns, deliberately:
#   - Single ReAct agent: interleaves think/act, re-deciding each
#     step from tool output (agents/single_agent/agent.py).
#   - Supervisor multi-agent: an LLM ROUTER picks the next
#     specialist dynamically (agents/multi_agent/orchestrator.py).
#   - This planner: decides the entire sequence once, then a dumb
#     executor runs it. Cheaper and fully auditable (you can read
#     the plan before a single step runs), at the cost of not
#     adapting mid-run — the right trade for scripted automations.
#
# The planner may use ONLY the tools the registry injects into its
# prompt. It must set needs_clarification rather than invent a tool
# — but that is a PREFERENCE the model can still violate, so it is
# not the guardrail. The guardrail is feasibility.py, which rejects
# any plan referencing an unregistered tool in CODE.
# =============================================================

from __future__ import annotations

import logging
from typing import Optional

from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from . import config
from .registry import ToolRegistry

logger = logging.getLogger(__name__)


class PlanStep(BaseModel):
    step_number: int = Field(description="1-based position in the plan")
    tool_name: str = Field(description="MUST be one of the listed registry tools")
    args: dict = Field(
        default_factory=dict,
        description="arguments for the tool. For a value not knowable at plan "
                    "time, use a reference string '$step_N.field' (an earlier "
                    "step's result) or '$trigger.field' (the event payload).",
    )
    rationale: str = Field(description="one sentence: why this step, here")
    when: Optional[str] = Field(
        default=None,
        description="optional guard condition, evaluated in CODE (not by you). "
                    "A single comparison '$ref OP value', e.g. '$step_1.count >= 2'. "
                    "The step runs only if it is true; otherwise it is SKIPPED. "
                    "Use this for the 'if ...' part of a request.",
    )


class WorkflowPlan(BaseModel):
    goal: str = Field(description="one sentence restating the automation's goal")
    trigger: Optional[str] = Field(
        default=None,
        description="the event type that fires this workflow (e.g. "
                    "'payment.captured'), or null for on-demand only",
    )
    steps: list[PlanStep] = Field(description="ordered steps; empty if clarification needed")
    needs_clarification: bool = Field(
        default=False,
        description="true if the request cannot be planned with the available "
                    "tools, or is ambiguous, or asks for a capability that does "
                    "not exist (e.g. deleting data)",
    )
    clarification_question: Optional[str] = Field(
        default=None, description="if needs_clarification, the question to ask the user"
    )


PLANNER_SYSTEM_PROMPT = """You are a workflow planner for a fraud-operations
platform. You turn a plain-English automation request into an ORDERED plan of
tool calls, produced entirely up front (plan-and-execute — you do not get to
see results and re-plan).

HARD RULES:
1. You may use ONLY the tools listed in the user message. Never invent a tool
   or a capability. If the request needs a tool that is not listed (for example
   deleting, updating, or exporting data — there are NO such tools), do NOT
   fake it: set needs_clarification=true, leave steps empty, and put the reason
   in clarification_question.
2. For a value you cannot know at plan time, use a reference string:
   '$step_N.field' for an earlier step's result field, or '$trigger.field' for
   a field of the triggering event payload. References may only point at
   EARLIER steps.
3. Prefer the deterministic 'count_recent_decisions' over 'query_decisions'
   whenever the request is a simple countable condition (e.g. "N or more BLOCK
   decisions in H hours"). Reserve 'query_decisions' for open-ended questions.
4. Keep plans minimal — only the steps needed to achieve the goal.
5. When the request is conditional ("... IF the user has 2+ BLOCKs, THEN send
   ..."), do NOT bake the decision into a step's presence. Emit the check as
   its own step, and put a `when` guard on every step that should only run if
   the condition holds — e.g. the notify step gets when='$step_1.count >= 2'.
   `when` is a single comparison evaluated in code; the steps it guards are
   SKIPPED (not run) when it is false. This keeps the IF out of your hands and
   in deterministic code.

Set the trigger field if the request is event-driven ("after every payment
capture ..."), otherwise null."""


class Planner:
    """NL instruction -> WorkflowPlan. The LLM is injectable so tests can
    supply a fake; the real one is built lazily from config."""

    def __init__(self, registry: ToolRegistry, llm=None) -> None:
        self._registry = registry
        self._llm = llm if llm is not None else self._build_llm()

    @staticmethod
    def _build_llm():
        # Structured output via json_schema (consistent with the rest of the
        # platform after the function-calling fix). NOTE: if the planner model
        # is a reasoning model, json_mode + local Pydantic validation — the
        # pattern the LLM-judge uses — is the more robust fallback, because the
        # free-form `args` dict stresses strict schema handling.
        from langchain_groq import ChatGroq
        return ChatGroq(
            model=config.PLANNER_MODEL_NAME,
            temperature=config.PLANNER_TEMPERATURE,
            api_key=config.GROQ_API_KEY,
        ).with_structured_output(WorkflowPlan, method="json_schema")

    def plan(self, instruction: str, trigger_context: Optional[dict] = None) -> WorkflowPlan:
        human = (
            f"Available tools (use ONLY these):\n{self._registry.listing_for_prompt()}\n\n"
            f"Automation request:\n{instruction}"
        )
        if trigger_context:
            human += (
                f"\n\nTriggering event payload (reference its fields as "
                f"$trigger.<field>): {trigger_context}"
            )
        plan: WorkflowPlan = self._llm.invoke([
            SystemMessage(content=PLANNER_SYSTEM_PROMPT),
            HumanMessage(content=human),
        ])
        logger.info("planned goal=%r steps=%d clarify=%s",
                    plan.goal, len(plan.steps), plan.needs_clarification)
        return plan
