# =============================================================
# DECISION AGENT — specialist #4: synthesize and decide
# =============================================================
# Terminal specialist — routing here ends the run. It makes NO
# tool calls: by the time the orchestrator routes to it, every
# piece of evidence it may use is already on the blackboard as a
# typed finding. Giving the decider its own tools would reopen
# the door Phase 4 deliberately closed — evidence-gathering and
# deciding are separate concerns handled by separate agents,
# which is the whole argument for this architecture over the
# Phase 3 monolith.
#
# The output schema deliberately mirrors Phase 3's DecisionOutput
# field-for-field rather than importing it — importing would drag
# in single_agent/agent.py's ChatGroq/graph machinery just for a
# 4-field pydantic class, and would couple the two architectures'
# internals. The CONTRACT they share (what a decision row looks
# like) is what matters, and that contract's real home is the
# DECISIONS.FACT_DECISIONS schema both write to.
# =============================================================

import logging

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from pydantic import BaseModel, Field
from typing import Literal

from .state import MultiAgentState

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are the DECISION specialist on a fraud decisioning
team — the last agent to act. Specialists have already gathered evidence:
feature analysis, policy guidance, and possibly a per-user risk assessment.
Your only job: weigh their findings and issue the final decision.

Rules:
- Your decision must be grounded in the policy specialist's guidance —
  cite what policy actually said. If the policy specialist has not
  contributed, that is an orchestration failure: say so and ESCALATE.
- If the risk specialist marked the case borderline, prefer ESCALATE over
  a confident ALLOW or BLOCK.
- If the risk specialist was skipped, you are deciding without per-user
  context — only be confident when the absolute signals plus policy make
  the case clear-cut on their own.
- Confidence reflects how directly the evidence and policy support the
  decision: a saturated hard signal matching an explicit policy threshold
  deserves high confidence; a judgment call on moderate signals deserves
  low confidence and usually ESCALATE."""


class DecisionOutput(BaseModel):
    """Same four fields as Phase 3 — see module docstring for why
    this is a mirrored contract, not a shared import."""
    decision: Literal["ALLOW", "BLOCK", "ESCALATE"] = Field(
        description="The final decision for this transaction."
    )
    confidence_score: float = Field(
        ge=0.0, le=1.0,
        description="0.0 to 1.0 — how directly the evidence and policy support this decision."
    )
    identified_pattern: str = Field(
        description="One of VELOCITY_SPIKE, GEO_JUMP, NEW_DEVICE, AMOUNT_ANOMALY, or NONE if no pattern applies."
    )
    reasoning_text: str = Field(
        description="Human-readable synthesis citing each specialist's findings and the policy guidance."
    )


class DecisionAgent:
    """One synthesis LLM call over the completed blackboard."""

    name = "decision_agent"

    def __init__(self, llm):
        self._llm = llm.with_structured_output(DecisionOutput, method="json_schema")

    def run(self, state: MultiAgentState) -> dict:
        risk_section = (
            f"Risk specialist (per-user baseline):\n{state['risk_assessment']}\n"
            f"Marked borderline: {state['is_borderline']}"
            if state.get("risk_assessment")
            else "Risk specialist: SKIPPED by the orchestrator — no per-user "
                 "baseline context available."
        )

        result: DecisionOutput = self._llm.invoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=(
                f"Transaction {state['transaction_id']}: ${state['amount']} at a "
                f"{state['merchant_category']} merchant in {state['city']}, "
                f"{state['country']} (user {state['user_id']}).\n\n"
                f"Feature specialist:\n"
                f"{state.get('feature_findings') or 'NOT RUN'}\n"
                f"Elevated patterns: {state.get('elevated_patterns') or 'none'}\n\n"
                f"{risk_section}\n\n"
                f"Policy specialist:\n"
                f"{state.get('policy_guidance') or 'NOT RUN'}\n\n"
                f"Provide your final structured decision."
            )),
        ])

        logger.info(
            f"[{self.name}] {result.decision} "
            f"(confidence {result.confidence_score}, pattern {result.identified_pattern})"
        )

        return {
            "decision": result.decision,
            "confidence_score": result.confidence_score,
            "identified_pattern": result.identified_pattern,
            "reasoning_text": result.reasoning_text,
            "agents_invoked": [self.name],
            "messages": [AIMessage(content=result.reasoning_text, name=self.name)],
        }
