# =============================================================
# RISK AGENT — specialist #2: how bad is this FOR THIS USER?
# =============================================================
# The feature agent reads absolute signals; this agent reads them
# RELATIVE to the specific user's own baseline — the distinction
# Phase 3's system prompt drew with "reserve get_user_history for
# genuinely borderline cases". In this architecture that judgment
# call belongs to the orchestrator: it may skip this specialist
# entirely when the feature findings are already clear-cut, which
# both saves a Snowflake round trip + LLM call AND mirrors how a
# real fraud team works (you don't pull the customer file for an
# obvious card-testing bot).
#
# Same specialist shape as feature_agent.py: deterministic
# history fetch in code, one focused LLM call to interpret it.
# =============================================================

import logging

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from pydantic import BaseModel, Field

from .state import MultiAgentState

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are the RISK ASSESSMENT specialist on a fraud
decisioning team. You receive a transaction's elevated signals plus the
user's baseline profile (typical spend, home location, risk tier, account
age, trusted devices). Your only job: judge how anomalous this transaction
is FOR THIS SPECIFIC USER.

A $900 purchase is alarming for a user who averages $30 and unremarkable
for one who averages $400. A foreign-city transaction means something
different for a HIGH-risk-tier frequent traveler than for a LOW-tier
homebody. That per-user context is exactly what you add — the absolute
signal analysis has already been done by another specialist.

Call the case borderline when the evidence genuinely cuts both ways;
do not manufacture certainty either direction. Do not recommend a
final decision — that is another specialist's job."""


class RiskAssessment(BaseModel):
    """
    is_borderline is the load-bearing field: the decision agent is
    told to prefer ESCALATE over a confident ALLOW/BLOCK when the
    specialist who actually saw the user's baseline flagged the case
    as ambiguous. The prose carries the why.
    """
    assessment: str = Field(
        description=(
            "2-4 sentences comparing this transaction against the user's "
            "own baseline: spend vs their average/stddev, location vs their "
            "home, device vs their trusted device count, weighted by risk tier."
        )
    )
    is_borderline: bool = Field(
        description=(
            "True if the evidence genuinely cuts both ways for this user "
            "and a human should probably look; False if the picture is clear."
        )
    )


class RiskAgent:
    """
    One deterministic user-history fetch + one focused LLM call.
    Tool injected by the orchestrator, same as every specialist.
    """

    name = "risk_agent"

    def __init__(self, llm, get_user_history_tool):
        self._llm = llm.with_structured_output(RiskAssessment, method="json_schema")
        self._get_history = get_user_history_tool

    def run(self, state: MultiAgentState) -> dict:
        user_history = self._get_history.invoke({"user_id": state["user_id"]})

        assessment: RiskAssessment = self._llm.invoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=(
                f"Transaction: ${state['amount']} at a {state['merchant_category']} "
                f"merchant in {state['city']}, {state['country']}.\n\n"
                f"Feature specialist's findings:\n"
                f"{state.get('feature_findings') or '(feature analysis not run yet)'}\n"
                f"Elevated patterns: {state.get('elevated_patterns') or 'none identified'}\n\n"
                f"This user's baseline profile:\n{user_history}"
            )),
        ])

        logger.info(f"[{self.name}] borderline={assessment.is_borderline}")

        return {
            "risk_assessment": assessment.assessment,
            "is_borderline": assessment.is_borderline,
            "agents_invoked": [self.name],
            "messages": [AIMessage(content=assessment.assessment, name=self.name)],
        }
