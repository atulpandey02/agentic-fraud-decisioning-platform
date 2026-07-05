# =============================================================
# FEATURE AGENT — specialist #1: what signals are elevated?
# =============================================================
# Every specialist in this phase follows the same shape, and the
# shape is a deliberate departure from Phase 3's ReAct loop:
#
#   deterministic tool call(s) first, ONE focused LLM call second.
#
# In the single agent, the LLM decided when and whether to fetch
# features — that autonomy is what a ReAct loop buys, and what
# it costs is an extra reasoning round-trip per tool call plus
# the risk of the model skipping a fetch it should have made.
# A SPECIALIST has no such choice to make: the feature agent's
# entire job is "look at the features" — running the fetch
# unconditionally in code is strictly more reliable and strictly
# cheaper than asking a model to decide to do the only thing it
# exists to do. The autonomy lives one level up, in the
# orchestrator, which decides whether this specialist runs at all.
# =============================================================

import logging
from typing import List

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from pydantic import BaseModel, Field

from state import MultiAgentState

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are the FEATURE ANALYSIS specialist on a fraud
decisioning team. Your only job: read a transaction's computed fraud
features and report which signals are elevated and which are normal.

The four patterns this platform detects, and their key signals:
- VELOCITY_SPIKE: velocity_15min above 5 transactions
- GEO_JUMP: a SPEED judgment, not a distance judgment — divide
  geo_distance_km by time_since_last_txn_min to get implied speed; above
  900 km/h is physically impossible travel. A large distance over hours or
  days is ordinary travel, not a jump. If time_since_last_txn_min is
  NEGATIVE or missing, the location state is out-of-order and the
  geo_distance_km value is UNRELIABLE — say so explicitly rather than
  treating it as travel evidence.
- NEW_DEVICE: is_new_device true (supporting signal only, weak alone)
- AMOUNT_ANOMALY: amount_zscore beyond ±3, or suspiciously round
  card-testing amounts ($1.00, $5.00) / threshold-evasion amounts ($99.99, $499.99)

Report ONLY what the numbers show. Do not recommend a decision — that is
another specialist's job. If the online feature store has no data for this
user, say so and work from the transaction snapshot alone."""


class FeatureFindings(BaseModel):
    """
    Structured findings — the machine-readable half (elevated_patterns)
    drives the orchestrator's routing and the policy agent's search
    scope; the prose half (summary) feeds the decision agent and the
    audit narrative. Same structured-output-over-free-text reasoning
    as Phase 3's DecisionOutput.
    """
    elevated_patterns: List[str] = Field(
        description=(
            "Which of VELOCITY_SPIKE, GEO_JUMP, NEW_DEVICE, AMOUNT_ANOMALY "
            "show elevated signals in this transaction. Empty list if none do."
        )
    )
    summary: str = Field(
        description=(
            "2-4 sentences: which signals are elevated (with the actual "
            "numbers), which are normal, and any notable interaction between them."
        )
    )


class FeatureAgent:
    """
    Wraps one LLM call + one deterministic feature fetch.

    The tool comes in through the constructor rather than being
    imported here — the orchestrator owns the single bridge import
    of Phase 3's tools (see orchestrator.py for why that bridge
    exists at all), and hands each specialist exactly the tools it
    needs. That keeps every specialist file free of cross-phase
    import mechanics and makes each one trivially testable with a
    stub tool.
    """

    name = "feature_agent"

    def __init__(self, llm, get_features_tool):
        self._llm = llm.with_structured_output(FeatureFindings)
        self._get_features = get_features_tool

    def run(self, state: MultiAgentState) -> dict:
        # Deterministic fetch — .invoke() on the LangChain tool object
        # gives us the same formatted string the Phase 3 agent saw,
        # including the graceful "no feature data found" message when
        # the online store's 24h TTL has expired this user's entry.
        online_features = self._get_features.invoke({"user_id": state["user_id"]})

        findings: FeatureFindings = self._llm.invoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=(
                f"Transaction snapshot (from the feature pipeline):\n"
                f"amount: ${state['amount']}\n"
                f"velocity_15min: {state['velocity_15min']}\n"
                f"geo_distance_km: {state['geo_distance_km']}\n"
                f"time_since_last_txn_min: {state.get('time_since_last_txn_min')}\n"
                f"amount_zscore: {state['amount_zscore']}\n"
                f"is_new_device: {state['is_new_device']}\n"
                f"pre-computed risk_score: {state['risk_score_raw']}\n\n"
                f"Current online feature store state for this user:\n"
                f"{online_features}"
            )),
        ])

        logger.info(f"[{self.name}] elevated: {findings.elevated_patterns or 'none'}")

        return {
            "feature_findings": findings.summary,
            "elevated_patterns": findings.elevated_patterns,
            "agents_invoked": [self.name],
            "messages": [AIMessage(content=findings.summary, name=self.name)],
        }
