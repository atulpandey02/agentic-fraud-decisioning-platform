# =============================================================
# POLICY AGENT — specialist #3: what does documented policy say?
# =============================================================
# The RAG specialist. Phase 3's agent decided for itself what to
# ask the knowledge base and when; here the search strategy is
# code, informed by what the feature agent already found: query
# once PER ELEVATED PATTERN (using the pattern filter Phase 2's
# hybrid_search was built with) instead of one broad, unfocused
# search across all four patterns. When nothing is elevated, one
# general query asks what policy says about flagged-but-
# unremarkable transactions — the "why was this even flagged"
# case deserves policy grounding too, not a shrug.
#
# The LLM's role is condensation, not retrieval: hybrid search
# returns up to 3 chunks per pattern with relevance scores; the
# model's one call distills what is actually APPLICABLE to this
# transaction, with thresholds quoted. Handing the decision agent
# 9 raw chunks would bury the two sentences that matter.
# =============================================================

import logging

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from pydantic import BaseModel, Field

from state import MultiAgentState

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are the POLICY specialist on a fraud decisioning
team. You receive raw excerpts retrieved from the platform's fraud policy
knowledge base. Your only job: distill what the documented policy actually
says that APPLIES to this specific transaction.

Quote thresholds and rules precisely (e.g. "policy flags velocity above 5
transactions per 15 minutes"). If the retrieved policy does not cover the
situation, say so explicitly — do not fill gaps with general fraud
knowledge, because the decision must be grounded in what THIS platform's
policy documents actually say. Do not recommend a final decision."""


class PolicyGuidance(BaseModel):
    guidance: str = Field(
        description=(
            "The applicable policy rules for this transaction, with exact "
            "thresholds quoted and the source pattern named for each rule. "
            "State explicitly if policy does not cover the situation."
        )
    )


class PolicyAgent:
    """
    Deterministic retrieval (one hybrid search per elevated
    pattern) + one condensing LLM call. Tool injected by the
    orchestrator, same as every specialist.
    """

    name = "policy_agent"

    def __init__(self, llm, get_policy_context_tool):
        self._llm = llm.with_structured_output(PolicyGuidance)
        self._get_policy = get_policy_context_tool

    def run(self, state: MultiAgentState) -> dict:
        elevated = state.get("elevated_patterns") or []

        excerpts = []
        if elevated:
            for pattern in elevated:
                chunk = self._get_policy.invoke({
                    "question": (
                        f"What are the detection thresholds and recommended "
                        f"handling for {pattern}?"
                    ),
                    "fraud_pattern": pattern,
                })
                excerpts.append(f"=== Retrieved for {pattern} ===\n{chunk}")
        else:
            chunk = self._get_policy.invoke({
                "question": (
                    "How should a transaction be handled when it was flagged "
                    "for review but no individual fraud signal is strongly elevated?"
                ),
            })
            excerpts.append(f"=== Retrieved (no specific pattern elevated) ===\n{chunk}")

        guidance: PolicyGuidance = self._llm.invoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=(
                f"Transaction: ${state['amount']}, velocity_15min="
                f"{state['velocity_15min']}, geo_distance_km={state['geo_distance_km']}, "
                f"amount_zscore={state['amount_zscore']}, is_new_device={state['is_new_device']}.\n"
                f"Elevated patterns: {elevated or 'none'}\n\n"
                f"Retrieved policy excerpts:\n\n" + "\n\n".join(excerpts)
            )),
        ])

        logger.info(f"[{self.name}] searched {len(excerpts)} pattern scope(s)")

        return {
            "policy_guidance": guidance.guidance,
            "agents_invoked": [self.name],
            "messages": [AIMessage(content=guidance.guidance, name=self.name)],
        }
