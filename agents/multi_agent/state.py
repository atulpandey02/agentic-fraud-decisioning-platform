# =============================================================
# MULTI-AGENT STATE — shared blackboard for all five agents
# =============================================================
# Same TypedDict-through-StateGraph mechanics as Phase 3, with
# one structural difference that IS the point of Phase 4: in the
# single agent, everything the model learned lived inside one
# conversation (`messages`) and only that model ever read it.
# Here, four specialists each contribute a TYPED finding field
# (feature_findings, risk_assessment, policy_guidance, ...) that
# the orchestrator and the other specialists read directly —
# a blackboard, not a group chat. Passing findings as typed
# fields instead of making every specialist re-parse the full
# message history keeps each specialist's prompt small and
# focused on exactly the evidence it needs.
#
# `messages` still exists, but demoted to a NARRATIVE log: each
# agent appends one AIMessage (tagged with name=agent_name)
# summarizing what it did. That log is what FACT_AGENT_TRACES'
# agent_name column was designed for on Day 1 — the Phase 6
# trace writer persists this list, one row per step, and the
# per-message `name` attribute is what distinguishes a handoff
# trail from a single agent's monologue.
# =============================================================

import operator
from typing import Annotated, Optional, TypedDict

from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


class MultiAgentState(TypedDict):
    """
    The blackboard. Three zones:

    INPUT — the transaction, populated once before the graph
    starts (identical fields to Phase 3's AgentState, so the
    same FACT_FEATURE_SNAPSHOTS row feeds either architecture
    unchanged).

    FINDINGS — one slot per specialist, written exactly once by
    that specialist, None until then. The orchestrator's routing
    prompt is built from which of these are still None — "what
    do we not know yet" is literally the routing question.

    OUTPUT — the final decision, written only by decision_agent.
    Same four fields as Phase 3, deliberately: FACT_DECISIONS
    doesn't care which architecture produced a row.
    """

    messages: Annotated[list[BaseMessage], add_messages]

    # ---- Input: the transaction being evaluated ----
    transaction_id: str
    user_id: str
    amount: float
    merchant_category: str
    city: str
    country: str
    risk_score_raw: float
    is_flagged_for_review: bool
    is_new_device: bool
    geo_distance_km: Optional[float]
    amount_zscore: Optional[float]
    velocity_15min: Optional[int]

    # ---- Findings: one slot per specialist ----
    feature_findings: Optional[str]       # feature_agent: which signals are elevated, and why
    elevated_patterns: Optional[list]     # feature_agent: machine-readable pattern list for routing/policy
    risk_assessment: Optional[str]        # risk_agent: risk read vs this user's own baseline
    is_borderline: Optional[bool]         # risk_agent: drives whether ESCALATE should be preferred
    policy_guidance: Optional[str]        # policy_agent: what documented policy actually says

    # ---- Routing bookkeeping ----
    # operator.add as the reducer means each node APPENDS its own
    # name rather than overwriting the list — same reasoning as
    # add_messages for the message log. The orchestrator reads
    # this to know which specialists already ran (each runs at
    # most once) and to enforce the step cap.
    agents_invoked: Annotated[list, operator.add]
    next_agent: Optional[str]             # the router's choice — read by the conditional edge

    # ---- Output: what decision_agent decides ----
    decision: Optional[str]               # "ALLOW" | "BLOCK" | "ESCALATE"
    confidence_score: Optional[float]     # 0.0 to 1.0
    reasoning_text: Optional[str]         # human-readable synthesis of all findings
    identified_pattern: Optional[str]     # which fraud pattern, if any
