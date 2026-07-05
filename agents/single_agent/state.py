# =============================================================
# AGENT STATE — the typed object that flows through the graph
# =============================================================
# In LangGraph, state is a TypedDict threaded through every node.
# Each node reads from it and returns a partial update; LangGraph
# merges updates using the reducer specified per field (Annotated
# with add_messages here, meaning new messages APPEND to the list
# rather than replacing it — this is what makes the ReAct loop's
# conversation history accumulate correctly across cycles).
# =============================================================

from typing import Annotated, Optional, TypedDict
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


class AgentState(TypedDict):
    """
    Everything the agent needs, and everything it produces.

    messages: the ReAct loop's running conversation — system
        prompt, the transaction being evaluated, every tool call
        the LLM makes, every tool result, and the LLM's reasoning
        at each step. This list IS the audit trail — it maps
        directly to FACT_AGENT_TRACES (step_number = position in
        this list, step_type = HumanMessage/AIMessage/ToolMessage
        type, tool_name/tool_input/tool_output = the tool call
        details LangGraph already captures on AIMessage.tool_calls).

    The transaction fields below are the INPUT — populated once,
    before the graph starts, from a flagged transaction's Redis
    feature snapshot. The agent's tools then pull ADDITIONAL
    context (policy docs, user history) during its own reasoning;
    that retrieved context lives in the messages list as tool
    results, not as separate state fields, since it's transient
    reasoning context rather than a fixed input.

    The decision fields below are the OUTPUT — populated once,
    at the end, when the agent reaches a final answer.
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

    # ---- Output: what the agent decides ----
    decision: Optional[str]              # "ALLOW" | "BLOCK" | "ESCALATE"
    confidence_score: Optional[float]    # 0.0 to 1.0
    reasoning_text: Optional[str]        # human-readable explanation
    identified_pattern: Optional[str]    # which fraud pattern, if any