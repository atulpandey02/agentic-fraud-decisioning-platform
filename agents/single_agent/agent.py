# =============================================================
# FRAUD DECISIONING AGENT — explicit ReAct StateGraph
# =============================================================
# Built with StateGraph directly rather than LangGraph's
# create_react_agent prebuilt helper. create_react_agent would
# build the exact same call_model <-> tools loop in one line —
# using it here would hide the mechanics we specifically want
# visible: how the conditional routing works, how the loop
# terminates, how MAX_REASONING_STEPS prevents a runaway agent.
#
# The graph:
#   START -> agent -> (has tool calls?) -> tools -> agent  [loop]
#                   -> (no tool calls)   -> finalize -> END
# =============================================================

import logging
from typing import Literal

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
from pydantic import BaseModel, Field

import config
from state import AgentState
from tools import ALL_TOOLS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are a fraud decisioning agent for a real-time transaction platform.

For every transaction you evaluate, you have three tools available:
- get_transaction_features: live signals for this user right now (velocity, geo-distance, amount z-score, new device, pre-computed risk score)
- get_policy_context: search the fraud policy knowledge base for guidance
- get_user_history: this user's baseline profile, for context on borderline cases

You do not need to use every tool for every transaction. If the current
features already make a case clear-cut (for example, a hard-rule signal
already fully saturated), you can reason to a decision with fewer tool
calls. Reserve get_user_history for genuinely borderline cases.

Always call get_policy_context at least once before deciding — your
decision must be grounded in the platform's actual documented policy,
not general fraud knowledge. Cite what the policy actually said in
your reasoning.

Your final decision must be one of: ALLOW, BLOCK, ESCALATE.
Base your confidence score on how directly the evidence and policy
support your decision — a hard-rule policy match deserves high
confidence; a judgment call on ambiguous, moderate signals deserves
lower confidence and should usually lead to ESCALATE rather than a
confident ALLOW or BLOCK.
"""


class DecisionOutput(BaseModel):
    """
    Structured final output — produced by a dedicated finalize
    call once the ReAct loop stops requesting tools, rather than
    parsed out of free text. Free-text parsing of an LLM's natural
    final answer is fragile; asking the same model to restate its
    conclusion in this exact schema is far more reliable.
    """
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
        description="Human-readable explanation citing the specific feature values and policy guidance retrieved."
    )


class FraudDecisioningAgent:
    """
    Wraps the compiled StateGraph. One instance per process —
    the LLM client and tool-bound graph are built once, then
    reused across many transaction evaluations.
    """

    def __init__(self):
        if not config.GROQ_API_KEY:
            raise RuntimeError(
                "GROQ_API_KEY not set. Add it to your .env file before running the agent."
            )

        self._llm = ChatGroq(
            model=config.LLM_MODEL_NAME,
            temperature=config.LLM_TEMPERATURE,
            api_key=config.GROQ_API_KEY,
        )
        self._llm_with_tools = self._llm.bind_tools(ALL_TOOLS)
        self._llm_structured = self._llm.with_structured_output(DecisionOutput)

        self._graph = self._build_graph()

    # ----------------------------------------------------------
    # NODES
    # ----------------------------------------------------------
    def _call_model(self, state: AgentState) -> dict:
        """
        The 'reason' half of ReAct. Invokes the LLM with the
        current message history — which includes every prior
        tool result — so each call genuinely observes what the
        previous tool calls returned before deciding what to do
        next.
        """
        response = self._llm_with_tools.invoke(state["messages"])
        return {"messages": [response]}

    def _finalize(self, state: AgentState) -> dict:
        """
        Runs once, after the ReAct loop stops requesting tools.
        Asks the SAME conversation history to be restated as a
        structured DecisionOutput, rather than trying to parse
        the loop's last free-text message directly.
        """
        result: DecisionOutput = self._llm_structured.invoke(
            state["messages"] + [
                HumanMessage(content=(
                    "Based on everything above, provide your final structured decision."
                ))
            ]
        )
        return {
            "decision": result.decision,
            "confidence_score": result.confidence_score,
            "identified_pattern": result.identified_pattern,
            "reasoning_text": result.reasoning_text,
        }

    # ----------------------------------------------------------
    # ROUTING
    # ----------------------------------------------------------
    def _should_continue(self, state: AgentState) -> str:
        """
        The conditional edge deciding whether to loop back for
        another tool call or move to finalize.

        MAX_REASONING_STEPS is a hard safety cap — counts AI
        messages so far and forces "finalize" once the cap is
        hit, even if the LLM still wants to call more tools.
        Without this, a confused or looping agent could run
        indefinitely, burning tokens on every retry.
        """
        ai_message_count = sum(1 for m in state["messages"] if isinstance(m, AIMessage))
        if ai_message_count >= config.MAX_REASONING_STEPS:
            logger.warning(
                f"Hit MAX_REASONING_STEPS ({config.MAX_REASONING_STEPS}) — "
                f"forcing finalize regardless of pending tool calls"
            )
            return "finalize"

        last_message = state["messages"][-1]
        if isinstance(last_message, AIMessage) and last_message.tool_calls:
            return "tools"
        return "finalize"

    # ----------------------------------------------------------
    # GRAPH CONSTRUCTION
    # ----------------------------------------------------------
    def _build_graph(self):
        graph = StateGraph(AgentState)

        graph.add_node("agent", self._call_model)
        graph.add_node("tools", ToolNode(ALL_TOOLS))
        graph.add_node("finalize", self._finalize)

        graph.add_edge(START, "agent")
        graph.add_conditional_edges(
            "agent",
            self._should_continue,
            {"tools": "tools", "finalize": "finalize"},
        )
        graph.add_edge("tools", "agent")
        graph.add_edge("finalize", END)

        # MemorySaver — same honest limitation noted in the DQ
        # Monitor project: this is RAM-based and ephemeral, lost
        # on process restart. It's enough to let LangGraph track
        # state across the graph's own execution; it is NOT the
        # durable audit trail. FACT_AGENT_TRACES in Snowflake is
        # what actually persists the reasoning trace long-term —
        # writing to it is the next piece to build, not yet done.
        checkpointer = MemorySaver()
        return graph.compile(checkpointer=checkpointer)

    # ----------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------
    def evaluate(self, transaction: dict, thread_id: str) -> AgentState:
        """
        Run the full ReAct loop for one transaction.

        thread_id is LangGraph's identifier for this specific
        execution's checkpointed state — using the transaction_id
        keeps each transaction's reasoning trace independent.
        """
        initial_state: AgentState = {
            "messages": [
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(content=(
                    f"Evaluate this transaction:\n"
                    f"transaction_id: {transaction['transaction_id']}\n"
                    f"user_id: {transaction['user_id']}\n"
                    f"amount: ${transaction['amount']}\n"
                    f"merchant_category: {transaction['merchant_category']}\n"
                    f"location: {transaction['city']}, {transaction['country']}\n"
                    f"pre-computed risk_score: {transaction['risk_score_raw']}\n"
                    f"is_flagged_for_review: {transaction['is_flagged_for_review']}\n"
                    f"is_new_device: {transaction['is_new_device']}\n"
                    f"geo_distance_km: {transaction.get('geo_distance_km')}\n"
                    f"time_since_last_txn_min: {transaction.get('time_since_last_txn_min')}"
                    f" (negative or missing means out-of-order state — the geo distance above is then unreliable)\n"
                    f"amount_zscore: {transaction.get('amount_zscore')}\n"
                    f"velocity_15min: {transaction.get('velocity_15min')}\n"
                ))
            ],
            "transaction_id": transaction["transaction_id"],
            "user_id": transaction["user_id"],
            "amount": transaction["amount"],
            "merchant_category": transaction["merchant_category"],
            "city": transaction["city"],
            "country": transaction["country"],
            "risk_score_raw": transaction["risk_score_raw"],
            "is_flagged_for_review": transaction["is_flagged_for_review"],
            "is_new_device": transaction["is_new_device"],
            "geo_distance_km": transaction.get("geo_distance_km"),
            "time_since_last_txn_min": transaction.get("time_since_last_txn_min"),
            "amount_zscore": transaction.get("amount_zscore"),
            "velocity_15min": transaction.get("velocity_15min"),
            "decision": None,
            "confidence_score": None,
            "reasoning_text": None,
            "identified_pattern": None,
        }

        config_dict = {"configurable": {"thread_id": thread_id}}
        final_state = self._graph.invoke(initial_state, config=config_dict)
        return final_state