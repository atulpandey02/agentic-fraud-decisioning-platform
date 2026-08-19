# =============================================================
# FRAUD ORCHESTRATOR — supervisor-pattern multi-agent graph
# =============================================================
# Phase 4's architecture, and why it isn't just Phase 3 again:
#
#   START -> orchestrator -> (routes to) -> specialist -> back
#   to orchestrator -> ... -> decision_agent -> END
#
# The single agent interleaved evidence-gathering and deciding
# inside one conversation; one model juggled every concern with
# every tool. Here each concern is a SPECIALIST with a focused
# prompt and exactly the tools it needs, and the model-driven
# judgment that remains is ROUTING: which specialist runs next,
# and whether one can be skipped. That split is what makes the
# system's reasoning inspectable per-concern (Phase 6 traces get
# an agent_name per step) and extensible per-concern (a fifth
# specialist is a new node + one line in the router prompt, not
# a rewrite of a monolithic prompt).
#
# Why LLM routing WITH code guardrails, not one or the other:
#   A fixed pipeline (feature -> risk -> policy -> decision, always)
#   needs no LLM router at all — but then clear-cut cases pay for
#   a user-history fetch and an extra LLM call they don't need,
#   and the "orchestrator" is just a for-loop wearing a title.
#   Pure LLM routing, by contrast, can skip the policy grounding
#   or ping-pong between specialists. So: the LLM chooses the
#   ORDER and the SKIPS (the judgment part), while code enforces
#   the INVARIANTS (the correctness part) — each specialist runs
#   at most once, policy must run before a decision, and a hard
#   step cap ends a confused run. Same philosophy as Phase 1's
#   two-tier scoring: judgment where judgment helps, hard rules
#   where a property must always hold.
# =============================================================

import logging
from typing import Literal

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from pydantic import BaseModel, Field

from . import config
from .state import MultiAgentState
from .feature_agent import FeatureAgent
from .risk_agent import RiskAgent
from .policy_agent import PolicyAgent
from .decision_agent import DecisionAgent

# -------------------------------------------------------------
# TOOL REUSE — Phase 4 reuses Phase 3's three tools directly.
#
# The specialists reuse the single agent's tools (same Redis key
# conventions, same DIM queries, same Weaviate search) rather than
# forking them — the tools ARE the platform's data-access layer for
# agents; two copies would drift. This is now a plain package import:
# the sys.path insert/remove that used to make the `tools` module
# findable is gone, because it is reachable by its real package path.
#
# Specialists still receive their tools via constructor injection
# from here (one owner of the cross-area reuse), so every specialist
# file stays free of import wiring and is trivially stubbable in tests.
# -------------------------------------------------------------
from fraud_platform.agents.single_agent.tools import (
    get_transaction_features,
    get_policy_context,
    get_user_history,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


ROUTER_SYSTEM_PROMPT = """You are the ORCHESTRATOR of a fraud decisioning
team. You never analyze transactions yourself — you decide which specialist
acts next, based on what the team has established so far.

Your specialists:
- feature_agent: reads the transaction's computed fraud features and
  reports which signals are elevated. Usually the right first step.
- risk_agent: pulls the user's baseline profile and judges how anomalous
  this transaction is for THIS user. Valuable for borderline cases;
  skippable when the absolute signals are already decisive (e.g. a fully
  saturated hard signal), saving a database fetch and a reasoning step.
- policy_agent: retrieves what the platform's documented fraud policy says
  about the elevated patterns. REQUIRED before any decision — decisions
  must be grounded in documented policy. Most effective after
  feature_agent has identified which patterns are elevated.
- decision_agent: synthesizes all findings into the final decision.
  Routing here ENDS the run — only choose it when the gathered evidence
  is sufficient.

Choose exactly one next specialist from the available list."""


class RouteDecision(BaseModel):
    """
    Structured routing output. rationale is not decorative — it is
    appended to the narrative message log, so the Phase 6 trace of
    a decision shows WHY each handoff happened, not just that it did.
    """
    next_agent: Literal[
        "feature_agent", "risk_agent", "policy_agent", "decision_agent"
    ] = Field(description="The specialist to run next.")
    rationale: str = Field(
        description="One sentence: why this specialist, given what is already known."
    )


class FraudOrchestrator:
    """
    Builds and wraps the compiled supervisor graph. One instance
    per process — LLM client, specialists, and graph built once,
    reused across transactions (same reasoning as Phase 3's
    FraudDecisioningAgent).
    """

    def __init__(self):
        if not config.GROQ_API_KEY:
            raise RuntimeError(
                "GROQ_API_KEY not set. Add it to your .env file before running the orchestrator."
            )

        # One shared ChatGroq client; each agent binds its own
        # structured-output schema onto it. Separate clients per
        # agent would multiply connection pools for zero benefit —
        # the binding, not the client, is what differs per agent.
        self._llm = ChatGroq(
            model=config.LLM_MODEL_NAME,
            temperature=config.LLM_TEMPERATURE,
            api_key=config.GROQ_API_KEY,
        )
        self._router_llm = self._llm.with_structured_output(RouteDecision, method="json_schema")

        self._specialists = {
            "feature_agent": FeatureAgent(self._llm, get_transaction_features),
            "risk_agent": RiskAgent(self._llm, get_user_history),
            "policy_agent": PolicyAgent(self._llm, get_policy_context),
            "decision_agent": DecisionAgent(self._llm),
        }

        self._graph = self._build_graph()

    # ----------------------------------------------------------
    # ROUTER NODE
    # ----------------------------------------------------------
    def _route(self, state: MultiAgentState) -> dict:
        """
        Ask the LLM which specialist runs next, then enforce the
        invariants in code. The guardrails run AFTER the model's
        choice on purpose: the model's pick is a preference, the
        invariants are law, and law wins quietly with a logged
        override rather than by re-prompting and hoping.
        """
        invoked = state["agents_invoked"]
        available = [a for a in config.SPECIALIST_AGENTS if a not in invoked]

        # Hard stops first — no LLM call needed when there is no
        # actual choice left to make.
        if available == ["decision_agent"] or len(invoked) >= config.ORCHESTRATOR_MAX_STEPS - 1:
            if "policy_agent" in available and "policy_agent" not in invoked:
                # Step cap hit with policy still missing: policy is
                # the one specialist a decision cannot go without.
                choice, rationale = "policy_agent", (
                    "Step cap reached — forcing the required policy grounding "
                    "before the decision."
                )
            else:
                choice, rationale = "decision_agent", (
                    "All other specialists have contributed (or the step cap "
                    "was reached) — moving to the final decision."
                )
        else:
            evidence = (
                f"Specialists already run: {invoked or 'none yet'}\n"
                f"Available next: {available}\n\n"
                f"Feature findings: {state.get('feature_findings') or '(not gathered)'}\n"
                f"Elevated patterns: {state.get('elevated_patterns') if state.get('feature_findings') else '(not gathered)'}\n"
                f"Risk assessment: {state.get('risk_assessment') or '(not gathered)'}\n"
                f"Policy guidance: {state.get('policy_guidance') or '(not gathered)'}\n\n"
                f"Transaction basics: ${state['amount']}, velocity_15min="
                f"{state['velocity_15min']}, geo_distance_km={state['geo_distance_km']}, "
                f"time_since_last_txn_min={state.get('time_since_last_txn_min')}, "
                f"amount_zscore={state['amount_zscore']}, is_new_device={state['is_new_device']}."
            )
            route: RouteDecision = self._router_llm.invoke([
                SystemMessage(content=ROUTER_SYSTEM_PROMPT),
                HumanMessage(content=evidence),
            ])
            choice, rationale = route.next_agent, route.rationale

            # Invariant 1: each specialist runs at most once.
            if choice not in available:
                fallback = available[0]
                logger.warning(
                    f"Router chose '{choice}' which already ran — overriding to '{fallback}'"
                )
                choice, rationale = fallback, f"(guardrail override) {rationale}"

            # Invariant 2: no decision without policy grounding —
            # the code-level twin of Phase 3's prompt rule "always
            # call get_policy_context at least once before deciding".
            # There it was an instruction the model could ignore;
            # here it is structurally impossible to violate.
            if choice == "decision_agent" and "policy_agent" not in invoked:
                logger.warning(
                    "Router tried to decide without policy grounding — overriding to policy_agent"
                )
                choice, rationale = "policy_agent", (
                    "(guardrail override) A decision requires policy grounding first."
                )

        logger.info(f"[orchestrator] -> {choice}: {rationale}")
        return {
            "next_agent": choice,
            "messages": [AIMessage(content=f"Routing to {choice}: {rationale}", name="orchestrator")],
        }

    # ----------------------------------------------------------
    # GRAPH CONSTRUCTION
    # ----------------------------------------------------------
    def _build_graph(self):
        graph = StateGraph(MultiAgentState)

        graph.add_node("orchestrator", self._route)
        for name, specialist in self._specialists.items():
            graph.add_node(name, specialist.run)

        graph.add_edge(START, "orchestrator")
        # The conditional edge just reads the router's recorded
        # choice — all routing intelligence lives in _route, so
        # there is exactly one place to debug a bad handoff.
        graph.add_conditional_edges(
            "orchestrator",
            lambda state: state["next_agent"],
            {name: name for name in config.SPECIALIST_AGENTS},
        )
        for name in config.SPECIALIST_AGENTS:
            if name != "decision_agent":
                graph.add_edge(name, "orchestrator")
        graph.add_edge("decision_agent", END)

        # MemorySaver: same honest limitation as Phase 3 — RAM-only,
        # enough for LangGraph's own execution tracking. Durable
        # persistence is FACT_DECISIONS + FACT_AGENT_TRACES, written
        # by the governance/observability layers, not by this graph.
        return graph.compile(checkpointer=MemorySaver())

    # ----------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------
    def evaluate(self, transaction: dict, thread_id: str) -> MultiAgentState:
        """
        Run the full multi-agent flow for one transaction. Takes
        the same transaction dict shape as Phase 3's evaluate() —
        one FACT_FEATURE_SNAPSHOTS row feeds either architecture.
        """
        initial_state: MultiAgentState = {
            "messages": [
                HumanMessage(content=(
                    f"Evaluate transaction {transaction['transaction_id']}: "
                    f"${transaction['amount']} by user {transaction['user_id']} "
                    f"at a {transaction['merchant_category']} merchant in "
                    f"{transaction['city']}, {transaction['country']}."
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
            "feature_findings": None,
            "elevated_patterns": None,
            "risk_assessment": None,
            "is_borderline": None,
            "policy_guidance": None,
            "agents_invoked": [],
            "next_agent": None,
            "decision": None,
            "confidence_score": None,
            "reasoning_text": None,
            "identified_pattern": None,
        }

        config_dict = {"configurable": {"thread_id": thread_id}}
        return self._graph.invoke(initial_state, config=config_dict)
