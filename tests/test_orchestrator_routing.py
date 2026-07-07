# =============================================================
# INTEGRATION TESTS (mocked LLM) — orchestrator routing guardrails
# =============================================================
# Priority 4 item 2: the router's LLM is replaced with a scripted
# fake so the CODE-ENFORCED invariants are tested deterministically,
# with no Groq call and no credentials. We invoke FraudOrchestrator's
# _route directly on a stand-in `self` carrying only a fake router —
# _route uses nothing else — so the whole ChatGroq/specialist
# construction (which needs an API key) is bypassed.
#
# The invariants under test are the WHOLE POINT of the supervisor
# pattern: the LLM proposes, code disposes.
# =============================================================

from types import SimpleNamespace

from fraud_platform.agents.multi_agent.orchestrator import FraudOrchestrator, RouteDecision


class ScriptedRouter:
    """Returns pre-scripted RouteDecisions; records how often it was
    asked (to prove the code short-circuits the LLM when there's no
    real choice to make)."""
    def __init__(self, *choices):
        self._choices = list(choices)
        self.calls = 0

    def invoke(self, _messages):
        self.calls += 1
        name = self._choices.pop(0)
        return RouteDecision(next_agent=name, rationale="scripted")


def _state(invoked, **over):
    s = {
        "agents_invoked": invoked,
        "feature_findings": None, "elevated_patterns": None,
        "risk_assessment": None, "policy_guidance": None,
        "amount": 100.0, "velocity_15min": 1, "geo_distance_km": 10.0,
        "time_since_last_txn_min": 30.0, "amount_zscore": 0.5, "is_new_device": False,
    }
    s.update(over)
    return s


def _route(router, state):
    stand_in = SimpleNamespace(_router_llm=router)
    return FraudOrchestrator._route(stand_in, state)


class TestRoutingInvariants:
    def test_normal_first_hop_uses_llm_choice(self):
        router = ScriptedRouter("feature_agent")
        out = _route(router, _state([]))
        assert out["next_agent"] == "feature_agent"
        assert router.calls == 1

    def test_decision_before_policy_is_overridden_to_policy(self):
        # the load-bearing rule: no decision without policy grounding.
        # LLM says decision_agent, but policy hasn't run -> forced to policy.
        router = ScriptedRouter("decision_agent")
        out = _route(router, _state(["feature_agent"], feature_findings="done"))
        assert out["next_agent"] == "policy_agent"

    def test_decision_allowed_once_policy_has_run(self):
        router = ScriptedRouter("decision_agent")
        out = _route(router, _state(
            ["feature_agent", "policy_agent"],
            feature_findings="done", policy_guidance="grounded",
        ))
        assert out["next_agent"] == "decision_agent"

    def test_specialist_not_repeated(self):
        # LLM picks an already-run specialist -> guardrail overrides to
        # an available one
        router = ScriptedRouter("feature_agent")
        out = _route(router, _state(["feature_agent"], feature_findings="done"))
        assert out["next_agent"] != "feature_agent"

    def test_only_decision_left_short_circuits_without_llm(self):
        # everyone but decision_agent has run -> no LLM call needed
        router = ScriptedRouter()  # empty: will IndexError if invoked
        out = _route(router, _state(
            ["feature_agent", "risk_agent", "policy_agent"],
            feature_findings="d", policy_guidance="g", risk_assessment="r",
        ))
        assert out["next_agent"] == "decision_agent"
        assert router.calls == 0

    def test_step_cap_with_policy_missing_forces_policy(self):
        # at the step cap (len(invoked) >= MAX_STEPS-1) with policy still
        # missing, policy is forced — a decision cannot go without it,
        # and the LLM is NOT consulted
        router = ScriptedRouter()  # empty: IndexError if invoked
        out = _route(router, _state(["a", "b", "c", "d", "e"], feature_findings="d"))
        assert out["next_agent"] == "policy_agent"
        assert router.calls == 0

    def test_step_cap_with_policy_done_forces_decision(self):
        router = ScriptedRouter()  # empty: IndexError if invoked
        out = _route(router, _state(
            ["policy_agent", "b", "c", "d", "e"], policy_guidance="g"
        ))
        assert out["next_agent"] == "decision_agent"
        assert router.calls == 0
