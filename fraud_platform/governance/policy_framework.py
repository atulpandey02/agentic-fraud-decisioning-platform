# =============================================================
# GOVERNANCE POLICY FRAMEWORK — who gets to act on a decision?
# =============================================================
# The agent decides WHAT should happen (ALLOW/BLOCK/ESCALATE);
# this framework decides HOW MUCH AUTONOMY that decision gets
# (AUTO_APPROVE / NOTIFY_ONLY / SUGGEST). Keeping those two
# judgments separate is the core Phase 5 idea: the agent's job
# is fraud reasoning, and it should not also be the arbiter of
# its own trustworthiness — that boundary belongs to a
# deterministic, auditable rule layer OUTSIDE the LLM.
#
# Why deterministic rules and not another LLM call:
#   Governance is exactly the place where "usually right" is not
#   good enough — a reviewer, a regulator, or tomorrow's debugger
#   must be able to answer "why did this action auto-execute?"
#   with a rule citation, not a probability. Same two-tier
#   philosophy as Phase 1's hard-rule overrides and Phase 4's
#   routing guardrails: judgment from models, invariants from code.
#
# The tier matrix reasons from ASYMMETRIC ERROR COSTS:
#   - A false ALLOW is money gone — unrecoverable. So silence
#     (AUTO_APPROVE) is only permitted for high-confidence allows
#     UNDER a value cap; above the cap the allow still executes
#     but is surfaced (NOTIFY_ONLY).
#   - A false BLOCK is a recoverable inconvenience (customer
#     calls, card unblocks) — safe to execute autonomously, but
#     customer-impacting, so it is always surfaced, never silent.
#   - Human attention is the scarcest resource in the loop. The
#     SUGGEST queue is reserved for where a human's marginal
#     value is real: the agent's own escalations and anything it
#     was not confident about. Queueing everything would bury
#     the reviewer and re-create the alert-fatigue problem fraud
#     teams already drown in.
# =============================================================

import logging
from typing import Tuple

from . import config

logger = logging.getLogger(__name__)


class GovernancePolicyFramework:
    """
    Stateless tier assignment. A class rather than a bare function
    for the same reason as every engine in this codebase: the
    thresholds arrive via constructor (defaulting from config), so
    a test — or a future per-merchant policy — can instantiate a
    stricter framework without monkeypatching module globals.
    """

    def __init__(
        self,
        confidence_floor: float = config.GOVERNANCE_CONFIDENCE_FLOOR,
        auto_allow_max_amount: float = config.AUTO_ALLOW_MAX_AMOUNT,
    ):
        self._confidence_floor = confidence_floor
        self._auto_allow_max_amount = auto_allow_max_amount

    def assign_tier(
        self, decision: str, confidence_score: float, amount: float
    ) -> Tuple[str, str]:
        """
        Map (decision, confidence, amount) -> (tier, rationale).

        Rule order matters and is deliberate: the reasons to WITHHOLD
        autonomy are checked before the reasons to grant it, so a
        case matching both (e.g. a low-confidence small ALLOW) always
        lands on the cautious side. The rationale string is returned
        for logging and the demo narrative — FACT_DECISIONS stores
        only the tier itself; the rationale is reconstructible from
        these rules plus the row's own decision/confidence/amount,
        which is the point of keeping governance deterministic.
        """
        # Rule 1 — the agent asked for a human. Honor it uncondition-
        # ally: overriding an ESCALATE with autonomy would make the
        # agent's most safety-relevant output meaningless.
        if decision == "ESCALATE":
            return config.TIER_SUGGEST, (
                "Agent escalated — its decision is a suggestion for the "
                "human review queue by definition."
            )

        # Rule 2 — the agent acted, but wasn't sure. Its own stated
        # uncertainty is the cheapest honest routing signal available.
        if confidence_score is None or confidence_score < self._confidence_floor:
            return config.TIER_SUGGEST, (
                f"Confidence {confidence_score} below the "
                f"{self._confidence_floor} autonomy floor — held for human review."
            )

        # Rule 3 — confident BLOCK: execute (blocking is the
        # recoverable side of the asymmetry) but never silently,
        # because it is customer-impacting.
        if decision == "BLOCK":
            return config.TIER_NOTIFY_ONLY, (
                "Confident block — executes autonomously (false blocks are "
                "recoverable) but is surfaced, since it impacts a customer."
            )

        # Rule 4 — confident ALLOW: silence is earned only under the
        # value cap; a wrong allow above it is unrecoverable money.
        if amount <= self._auto_allow_max_amount:
            return config.TIER_AUTO_APPROVE, (
                f"Confident allow at ${amount:.2f} (≤ ${self._auto_allow_max_amount:.0f} "
                f"cap) — routine, executes silently."
            )
        return config.TIER_NOTIFY_ONLY, (
            f"Confident allow but ${amount:.2f} exceeds the "
            f"${self._auto_allow_max_amount:.0f} silent-automation cap — "
            f"executes, surfaced for visibility."
        )
