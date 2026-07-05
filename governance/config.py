# =============================================================
# GOVERNANCE CONFIGURATION — Phase 5
# =============================================================
import os
import importlib.util

# -------------------------------------------------------------
# CONFIG LAYERING — same mechanism as Phase 4's config.py, one
# layer further out: load Phase 4's config by FILE PATH under a
# private module name and lift its UPPERCASE constants (which
# already include everything from Phase 3, transitively). Each
# phase's config is a superset of the phases beneath it — the
# price of the one-config-py-per-phase convention, paid once per
# phase in these ~15 lines instead of everywhere as copy-drift.
# -------------------------------------------------------------
_PHASE4_CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "agents", "multi_agent", "config.py")
)


def _load_config_by_path(path: str, unique_name: str):
    spec = importlib.util.spec_from_file_location(unique_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_phase4 = _load_config_by_path(_PHASE4_CONFIG_PATH, "_multi_agent_config")
globals().update({k: v for k, v in vars(_phase4).items() if k.isupper()})

# -------------------------------------------------------------
# GOVERNANCE TIERS
# The three tiers FACT_DECISIONS' governance_tier CHECK constraint
# was designed with on Day 1. Semantics, from most to least
# autonomous:
#
#   AUTO_APPROVE — the agent's decision executes with no human
#       involvement and no notification. Reserved for the routine,
#       low-stakes safe case.
#   NOTIFY_ONLY  — the decision executes autonomously but is
#       surfaced in a review feed for after-the-fact visibility.
#       For actions that are correct-by-evidence but customer-
#       impacting or high-value.
#   SUGGEST      — the decision is a SUGGESTION: held in the
#       review queue, nothing executes until a human confirms or
#       overrides. For ambiguity and for everything the agent
#       itself escalated.
# -------------------------------------------------------------
TIER_AUTO_APPROVE = "AUTO_APPROVE"
TIER_NOTIFY_ONLY = "NOTIFY_ONLY"
TIER_SUGGEST = "SUGGEST"

# -------------------------------------------------------------
# TIER THRESHOLDS
# -------------------------------------------------------------
GOVERNANCE_CONFIDENCE_FLOOR = 0.75   # below this, ANY decision is only a
                                     # suggestion — the agent's own stated
                                     # uncertainty is the cheapest, most
                                     # honest routing signal we have
AUTO_ALLOW_MAX_AMOUNT = 500.0        # ALLOW above this executes but is
                                     # surfaced (NOTIFY_ONLY) — a wrong
                                     # high-value allow is the one error
                                     # that cannot be clawed back, so
                                     # value caps how silent automation
                                     # is permitted to be

# -------------------------------------------------------------
# HUMAN REVIEW OUTCOMES — must match FACT_DECISIONS' CHECK
# constraint exactly (CONFIRMED / OVERRIDDEN / ESCALATED)
# -------------------------------------------------------------
REVIEW_OUTCOMES = ["CONFIRMED", "OVERRIDDEN", "ESCALATED"]
