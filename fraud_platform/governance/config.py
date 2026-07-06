# =============================================================
# GOVERNANCE CONFIGURATION — Phase 5
# =============================================================
# Connection facade over the one typed settings object (the importlib
# config-layering is gone with the package refactor). Governance
# writes decisions under AGENT_ROLE.
from fraud_platform.settings import get_settings

_s = get_settings()

SNOWFLAKE_ACCOUNT = _s.snowflake.account
SNOWFLAKE_USER = _s.snowflake.user
SNOWFLAKE_PASSWORD = _s.snowflake.password
SNOWFLAKE_DATABASE = _s.snowflake.database
SNOWFLAKE_WAREHOUSE = _s.snowflake.warehouse
SNOWFLAKE_ROLE = _s.agent_connect_role()

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
