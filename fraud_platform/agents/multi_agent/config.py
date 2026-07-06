# =============================================================
# MULTI-AGENT CONFIGURATION — Phase 4
# =============================================================
# The importlib "config layering" that used to load Phase 3's
# config by file path and re-export its constants is GONE — it only
# ever existed to dodge the per-phase `config` module-name
# collision, which the package structure eliminates. Connection
# values now come from the one typed settings object; this module
# is a thin phase-scoped facade plus the orchestration constants
# that are genuinely Phase 4's.
# -------------------------------------------------------------
from fraud_platform.settings import get_settings

_s = get_settings()

# LLM — the specialists and the router all run on this one client.
GROQ_API_KEY = _s.llm.groq_api_key
LLM_MODEL_NAME = _s.llm.agent_model
LLM_TEMPERATURE = 0.1

# Snowflake — the demo/eval fetches read FEATURES under AGENT_ROLE.
SNOWFLAKE_ACCOUNT = _s.snowflake.account
SNOWFLAKE_USER = _s.snowflake.user
SNOWFLAKE_PASSWORD = _s.snowflake.password
SNOWFLAKE_DATABASE = _s.snowflake.database
SNOWFLAKE_WAREHOUSE = _s.snowflake.warehouse
SNOWFLAKE_ROLE = _s.agent_connect_role()

# -------------------------------------------------------------
# ORCHESTRATION
# -------------------------------------------------------------
ORCHESTRATOR_MAX_STEPS = 6  # hard cap on router turns — same job as
                            # Phase 3's MAX_REASONING_STEPS, one level
                            # up: a confused router must not bounce
                            # between specialists forever. 6 allows
                            # every specialist once (4) plus slack for
                            # a routing correction, nothing more.

# The four specialists the orchestrator can route to. decision_agent
# is deliberately last-resort-and-terminal: routing there ends the
# run, so the router must not reach for it until the evidence the
# other specialists produce actually exists.
SPECIALIST_AGENTS = [
    "feature_agent",
    "risk_agent",
    "policy_agent",
    "decision_agent",
]
