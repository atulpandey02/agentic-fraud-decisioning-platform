# =============================================================
# BI DASHBOARD CONFIGURATION — Phase 7
# =============================================================
import os

# Connection facade over the one typed settings object (the importlib
# config-layering is gone). The BI SNOWFLAKE role is resolved
# specially below — it must be BI_ROLE, never the agent/admin role.
from fraud_platform.settings import get_settings

_s = get_settings()

GROQ_API_KEY = _s.llm.groq_api_key
SNOWFLAKE_ACCOUNT = _s.snowflake.account
SNOWFLAKE_USER = _s.snowflake.user
SNOWFLAKE_PASSWORD = _s.snowflake.password
SNOWFLAKE_DATABASE = _s.snowflake.database
SNOWFLAKE_WAREHOUSE = _s.snowflake.warehouse

# -------------------------------------------------------------
# NL2SQL GUARDRAILS
#
# The BI agent generates SQL from natural language — the single
# most injection-shaped thing an LLM can do. Defense in depth,
# every layer enforced in CODE after generation (never by asking
# the model nicely):
#   1. Table allowlist — only the two tables BI_ROLE was designed
#      to read (rbac.sql, Day 1). RAW (the PII schema) is not in
#      the list, so no generated query can name it and pass.
#   2. SELECT/CTE/set-op only — enforced by parsing to an AST and
#      permitting only read-shaped top-level nodes, not by matching
#      keywords (see sql_guard.py for why the regex approach was
#      replaced).
#   3. Row cap — the configured LIMIT is enforced even when the
#      model supplies a larger one, so a "show me everything"
#      question can't pull a million rows into a Streamlit table.
# -------------------------------------------------------------
BI_ALLOWED_TABLES = [
    "DECISIONS.FACT_DECISIONS",
    "FEATURES.FACT_FEATURE_SNAPSHOTS",
]
BI_MAX_ROWS = 200
BI_QUERY_TIMEOUT_SECONDS = 30

# -------------------------------------------------------------
# BI SNOWFLAKE ROLE — least privilege, enforced at startup
#
# The BI application connects as BI_ROLE, never the shared
# ACCOUNTADMIN default the rest of the codebase still carries.
# BI_ROLE's rbac.sql grants are SELECT on DECISIONS + FEATURES
# only — no RAW, no DIM, no writes. Two things make this real,
# both enforced in db.open_bi_connection():
#   1. The resolved role is refused if it is any admin role — a
#      misconfigured BI_SNOWFLAKE_ROLE=ACCOUNTADMIN must fail
#      loudly, not silently hand the BI surface superuser rights.
#   2. Secondary roles are disabled at session start. VERIFIED
#      the hard way during Priority 1: connecting as BI_ROLE with
#      the default secondary-roles=ALL still exposed RAW, because
#      the operator's own ACCOUNTADMIN remained active as a
#      secondary role. Primary-role selection alone is NOT a
#      security boundary on this account.
# -------------------------------------------------------------
BI_SNOWFLAKE_ROLE = os.getenv("BI_SNOWFLAKE_ROLE", "BI_ROLE")
BI_FORBIDDEN_ROLES = {"ACCOUNTADMIN", "ORGADMIN", "SECURITYADMIN", "SYSADMIN"}

# The dashboard reuses the agents' Groq model — NL2SQL is a
# text-to-text task the 70B model handles well; introducing a
# second model family here would double the "model deprecated"
# maintenance surface for no quality gain at this scale.
BI_LLM_MODEL = os.getenv("BI_LLM_MODEL", os.getenv("AGENT_LLM_MODEL", "llama-3.3-70b-versatile"))
BI_LLM_TEMPERATURE = 0.0   # SQL generation wants determinism; there are
                           # no tool calls in this path, so the Phase 3
                           # zero-temperature caveat doesn't apply
