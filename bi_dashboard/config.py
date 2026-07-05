# =============================================================
# BI DASHBOARD CONFIGURATION — Phase 7
# =============================================================
import os
import importlib.util

# -------------------------------------------------------------
# CONFIG LAYERING — outermost layer of the chain: bi_dashboard ⊃
# observability ⊃ governance ⊃ multi_agent ⊃ single_agent. Same
# file-path importlib mechanism as every layer since Phase 4.
# -------------------------------------------------------------
_PHASE6_CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "observability", "config.py")
)


def _load_config_by_path(path: str, unique_name: str):
    spec = importlib.util.spec_from_file_location(unique_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_phase6 = _load_config_by_path(_PHASE6_CONFIG_PATH, "_observability_config")
globals().update({k: v for k, v in vars(_phase6).items() if k.isupper()})

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
#   2. SELECT/WITH-only — statement must start with SELECT or
#      WITH; semicolons stripped so a second statement can't ride
#      along.
#   3. Row cap — LIMIT appended when the model forgets, so a
#      "show me everything" question can't pull a million rows
#      into a Streamlit table.
# -------------------------------------------------------------
BI_ALLOWED_TABLES = [
    "DECISIONS.FACT_DECISIONS",
    "FEATURES.FACT_FEATURE_SNAPSHOTS",
]
BI_MAX_ROWS = 200
BI_QUERY_TIMEOUT_SECONDS = 30

# The dashboard reuses the agents' Groq model — NL2SQL is a
# text-to-text task the 70B model handles well; introducing a
# second model family here would double the "model deprecated"
# maintenance surface for no quality gain at this scale.
BI_LLM_MODEL = os.getenv("BI_LLM_MODEL", os.getenv("AGENT_LLM_MODEL", "llama-3.3-70b-versatile"))
BI_LLM_TEMPERATURE = 0.0   # SQL generation wants determinism; there are
                           # no tool calls in this path, so the Phase 3
                           # zero-temperature caveat doesn't apply
