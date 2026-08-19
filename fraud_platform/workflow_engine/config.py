# =============================================================
# WORKFLOW ENGINE CONFIG
# =============================================================
# Same conventions as the rest of the platform: values come from
# the one typed settings object (Groq key, model) plus a few
# workflow-specific knobs from env, with sensible defaults. The
# feasibility thresholds here are the deterministic, code-enforced
# guardrails — the same "invariants from code, judgment from
# models" split as governance/policy_framework.py.
# =============================================================

import os
from pathlib import Path

from fraud_platform.settings import get_settings

_s = get_settings()

# -------------------------------------------------------------
# LLM — reuse the platform's Groq config. The planner is
# plan-and-execute (ONE structured call producing the whole plan),
# so it uses the agent model at a low temperature. Overridable so
# the planner can be pinned independently of the decision agents.
# -------------------------------------------------------------
GROQ_API_KEY = _s.llm.groq_api_key
PLANNER_MODEL_NAME = os.getenv(
    "WORKFLOW_PLANNER_MODEL", os.getenv("AGENT_LLM_MODEL", _s.llm.agent_model)
)
PLANNER_TEMPERATURE = 0.1

# -------------------------------------------------------------
# FEASIBILITY GUARDRAILS (deterministic, checked in code)
# -------------------------------------------------------------
MAX_PLAN_STEPS = int(os.getenv("WORKFLOW_MAX_PLAN_STEPS", "8"))

# A plan step's tool must belong to one of these categories. There
# are deliberately NO write tools registered; this allowlist exists
# so that adding a write/destructive tool later FAILS feasibility
# until someone consciously widens the policy — fail safe, not fail
# open (the same lesson as the audited Redis fail-open finding).
ALLOWED_TOOL_CATEGORIES = frozenset({"READ_DATA", "ANALYZE", "NOTIFY"})

# -------------------------------------------------------------
# PERSISTENCE — SQLite, single file, zero infra. The STATE MACHINE
# is real; the store is deliberately lightweight. Production would
# be Postgres/Snowflake (documented gap).
# -------------------------------------------------------------
WORKFLOW_DB_PATH = os.getenv(
    "WORKFLOW_DB_PATH",
    str(Path(__file__).resolve().parent / "workflow_state.db"),
)

# -------------------------------------------------------------
# CONNECTORS — Slack real delivery is a FUTURE feature. If
# SLACK_WEBHOOK_URL is set, the connector ALSO POSTs there; either
# way the outbox row is the demo artifact and is never gated on a
# real webhook. Email is outbox-only (SMTP later).
# -------------------------------------------------------------
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
