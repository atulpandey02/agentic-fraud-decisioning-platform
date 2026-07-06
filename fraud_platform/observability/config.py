# =============================================================
# OBSERVABILITY CONFIGURATION — Phase 6
# =============================================================
import os

# Connection facade over the one typed settings object (the importlib
# config-layering is gone). Eval reads FEATURES + writes DECISIONS
# under AGENT_ROLE, and needs the Groq key for the LLM judge.
from fraud_platform.settings import get_settings

_s = get_settings()

GROQ_API_KEY = _s.llm.groq_api_key
SNOWFLAKE_ACCOUNT = _s.snowflake.account
SNOWFLAKE_USER = _s.snowflake.user
SNOWFLAKE_PASSWORD = _s.snowflake.password
SNOWFLAKE_DATABASE = _s.snowflake.database
SNOWFLAKE_WAREHOUSE = _s.snowflake.warehouse
SNOWFLAKE_ROLE = _s.agent_connect_role()

# -------------------------------------------------------------
# LANGSMITH
# Dev-time tracing only — see langsmith_config.py for the
# LangSmith-vs-FACT_AGENT_TRACES division of labor.
# -------------------------------------------------------------
LANGSMITH_API_KEY = os.getenv("LANGSMITH_API_KEY")
LANGSMITH_PROJECT = os.getenv("LANGSMITH_PROJECT", "fraud-decisioning-platform")

# -------------------------------------------------------------
# EVALUATION
# -------------------------------------------------------------
EVAL_SAMPLE_SIZE = 6          # total transactions per eval run — deliberately
                              # small: each one costs ~5 Groq calls plus a
                              # judge call, and the point of THIS phase is a
                              # working measurement pipeline, not a big-N
                              # benchmark. Scale the N once the pipeline is
                              # trusted, not before.
EVAL_STRATIFY = True          # half fraud / half legit. A random draw from
                              # FACT_FEATURE_SNAPSHOTS' flagged rows is ~2/3
                              # legitimate (the hard-rule over-flagging noted
                              # in PROJECT_STATUS.md), so an unstratified
                              # sample would mostly measure false-positive
                              # handling and barely touch true-fraud recall.

# LLM-as-judge reuses the same Groq model as the agents. Honest
# limitation, documented rather than hidden: a same-family judge
# can exhibit self-preference bias. In production you'd use a
# stronger or at least different model family for judging; here
# the judge's value is the PIPELINE (scores + notes persisted per
# decision), which is model-swappable via this one constant.
JUDGE_MODEL_NAME = os.getenv("JUDGE_MODEL", os.getenv("AGENT_LLM_MODEL", "llama-3.3-70b-versatile"))
JUDGE_TEMPERATURE = 0.0       # zero, unlike the agents' 0.1 — the judge makes
                              # no tool calls (the reliability reason agents
                              # avoid zero), and scoring should be maximally
                              # repeatable run to run
