# =============================================================
# SINGLE AGENT CONFIGURATION — Phase 3
# =============================================================
import os

from dotenv import load_dotenv

# Same pattern as feature_engine.py and velocity_engine.py: every
# credential below comes from the repo-root .env, and os.getenv()
# only sees it if something actually loads that file first. This
# was missing when Phase 3 was first scaffolded — the agent died
# at construction with "GROQ_API_KEY not set" even though the key
# sat in .env the whole time. load_dotenv() with no arguments
# walks UP the directory tree from this file until it finds .env,
# so it resolves the repo root correctly regardless of which
# folder the process was launched from.
load_dotenv()

# -------------------------------------------------------------
# LLM
# Groq llama-3.3-70b-versatile — same model already proven
# working in the earlier Agentic DQ Monitor project (both the
# reasoning agent and the LLM-as-judge used Groq models there).
# If this model ID has been deprecated by the time you run this,
# check Groq's current model list — model availability on
# inference providers churns faster than most other parts of
# this stack.
# -------------------------------------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
LLM_MODEL_NAME = os.getenv("AGENT_LLM_MODEL", "llama-3.3-70b-versatile")
LLM_TEMPERATURE = 0.1   # low, not zero — fraud reasoning should be
                         # consistent, but zero temperature on some
                         # providers degrades tool-calling reliability

# -------------------------------------------------------------
# REDIS — reading, not writing
# Same connection settings as feature_engine.py, but this agent
# only ever calls GET, never SETEX — it is a pure consumer of
# the online feature store, not a participant in the pipeline
# that populates it.
# -------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# -------------------------------------------------------------
# SNOWFLAKE — reading DIM only
# Deliberately scoped to DIM.DIM_USERS and DIM.DIM_DEVICES —
# the agent's AGENT_ROLE (designed Day 1, not yet applied) has
# no grant on RAW at all. Even running as ACCOUNTADMIN during
# development, the queries this agent issues never touch RAW,
# as a matter of code discipline matching the intended production
# access boundary.
# -------------------------------------------------------------
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FRAUD_DETECTION")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

# -------------------------------------------------------------
# WEAVIATE — reused directly from Phase 2, same values
# -------------------------------------------------------------
WEAVIATE_HOST = os.getenv("WEAVIATE_HOST", "localhost")
WEAVIATE_HTTP_PORT = int(os.getenv("WEAVIATE_HTTP_PORT", 8090))
WEAVIATE_GRPC_PORT = int(os.getenv("WEAVIATE_GRPC_PORT", 50051))

# -------------------------------------------------------------
# AGENT BEHAVIOR
# -------------------------------------------------------------
MAX_REASONING_STEPS = 8   # safety cap on the ReAct loop — if the
                           # agent hasn't reached a decision after
                           # 8 tool-call/reasoning cycles, force a
                           # stop rather than loop indefinitely
                           # and burn tokens on a stuck agent