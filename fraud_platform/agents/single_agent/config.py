# =============================================================
# SINGLE AGENT CONFIGURATION — Phase 3
# =============================================================
# Connection values now come from the ONE typed settings object
# (fraud_platform.settings), validated once at startup — no more
# scattered os.getenv, no more load_dotenv() in every module. What
# stays here is what is genuinely THIS phase's: the agent's model
# temperature and its ReAct step cap. The names below are unchanged,
# so every `config.SNOWFLAKE_ACCOUNT` / `config.GROQ_API_KEY` call
# site keeps working — this module is a thin, phase-scoped facade
# over the shared settings, not a second source of truth.
# =============================================================

from fraud_platform.settings import get_settings

_s = get_settings()

# -------------------------------------------------------------
# LLM — Groq llama-3.3-70b-versatile (see settings for the model id;
# if it has been deprecated, override AGENT_LLM_MODEL in the env).
# -------------------------------------------------------------
GROQ_API_KEY = _s.llm.groq_api_key
LLM_MODEL_NAME = _s.llm.agent_model
LLM_TEMPERATURE = 0.1   # low, not zero — fraud reasoning should be
                         # consistent, but zero temperature on some
                         # providers degrades tool-calling reliability

# -------------------------------------------------------------
# REDIS — reading, not writing (pure consumer of the online store).
# -------------------------------------------------------------
REDIS_HOST = _s.redis.host
REDIS_PORT = _s.redis.port
REDIS_DB = _s.redis.db

# -------------------------------------------------------------
# SNOWFLAKE — reads DIM only, under the least-privilege AGENT_ROLE.
# The role now comes from settings.agent_connect_role() rather than
# defaulting to ACCOUNTADMIN — the Priority 1 rule ("no admin
# default in an application path") applied to the agent path.
# -------------------------------------------------------------
SNOWFLAKE_ACCOUNT = _s.snowflake.account
SNOWFLAKE_USER = _s.snowflake.user
SNOWFLAKE_PASSWORD = _s.snowflake.password
SNOWFLAKE_DATABASE = _s.snowflake.database
SNOWFLAKE_WAREHOUSE = _s.snowflake.warehouse
SNOWFLAKE_ROLE = _s.agent_connect_role()

# -------------------------------------------------------------
# WEAVIATE — reused directly from Phase 2, same values.
# -------------------------------------------------------------
WEAVIATE_HOST = _s.weaviate.host
WEAVIATE_HTTP_PORT = _s.weaviate.http_port
WEAVIATE_GRPC_PORT = _s.weaviate.grpc_port

# -------------------------------------------------------------
# AGENT BEHAVIOR
# -------------------------------------------------------------
MAX_REASONING_STEPS = 8   # safety cap on the ReAct loop — if the
                           # agent hasn't reached a decision after
                           # 8 tool-call/reasoning cycles, force a
                           # stop rather than loop indefinitely
                           # and burn tokens on a stuck agent
