# =============================================================
# AGENT TOOLS — get_features, get_policy_context, get_user_history
# =============================================================
# Each tool wraps a LIGHTWEIGHT reader class, deliberately
# separate from feature_engine.py's RedisFeatureWriter and
# SnowflakeFeatureWriter. Those classes are coupled to PySpark
# and to WRITING — pulling them in here would force this
# lightweight agent script to import all of PySpark just to run
# a single GET against Redis. Instead, these readers reuse the
# same key-naming conventions and table structure, with none of
# the heavy dependencies.
#
# The Weaviate/embedding classes from Phase 2 are the opposite
# case — already lightweight, already exactly fit for purpose —
# so those get imported and reused directly, not rebuilt.
# =============================================================

import json
import logging
from typing import Optional

import redis
import snowflake.connector
from langchain_core.tools import tool

from . import config as agent_config

# Reuse Phase 2's retrieval classes directly. Now that the code is a
# real package, this is an ordinary absolute import — the sys.path
# insert + sys.modules["config"] pop/restore dance that used to live
# here (to stop retrieval's `config` from colliding with the agent's
# `config`) is GONE. The two modules are distinct package paths now
# (fraud_platform.retrieval.config vs fraud_platform.agents.single_agent.config),
# so there is simply no name to collide.
from fraud_platform.retrieval.weaviate_client import FraudKnowledgeBase
from fraud_platform.retrieval.embedder import Embedder

logger = logging.getLogger(__name__)


# =============================================================
# LIGHTWEIGHT REDIS READER
# =============================================================
class FeatureReader:
    """
    Reads the online feature store. Pure consumer — GET only,
    never SETEX. Same key pattern as feature_engine.py's
    RedisFeatureWriter (user:{user_id}:features), so this class
    and the writer agree on the data shape without sharing code.
    """

    def __init__(self):
        self._client = redis.Redis(
            host=agent_config.REDIS_HOST,
            port=agent_config.REDIS_PORT,
            db=agent_config.REDIS_DB,
            decode_responses=True,
        )

    def get_features(self, user_id: str) -> Optional[dict]:
        raw = self._client.get(f"user:{user_id}:features")
        return json.loads(raw) if raw else None


# =============================================================
# LIGHTWEIGHT SNOWFLAKE READER
# =============================================================
class UserHistoryReader:
    """
    Reads DIM.DIM_USERS and DIM.DIM_DEVICES only. Deliberately
    never touches RAW or FEATURES — matches the AGENT_ROLE access
    boundary designed on Day 1 (agents get DIM + FEATURES + write
    to DECISIONS; RAW is pipeline-only), enforced here as a
    matter of code discipline even while running under
    ACCOUNTADMIN during development.
    """

    def __init__(self):
        self._conn = None

    def _get_connection(self):
        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(
                account=agent_config.SNOWFLAKE_ACCOUNT,
                user=agent_config.SNOWFLAKE_USER,
                password=agent_config.SNOWFLAKE_PASSWORD,
                database=agent_config.SNOWFLAKE_DATABASE,
                warehouse=agent_config.SNOWFLAKE_WAREHOUSE,
                role=agent_config.SNOWFLAKE_ROLE,
                schema="DIM",
            )
        return self._conn

    def get_user_history(self, user_id: str) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT full_name, home_city, home_country, risk_tier,
                       avg_transaction_amt, stddev_transaction_amt,
                       account_created_at
                FROM DIM.DIM_USERS
                WHERE user_id = %(user_id)s AND is_current = TRUE
                """,
                {"user_id": user_id},
            )
            row = cursor.fetchone()
            if not row:
                return None
            cols = [d[0].lower() for d in cursor.description]
            user_profile = dict(zip(cols, row))

            cursor.execute(
                """
                SELECT COUNT(*) AS trusted_device_count
                FROM DIM.DIM_DEVICES
                WHERE user_id = %(user_id)s AND is_trusted = TRUE
                """,
                {"user_id": user_id},
            )
            device_row = cursor.fetchone()
            user_profile["trusted_device_count"] = device_row[0] if device_row else 0

            return user_profile
        finally:
            cursor.close()


# =============================================================
# SHARED INSTANCES
# One of each reader per process — these tools get called
# repeatedly across many transactions in a real run, so the
# same connection-reuse reasoning from feature_engine.py applies:
# lazy-cached, per-instance, safe because each is used by exactly
# this one agent process, sequentially, never concurrently with
# itself.
# =============================================================
_feature_reader = FeatureReader()
_history_reader = UserHistoryReader()
_embedder: Optional[Embedder] = None
_knowledge_base: Optional[FraudKnowledgeBase] = None


def _get_embedder() -> Embedder:
    global _embedder
    if _embedder is None:
        _embedder = Embedder()
    return _embedder


def _get_knowledge_base() -> FraudKnowledgeBase:
    global _knowledge_base
    if _knowledge_base is None:
        _knowledge_base = FraudKnowledgeBase().connect()
    return _knowledge_base


# =============================================================
# THE THREE TOOLS
# Docstrings here are NOT just documentation — they are what the
# LLM actually reads to decide when and how to call each tool.
# Precision in these docstrings directly affects tool-calling
# reliability.
# =============================================================

@tool
def get_transaction_features(user_id: str) -> str:
    """
    Get the current, live fraud-detection features for a user
    from the online feature store. Returns velocity counts,
    amount z-score, geo-distance from last known location, new
    device flag, and the pre-computed risk score.

    Use this FIRST, before other tools — it tells you what
    signals are actually elevated for this user right now, which
    should guide what you look up next (for example, only look
    up policy context for the specific pattern that's actually
    elevated, not all four patterns every time).

    Args:
        user_id: the user's unique identifier.
    """
    features = _feature_reader.get_features(user_id)
    if not features:
        return f"No feature data found in the online store for user {user_id}."
    return json.dumps(features, indent=2, default=str)


@tool
def get_policy_context(question: str, fraud_pattern: Optional[str] = None) -> str:
    """
    Search the fraud policy knowledge base for guidance relevant
    to a question. Uses hybrid search (keyword + semantic), so
    you can ask in natural language rather than needing exact
    terminology from the policy documents.

    Optionally narrow to one specific pattern if you already know
    which one is relevant (VELOCITY_SPIKE, GEO_JUMP, NEW_DEVICE,
    or AMOUNT_ANOMALY) — this returns more focused results than
    a broad search across all four patterns.

    Args:
        question: what you want to know, in plain language —
            e.g. "should I block a transaction from a new device
            with a normal amount".
        fraud_pattern: optional. One of VELOCITY_SPIKE, GEO_JUMP,
            NEW_DEVICE, AMOUNT_ANOMALY, to restrict the search.
    """
    embedder = _get_embedder()
    kb = _get_knowledge_base()

    query_vector = embedder.embed_query(question)
    results = kb.hybrid_search(
        query_text=question,
        query_vector=query_vector,
        limit=3,
        pattern_filter=fraud_pattern,
    )

    if not results:
        return "No relevant policy guidance found."

    formatted = []
    for r in results:
        formatted.append(
            f"[{r['pattern']} / {r['section']}] (relevance: {r['score']:.2f})\n{r['text']}"
        )
    return "\n\n---\n\n".join(formatted)


@tool
def get_user_history(user_id: str) -> str:
    """
    Get a user's baseline profile: their typical spending
    average and standard deviation, home location, risk tier,
    account age, and how many trusted devices they have on file.

    Use this when a transaction is borderline and you need more
    context about what's NORMAL for this specific user, beyond
    just the current transaction's features. Not needed for
    clear-cut cases where the current features alone are decisive
    (for example, an already-saturated hard-rule signal).

    Args:
        user_id: the user's unique identifier.
    """
    history = _history_reader.get_user_history(user_id)
    if not history:
        return f"No user profile found for user {user_id}."
    return json.dumps(history, indent=2, default=str)


ALL_TOOLS = [get_transaction_features, get_policy_context, get_user_history]