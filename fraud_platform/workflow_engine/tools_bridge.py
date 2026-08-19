# =============================================================
# TOOLS BRIDGE — wrap existing platform capabilities as ToolSpecs
# =============================================================
# The workflow engine does NOT reimplement data access. It reuses
# what already exists — the Phase 3 agent tools (Redis features,
# Snowflake user history) and the Agentic BI guarded NL2SQL — and
# exposes them through the registry's uniform ToolSpec interface.
# The heavy platform imports are LAZY (inside the execute closures)
# so building the registry — and unit-testing its shape — needs no
# live Redis/Snowflake/Weaviate/Groq.
#
# `count_recent_decisions` is the one NEW tool: deterministic,
# parameterized SQL with NO LLM. It is the workhorse for trigger
# CONDITIONS ("2+ BLOCKs in 24h"), and being deterministic it is
# preferred over the LLM-authored query_decisions whenever the
# request is a countable condition (the planner prompt enforces this).
# =============================================================

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from . import config
from .connectors import OutboxWriter, make_email_send, make_slack_send
from .registry import ANALYZE, NOTIFY, READ_DATA, ToolRegistry, ToolSpec

logger = logging.getLogger(__name__)


# -------------------------------------------------------------
# ARG SCHEMAS — each tool's contract, validated at dispatch.
# -------------------------------------------------------------
class UserIdArgs(BaseModel):
    user_id: str = Field(description="the user's unique identifier")


class QueryArgs(BaseModel):
    question: str = Field(description="a plain-English analytics question over the decision log")


class CountArgs(BaseModel):
    user_id: str = Field(description="the user's unique identifier")
    decision: str = Field(description="one of ALLOW, BLOCK, ESCALATE")
    hours: int = Field(default=24, ge=1, le=720, description="look-back window in hours")


class SlackArgs(BaseModel):
    channel: str = Field(description="Slack channel, e.g. #fraud-ops")
    text: str = Field(description="the message body")


class EmailArgs(BaseModel):
    to: str = Field(description="recipient email address")
    subject: str = Field(description="email subject")
    body: str = Field(description="email body")


# -------------------------------------------------------------
# EXECUTE CLOSURES — lazy imports keep the platform's heavy deps
# out of registry construction and tests.
# -------------------------------------------------------------
def _get_transaction_features(user_id: str) -> str:
    from fraud_platform.agents.single_agent.tools import get_transaction_features
    return get_transaction_features.invoke({"user_id": user_id})


def _get_user_history(user_id: str) -> str:
    from fraud_platform.agents.single_agent.tools import get_user_history
    return get_user_history.invoke({"user_id": user_id})


def _query_decisions(question: str) -> dict:
    """Bridge the guarded, read-only Agentic BI NL2SQL. The SQL is
    re-validated as SELECT-only by the BI layer's AST guard; we
    return the SQL alongside the rows so it stays auditable — the
    'SQL always shown' principle carried into workflows."""
    from fraud_platform.bi_dashboard.nl2sql_agent import NL2SQLAgent
    gq, cols, rows = NL2SQLAgent().ask(question)
    return {"sql": gq.sql, "columns": cols, "rows": rows[:100]}


def _count_recent_decisions(user_id: str, decision: str, hours: int = 24) -> dict:
    """Deterministic, parameterized COUNT over FACT_DECISIONS — no LLM.
    Connects under the least-privilege agent role, reads DECISIONS."""
    import snowflake.connector
    from fraud_platform.settings import get_settings

    s = get_settings()
    conn = snowflake.connector.connect(
        account=s.snowflake.account, user=s.snowflake.user,
        password=s.snowflake.password, database=s.snowflake.database,
        warehouse=s.snowflake.warehouse, role=s.agent_connect_role(),
        schema="DECISIONS",
    )
    try:
        cur = conn.cursor()
        cur.execute("USE SECONDARY ROLES NONE")
        cur.execute(
            """
            SELECT COUNT(*) FROM DECISIONS.FACT_DECISIONS
            WHERE user_id = %(user_id)s
              AND decision = %(decision)s
              AND decided_at >= DATEADD(hour, -%(hours)s, CURRENT_TIMESTAMP())
            """,
            {"user_id": user_id, "decision": decision, "hours": hours},
        )
        count = cur.fetchone()[0]
        cur.close()
        return {"user_id": user_id, "decision": decision, "hours": hours, "count": int(count)}
    finally:
        conn.close()


# -------------------------------------------------------------
# REGISTRY ASSEMBLY
# -------------------------------------------------------------
def build_default_registry(outbox_writer: OutboxWriter) -> ToolRegistry:
    """The workflow engine's default tool catalog. `outbox_writer` is
    injected so NOTIFY connectors persist to the workflow store the
    caller owns (SQLite here; Postgres later) without this module
    importing the persistence layer."""
    r = ToolRegistry()

    r.register(ToolSpec(
        name="get_transaction_features",
        description="Get a user's current live fraud-detection features "
                    "(velocity, amount z-score, geo distance, new-device flag) "
                    "from the online feature store.",
        args_schema=UserIdArgs, category=READ_DATA, execute=_get_transaction_features,
    ))
    r.register(ToolSpec(
        name="get_user_history",
        description="Get a user's baseline profile: typical spend, home "
                    "location, risk tier, account age, trusted device count.",
        args_schema=UserIdArgs, category=READ_DATA, execute=_get_user_history,
    ))
    r.register(ToolSpec(
        name="query_decisions",
        description="Answer a plain-English analytics question over the "
                    "decision log with guarded, read-only SELECT-only SQL "
                    "(the generated SQL is returned for audit). Use for "
                    "open-ended questions, NOT for simple countable conditions.",
        args_schema=QueryArgs, category=READ_DATA, execute=_query_decisions,
    ))
    r.register(ToolSpec(
        name="count_recent_decisions",
        description="Deterministically COUNT how many decisions of a given "
                    "type (ALLOW/BLOCK/ESCALATE) a user received in the last N "
                    "hours. No LLM. PREFER this over query_decisions for any "
                    "countable trigger condition.",
        args_schema=CountArgs, category=ANALYZE, execute=_count_recent_decisions,
    ))
    r.register(ToolSpec(
        name="slack_send_message",
        description="Send a Slack message to a channel. Writes to the "
                    "auditable outbox (and posts to a real webhook only if "
                    "configured).",
        args_schema=SlackArgs, category=NOTIFY,
        execute=make_slack_send(outbox_writer),
        read_only=False, requires_approval=False,
    ))
    r.register(ToolSpec(
        name="email_send",
        description="Send an email. Writes to the auditable outbox (mock "
                    "delivery — SMTP is a future feature).",
        args_schema=EmailArgs, category=NOTIFY,
        execute=make_email_send(outbox_writer),
        read_only=False, requires_approval=False,
    ))
    return r
