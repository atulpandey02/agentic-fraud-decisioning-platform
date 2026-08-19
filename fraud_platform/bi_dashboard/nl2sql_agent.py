# =============================================================
# NL2SQL AGENT — natural language -> guarded SQL -> results
# =============================================================
# The Phase 7 "agentic BI" core: an analyst asks a question in
# English; the agent writes Snowflake SQL against the decision
# audit tables, the guardrails vet it, and only then does it run.
#
# The trust model, stated bluntly: the LLM is treated as an
# UNTRUSTED SQL author. Every safety property is enforced in
# code AFTER generation — the prompt asks for good behavior as a
# quality measure, but nothing depends on the model complying.
# This is the same layering as Phase 4 (router chooses, code
# enforces) and Phase 5 (agent decides, rules govern), applied to
# the sharpest tool yet: the prompt is the paved road, the
# validator is the guardrail, and only the guardrail is load-
# bearing. The allowlist notably excludes RAW.* — the BI surface
# must not be a side door into the PII schema BI_ROLE was
# explicitly designed never to see (rbac.sql, Day 1).
#
# Two layers of that guardrail, hardened in Priority 1:
#   - STRUCTURE: SQLValidator (sql_guard.py) parses the generated
#     SQL to an AST and allowlists query shape + table references,
#     replacing the original regex keyword scan that a table
#     function or dynamic identifier could walk straight past.
#   - AUTHORITY: db.open_bi_connection() confines the session to
#     BI_ROLE with secondary roles disabled, so even a query the
#     validator somehow passed cannot read what BI_ROLE can't.
#   Defense in depth: the validator not being perfect is fine
#   precisely because the database grants are the backstop.
# =============================================================

import logging
from typing import List, Tuple

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field

from . import config
from .sql_guard import SQLValidator, QueryRejected
from .connection import open_bi_connection

logger = logging.getLogger(__name__)


# The schema card the model sees. Hand-written, not introspected
# from Snowflake at runtime: the two tables change rarely, an
# INFORMATION_SCHEMA dump would include columns we don't want to
# advertise, and a curated card can explain SEMANTICS (what
# eval_correct=NULL means) that no introspection could produce.
SCHEMA_CARD = """
Table DECISIONS.FACT_DECISIONS — one row per agent fraud decision:
  decision_id VARCHAR, transaction_id VARCHAR, user_id VARCHAR,
  snapshot_id VARCHAR,
  decision VARCHAR            -- 'ALLOW' | 'BLOCK' | 'ESCALATE'
  confidence_score FLOAT      -- 0.0-1.0
  reasoning_text TEXT, identified_pattern VARCHAR,
  governance_tier VARCHAR     -- 'AUTO_APPROVE' | 'SUGGEST' | 'NOTIFY_ONLY'
  decided_at TIMESTAMP_NTZ, processing_latency_ms INT,
  human_reviewed BOOLEAN, human_reviewer_id VARCHAR,
  human_outcome VARCHAR       -- 'CONFIRMED' | 'OVERRIDDEN' | 'ESCALATED'
  human_reviewed_at TIMESTAMP_NTZ, human_notes TEXT,
  eval_correct BOOLEAN        -- NULL means the decision was an ESCALATE
                              -- (deferrals are scored neither right nor wrong)
  eval_scored_at TIMESTAMP_NTZ, llm_judge_score FLOAT, llm_judge_notes TEXT

Table FEATURES.FACT_FEATURE_SNAPSHOTS — one row per transaction's computed features:
  snapshot_id VARCHAR, transaction_id VARCHAR, user_id VARCHAR,
  computed_at TIMESTAMP_NTZ,
  velocity_15min INT, txn_amount FLOAT, amount_zscore FLOAT,
  geo_distance_km FLOAT, time_since_last_txn_min FLOAT,
  is_new_device BOOLEAN, risk_score_raw FLOAT,
  is_flagged_for_review BOOLEAN,
  is_synthetic_fraud BOOLEAN  -- ground truth label
  fraud_pattern VARCHAR       -- 'VELOCITY_SPIKE'|'GEO_JUMP'|'NEW_DEVICE'|'AMOUNT_ANOMALY'|NULL

Join them on snapshot_id (or transaction_id).
"""

NL2SQL_SYSTEM_PROMPT = f"""You are a SQL analyst for a fraud decisioning
platform, writing Snowflake SQL.

{SCHEMA_CARD}

Rules:
- ONE single SELECT statement (CTEs via WITH are fine). Never modify data.
- Query ONLY the two tables above.
- Always include a LIMIT (at most {config.BI_MAX_ROWS}).
- Prefer readable column aliases — they become chart labels.
- For time series, GROUP BY a DATE_TRUNC of the timestamp and alias it.
- If the question cannot be answered from these tables, say so in the
  explanation and return an empty sql string."""


class GeneratedQuery(BaseModel):
    sql: str = Field(description="The Snowflake SELECT statement, or empty string if unanswerable.")
    explanation: str = Field(description="1-2 sentences: what the query computes and why it answers the question.")
    chart_hint: str = Field(
        description="One of: bar, line, pie, table, metric — the most natural way to display this result."
    )


# QueryRejected now lives in sql_guard (the guard's home) and is
# re-exported here so existing importers — the Streamlit app in
# particular — keep working unchanged.
__all__ = ["NL2SQLAgent", "QueryRejected", "GeneratedQuery"]


class NL2SQLAgent:
    """
    English question -> (validated SQL, columns, rows, metadata).
    One LLM call per question; validation and execution in code.
    """

    def __init__(self):
        self._llm = ChatGroq(
            model=config.BI_LLM_MODEL,
            temperature=config.BI_LLM_TEMPERATURE,
            api_key=config.GROQ_API_KEY,
        ).with_structured_output(GeneratedQuery, method="json_schema")
        # The structural guardrail — injected the allowlist and row
        # cap from config, so this agent doesn't reach into config
        # for policy and the validator stays independently testable.
        self._validator = SQLValidator(
            allowed_tables=config.BI_ALLOWED_TABLES,
            max_rows=config.BI_MAX_ROWS,
        )
        self._conn = None

    def _get_connection(self):
        # The authority guardrail. open_bi_connection() confines the
        # session to BI_ROLE with secondary roles off, and fails
        # loudly rather than silently running over-privileged — see
        # db.py. Deliberately NOT config.SNOWFLAKE_ROLE (ACCOUNTADMIN
        # default): no admin credential belongs in the BI path.
        if self._conn is None or self._conn.is_closed():
            self._conn = open_bi_connection()
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed():
            self._conn.close()
            self._conn = None

    # ----------------------------------------------------------
    # GUARDRAILS — the load-bearing layer
    # ----------------------------------------------------------
    def validate_sql(self, sql: str) -> str:
        """
        Vet and normalize generated SQL, returning a safe, canonical,
        row-capped statement or raising QueryRejected. Delegates to
        SQLValidator (sql_guard.py) — AST parsing and structural
        allowlisting, which the original regex version could not do.
        Kept as a method (not inlined into ask) so the validation
        step stays an explicit, nameable part of the pipeline.
        """
        return self._validator.validate(sql)

    # ----------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------
    def ask(self, question: str) -> Tuple[GeneratedQuery, List[str], List[tuple]]:
        """
        Full round trip: generate -> validate -> execute.
        Returns (generated_query, column_names, rows).
        """
        generated: GeneratedQuery = self._llm.invoke([
            SystemMessage(content=NL2SQL_SYSTEM_PROMPT),
            HumanMessage(content=question),
        ])

        safe_sql = self.validate_sql(generated.sql)
        logger.info(f"NL2SQL executing:\n{safe_sql}")

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                safe_sql, timeout=config.BI_QUERY_TIMEOUT_SECONDS
            )
            columns = [d[0].lower() for d in cursor.description]
            rows = cursor.fetchall()
            return generated, columns, rows
        finally:
            cursor.close()
