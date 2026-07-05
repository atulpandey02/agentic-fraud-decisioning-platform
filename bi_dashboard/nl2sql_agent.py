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
# =============================================================

import re
import logging
from typing import List, Optional, Tuple

import snowflake.connector
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field

import config

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


class QueryRejected(Exception):
    """Raised when generated SQL fails guardrail validation —
    a distinct type so the UI can show 'the agent tried something
    disallowed' differently from a Snowflake execution error."""


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
        ).with_structured_output(GeneratedQuery)
        self._conn = None

    def _get_connection(self):
        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(
                account=config.SNOWFLAKE_ACCOUNT,
                user=config.SNOWFLAKE_USER,
                password=config.SNOWFLAKE_PASSWORD,
                database=config.SNOWFLAKE_DATABASE,
                warehouse=config.SNOWFLAKE_WAREHOUSE,
                role=config.SNOWFLAKE_ROLE,
            )
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
        Vet and normalize generated SQL. Raises QueryRejected with a
        human-readable reason on any violation. Returns the (possibly
        LIMIT-amended) statement to execute.

        Deliberately a blunt instrument: string/regex checks, not a
        SQL parser. A real deployment behind a real BI_ROLE would add
        the database's own grants as the final layer (defense in
        depth's whole point is that THIS layer doesn't have to be
        perfect) — but bluntness keeps the rules readable, and
        readable security rules are the ones that survive review.
        """
        cleaned = sql.strip().rstrip(";").strip()
        if not cleaned:
            raise QueryRejected("The agent produced no SQL for this question.")
        if ";" in cleaned:
            raise QueryRejected("Multiple SQL statements are not allowed.")

        head = cleaned.split(None, 1)[0].upper()
        if head not in ("SELECT", "WITH"):
            raise QueryRejected(f"Only SELECT queries are allowed (got '{head}').")

        forbidden = re.search(
            r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|CALL|COPY)\b",
            cleaned, re.IGNORECASE,
        )
        if forbidden:
            raise QueryRejected(f"Forbidden keyword: {forbidden.group(1).upper()}.")

        # Every schema-qualified table referenced must be allowlisted.
        # RAW.* is conspicuously absent from the allowlist — that IS
        # the PII boundary, enforced here in code.
        referenced = set(re.findall(r"\b([A-Za-z_]+\.[A-Za-z_]+)\b", cleaned))
        tables = {t.upper() for t in referenced if not t.upper().startswith("DATE_")}
        disallowed = {
            t for t in tables
            if "." in t and t not in [a.upper() for a in config.BI_ALLOWED_TABLES]
            # Only reject schema.table shapes that look like table refs,
            # not function calls or column paths — the second regex
            # element already excludes obvious DATE_ functions; anything
            # else unknown fails closed.
        }
        if disallowed:
            raise QueryRejected(f"Query references non-allowlisted tables: {sorted(disallowed)}.")

        if not re.search(r"\bLIMIT\s+\d+\b", cleaned, re.IGNORECASE):
            cleaned = f"{cleaned}\nLIMIT {config.BI_MAX_ROWS}"

        return cleaned

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
