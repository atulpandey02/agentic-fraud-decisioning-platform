# =============================================================
# AUDIT LOGGER — persists reasoning traces to FACT_AGENT_TRACES
# =============================================================
# The table has existed since Day 1 with zero rows and zero
# writers — this class is the writer it was waiting for. The
# mapping it implements is the one AgentState's docstring
# designed back in Phase 3: the messages list IS the audit
# trail; step_number = position in the list, step_type = the
# message's role, agent_name = who produced it.
#
# Division of labor with LangSmith (see langsmith_config.py):
#   FACT_AGENT_TRACES = the platform's OWN audit record — which
#   agent acted, in what order, saying what. Owned data, queryable
#   next to the decisions it explains, survives vendor churn.
#   LangSmith = prompt-level developer telemetry (full prompts,
#   token counts, latencies per LLM call) for debugging quality.
#   Compliance asks "who decided and why" — that lives here.
#   Engineering asks "why was that prompt slow/wrong" — LangSmith.
# =============================================================

import uuid
import json
import logging
from typing import List, Optional

import snowflake.connector
from langchain_core.messages import BaseMessage, AIMessage, HumanMessage, ToolMessage

import config

logger = logging.getLogger(__name__)


# The FACT_AGENT_TRACES column order, in ONE place. Both the INSERT's
# column list and the per-row params are built from this list, so they
# cannot drift out of alignment, and tests/test_schema_contract.py can
# validate it against db/schema_contract.py. tool_input/tool_output are
# VARIANT columns loaded via PARSE_JSON (see write_trace) — their
# positions here (indices 6, 7) drive which projected columns get wrapped.
FACT_AGENT_TRACES_COLUMNS = [
    "trace_id", "decision_id", "step_number", "agent_name", "step_type",
    "tool_name", "tool_input", "tool_output", "reasoning_text", "tokens_used",
]
_VARIANT_TRACE_COLUMNS = {"tool_input", "tool_output"}


class AgentTraceWriter:
    """
    Maps a finished run's messages list -> FACT_AGENT_TRACES rows.

    Same lazily-cached instance-level connection pattern as every
    Snowflake writer in this codebase (see SnowflakeFeatureWriter
    for the not-a-Singleton reasoning).
    """

    def __init__(self):
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
                schema="DECISIONS",
            )
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed():
            self._conn.close()
            self._conn = None

    # ----------------------------------------------------------
    # MESSAGE -> ROW MAPPING
    # ----------------------------------------------------------
    @staticmethod
    def _classify_step(msg: BaseMessage) -> str:
        """
        step_type per message role — the three values the schema
        comment anticipated ('TOOL_CALL', 'REASONING', 'HANDOFF')
        plus INPUT/TOOL_OUTPUT for the loop's non-AI messages:

          HumanMessage                     -> INPUT      (what was asked)
          AIMessage with tool_calls        -> TOOL_CALL  (Phase 3 ReAct steps)
          ToolMessage                      -> TOOL_OUTPUT
          AIMessage named 'orchestrator'   -> HANDOFF    (Phase 4 routing)
          any other AIMessage              -> REASONING  (analysis / findings)
        """
        if isinstance(msg, HumanMessage):
            return "INPUT"
        if isinstance(msg, ToolMessage):
            return "TOOL_OUTPUT"
        if isinstance(msg, AIMessage):
            if getattr(msg, "tool_calls", None):
                return "TOOL_CALL"
            if getattr(msg, "name", None) == "orchestrator":
                return "HANDOFF"
            return "REASONING"
        return "OTHER"

    def _row_for_message(self, decision_id: str, step_number: int, msg: BaseMessage) -> dict:
        step_type = self._classify_step(msg)

        tool_name: Optional[str] = None
        tool_input_json: Optional[str] = None
        tool_output_json: Optional[str] = None

        if step_type == "TOOL_CALL":
            calls = msg.tool_calls
            # An AIMessage can request several tools at once; the row
            # keeps ONE step per message (step_number stays the honest
            # position in the conversation) and stores every call in
            # the VARIANT column rather than exploding into sub-steps.
            tool_name = ", ".join(c["name"] for c in calls)
            tool_input_json = json.dumps([{"name": c["name"], "args": c["args"]} for c in calls])
        elif step_type == "TOOL_OUTPUT":
            tool_name = getattr(msg, "name", None)
            tool_output_json = json.dumps({"content": str(msg.content)})

        # Token usage: only real LLM responses carry usage_metadata.
        # The multi-agent narrative messages are constructed by our own
        # code, so they have none — NULL is the honest value there
        # (per-call token detail for those runs lives in LangSmith).
        usage = getattr(msg, "usage_metadata", None)
        tokens_used = usage.get("total_tokens") if usage else None

        content = msg.content if isinstance(msg.content, str) else json.dumps(msg.content)

        # agent_name: the message's own name tag when present (multi-
        # agent runs tag every message). The fallback differs by role:
        # an INPUT step is the REQUEST, not any agent's output, so it
        # gets 'user'; unnamed AI/tool steps only occur in Phase 3's
        # single-agent runs, whose messages carry no name tags.
        if step_type == "INPUT":
            agent_name = getattr(msg, "name", None) or "user"
        else:
            agent_name = getattr(msg, "name", None) or "single_agent"

        return {
            "trace_id": str(uuid.uuid4()),
            "decision_id": decision_id,
            "step_number": step_number,
            "agent_name": agent_name,
            "step_type": step_type,
            "tool_name": tool_name,
            "tool_input": tool_input_json,
            "tool_output": tool_output_json,
            "reasoning_text": content if step_type in ("INPUT", "REASONING", "HANDOFF") else None,
            "tokens_used": tokens_used,
        }

    # ----------------------------------------------------------
    # WRITE
    # ----------------------------------------------------------
    def write_trace(self, decision_id: str, messages: List[BaseMessage]) -> int:
        """
        Persist one run's full reasoning trace, linked to the
        FACT_DECISIONS row it produced.

        Why INSERT ... SELECT ... FROM VALUES instead of a plain
        executemany over INSERT ... VALUES: tool_input/tool_output
        are VARIANT columns, and Snowflake only accepts PARSE_JSON()
        in a SELECT projection, not inside a bulk VALUES clause. The
        FROM VALUES form binds every row's fields as plain strings,
        then the projection converts the two JSON columns — one
        statement, one round trip, arbitrary row count. (Traces are
        ~6-12 rows per decision, so even a per-row loop would work;
        this form is used because it is the CORRECT general pattern
        for VARIANT loads through the Python connector, worth having
        one canonical example of in this codebase.)
        """
        rows = [
            self._row_for_message(decision_id, i, msg)
            for i, msg in enumerate(messages)
        ]
        if not rows:
            return 0

        cols = FACT_AGENT_TRACES_COLUMNS
        col_list = ", ".join(cols)
        # Positional VALUES columns are 1-based; wrap the two VARIANT
        # columns in PARSE_JSON, pass the rest through unchanged.
        projection = ", ".join(
            f"PARSE_JSON(column{i + 1})" if c in _VARIANT_TRACE_COLUMNS else f"column{i + 1}"
            for i, c in enumerate(cols)
        )
        row_placeholder = "(" + ", ".join(["%s"] * len(cols)) + ")"
        placeholders = ", ".join([row_placeholder] * len(rows))
        sql = f"""
            INSERT INTO DECISIONS.FACT_AGENT_TRACES ({col_list})
            SELECT {projection}
            FROM VALUES {placeholders}
        """
        params = []
        for r in rows:
            params.extend(r[c] for c in cols)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
            logger.info(f"Trace written: {len(rows)} steps for decision {decision_id}")
            return len(rows)
        except Exception as e:
            conn.rollback()
            logger.error(f"FACT_AGENT_TRACES insert FAILED: {e}")
            raise
        finally:
            cursor.close()
