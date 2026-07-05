# =============================================================
# HITL HANDLER — persistence + the human review loop
# =============================================================
# This is where agent decisions FINALLY become durable: until
# Phase 5, every decision lived and died inside a process
# (MemorySaver's honest limitation, flagged in both agent
# phases). The handler owns all writes to DECISIONS.FACT_DECISIONS
# and the review workflow over it — deliberately the one place
# that knows that table's column layout, the same single-owner
# rule RedisFeatureWriter applies to Redis key naming.
#
# Persistence lives HERE, in governance, not inside the agents:
# a decision row is only complete once it has a governance_tier,
# and the tier is assigned by the framework in this phase. Having
# the agents write partial rows that governance later UPDATEs
# would mean two writers, a window where a decision exists
# without a tier, and an UPDATE that can silently miss. One
# writer, one INSERT, row born complete.
#
# Access boundary: everything here touches only the DECISIONS
# schema — INSERT/UPDATE/SELECT — exactly the AGENT_ROLE grants
# in rbac.sql (applied in full as part of this phase). Runs as
# ACCOUNTADMIN in development; the discipline matches the
# intended production role, same as the Phase 3 readers.
# =============================================================

import uuid
import logging
from datetime import datetime
from typing import List, Dict, Optional

import snowflake.connector

import config

logger = logging.getLogger(__name__)


class HITLHandler:
    """
    Decision persistence + human-in-the-loop review workflow.

    Same lazily-cached, instance-level connection pattern as
    SnowflakeFeatureWriter and UserHistoryReader — one caller,
    sequential use, reconnect only if the session died. See
    feature_engine.py's SnowflakeFeatureWriter docstring for why
    this is deliberately NOT a Singleton.
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
    # WRITE — one decision, born complete
    # ----------------------------------------------------------
    def persist_decision(
        self,
        final_state: dict,
        governance_tier: str,
        processing_latency_ms: int,
        snapshot_id: Optional[str] = None,
    ) -> str:
        """
        Insert one complete row into FACT_DECISIONS from a finished
        agent run (either architecture — the output fields are the
        shared contract both write to state).

        Returns the generated decision_id, which is the key every
        later stage hangs off: the review workflow below, and Phase
        6's FACT_AGENT_TRACES rows (trace steps reference the
        decision they produced).
        """
        decision_id = str(uuid.uuid4())
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO DECISIONS.FACT_DECISIONS (
                    decision_id, transaction_id, user_id, snapshot_id,
                    decision, confidence_score, reasoning_text,
                    identified_pattern, governance_tier,
                    decided_at, processing_latency_ms
                ) VALUES (
                    %(decision_id)s, %(transaction_id)s, %(user_id)s, %(snapshot_id)s,
                    %(decision)s, %(confidence_score)s, %(reasoning_text)s,
                    %(identified_pattern)s, %(governance_tier)s,
                    %(decided_at)s, %(processing_latency_ms)s
                )
                """,
                {
                    "decision_id": decision_id,
                    "transaction_id": final_state["transaction_id"],
                    "user_id": final_state["user_id"],
                    "snapshot_id": snapshot_id,
                    "decision": final_state["decision"],
                    "confidence_score": final_state["confidence_score"],
                    "reasoning_text": final_state["reasoning_text"],
                    "identified_pattern": final_state["identified_pattern"],
                    "governance_tier": governance_tier,
                    "decided_at": datetime.utcnow(),
                    "processing_latency_ms": processing_latency_ms,
                },
            )
            conn.commit()
            logger.info(
                f"Persisted decision {decision_id}: {final_state['decision']} "
                f"({governance_tier}) for txn {final_state['transaction_id']}"
            )
            return decision_id
        except Exception as e:
            conn.rollback()
            logger.error(f"FACT_DECISIONS insert FAILED: {e}")
            raise
        finally:
            cursor.close()

    # ----------------------------------------------------------
    # READ — the review queue
    # ----------------------------------------------------------
    def pending_reviews(self, limit: int = 10) -> List[Dict]:
        """
        The human reviewer's inbox: SUGGEST-tier decisions nobody
        has reviewed yet, oldest first — age-ordered because a held
        suggestion is a held customer transaction; fairness here is
        latency fairness.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT decision_id, transaction_id, user_id, decision,
                       confidence_score, identified_pattern, reasoning_text,
                       decided_at
                FROM DECISIONS.FACT_DECISIONS
                WHERE governance_tier = %(tier)s
                  AND human_reviewed = FALSE
                ORDER BY decided_at ASC
                LIMIT %(limit)s
                """,
                {"tier": config.TIER_SUGGEST, "limit": limit},
            )
            cols = [d[0].lower() for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    # ----------------------------------------------------------
    # WRITE — a human closes the loop
    # ----------------------------------------------------------
    def record_review(
        self, decision_id: str, reviewer_id: str, outcome: str, notes: str = ""
    ) -> None:
        """
        Record a human verdict on a queued decision. outcome must be
        one of CONFIRMED / OVERRIDDEN / ESCALATED — validated here in
        code AND by the table's CHECK constraint; the app-level check
        exists so a typo fails with a readable message instead of a
        Snowflake constraint error.

        These reviewed rows are Phase 6's most valuable evaluation
        data: human-labeled agent decisions, accumulating as a free
        byproduct of the review workflow.
        """
        if outcome not in config.REVIEW_OUTCOMES:
            raise ValueError(
                f"outcome must be one of {config.REVIEW_OUTCOMES}, got {outcome!r}"
            )

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE DECISIONS.FACT_DECISIONS
                SET human_reviewed = TRUE,
                    human_reviewer_id = %(reviewer_id)s,
                    human_outcome = %(outcome)s,
                    human_reviewed_at = %(reviewed_at)s,
                    human_notes = %(notes)s
                WHERE decision_id = %(decision_id)s
                """,
                {
                    "reviewer_id": reviewer_id,
                    "outcome": outcome,
                    "reviewed_at": datetime.utcnow(),
                    "notes": notes,
                    "decision_id": decision_id,
                },
            )
            conn.commit()
            logger.info(f"Review recorded: {decision_id} -> {outcome} by {reviewer_id}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Review UPDATE failed: {e}")
            raise
        finally:
            cursor.close()
