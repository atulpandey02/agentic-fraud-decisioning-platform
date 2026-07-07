# =============================================================
# INTEGRATION TESTS (mocked Snowflake) — HITL state transitions
# =============================================================
# Priority 4 item 2: exercise the HITL workflow against a MOCKED
# Snowflake adapter, so the state-transition logic (atomic review,
# rowcount handling, validation-before-write) is covered with no
# credentials and no live database. The connection is replaced with
# a MagicMock; we assert on what SQL/params the handler issues and
# how it reacts to the driver's rowcount.
# =============================================================

from unittest.mock import MagicMock

import pytest

from fraud_platform.governance.hitl_handler import HITLHandler, ReviewConflict
from fraud_platform.db.validators import ValidationError


def _handler_with_mock(rowcount=1):
    """An HITLHandler whose _get_connection returns a mock conn whose
    cursor reports `rowcount`. Returns (handler, conn, cursor)."""
    handler = HITLHandler()
    cursor = MagicMock()
    cursor.rowcount = rowcount
    conn = MagicMock()
    conn.cursor.return_value = cursor
    handler._get_connection = lambda: conn
    return handler, conn, cursor


class TestRecordReviewAtomicity:
    def test_successful_review_commits(self):
        handler, conn, cursor = _handler_with_mock(rowcount=1)
        handler.record_review("dec1", "reviewer_A", "CONFIRMED", "ok")
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        # the atomic guard must be in the executed SQL
        sql = cursor.execute.call_args[0][0]
        assert "human_reviewed = FALSE" in sql

    def test_zero_rows_raises_conflict_and_rolls_back(self):
        # already-reviewed or missing row -> UPDATE matches nothing
        handler, conn, cursor = _handler_with_mock(rowcount=0)
        with pytest.raises(ReviewConflict):
            handler.record_review("dec1", "reviewer_B", "OVERRIDDEN")
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_invalid_outcome_rejected_before_db(self):
        handler, conn, cursor = _handler_with_mock(rowcount=1)
        with pytest.raises(ValidationError):
            handler.record_review("dec1", "reviewer_C", "APPROVED")  # not a valid outcome
        cursor.execute.assert_not_called()  # never reached the database


class TestPersistDecisionValidation:
    def _state(self, **over):
        s = {
            "transaction_id": "t1", "user_id": "u1", "decision": "ALLOW",
            "confidence_score": 0.9, "reasoning_text": "ok", "identified_pattern": "NONE",
        }
        s.update(over)
        return s

    def test_valid_decision_inserts_and_commits(self):
        handler, conn, cursor = _handler_with_mock()
        did = handler.persist_decision(self._state(), "AUTO_APPROVE", 100, snapshot_id="s1")
        assert isinstance(did, str) and did
        conn.commit.assert_called_once()

    def test_autonomous_without_confidence_rejected_before_insert(self):
        handler, conn, cursor = _handler_with_mock()
        with pytest.raises(ValidationError):
            handler.persist_decision(
                self._state(decision="BLOCK", confidence_score=None), "NOTIFY_ONLY", 100
            )
        cursor.execute.assert_not_called()

    def test_bad_tier_rejected_before_insert(self):
        handler, conn, cursor = _handler_with_mock()
        with pytest.raises(ValidationError):
            handler.persist_decision(self._state(), "MAYBE_LATER", 100)
        cursor.execute.assert_not_called()

    def test_escalate_may_omit_confidence(self):
        handler, conn, cursor = _handler_with_mock()
        did = handler.persist_decision(
            self._state(decision="ESCALATE", confidence_score=None), "SUGGEST", 100
        )
        assert did
        conn.commit.assert_called_once()
