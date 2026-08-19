# =============================================================
# UNIT TESTS — workflow state machine + SQLite persistence
# =============================================================
# In-memory SQLite (":memory:"), no infra. Covers the transition
# guard, the run/checkpoint/resume mechanics, and the outbox writer
# that wires the connectors to persistence.
# =============================================================

import pytest

from fraud_platform.workflow_engine.state import (
    InvalidTransition, WorkflowState, WorkflowStore,
    assert_transition, can_transition,
)


def _store():
    return WorkflowStore(db_path=":memory:")


class TestTransitions:
    def test_legal_and_illegal_edges(self):
        assert can_transition(WorkflowState.SUBMITTED, WorkflowState.PLANNED)
        assert not can_transition(WorkflowState.SUBMITTED, WorkflowState.RUNNING)
        # terminal states have no outgoing edges
        assert not can_transition(WorkflowState.COMPLETED, WorkflowState.RUNNING)
        with pytest.raises(InvalidTransition):
            assert_transition(WorkflowState.READY, WorkflowState.COMPLETED)

    def test_store_transition_validates(self):
        s = _store()
        wid = s.create_workflow("u1", "do a thing")
        s.transition(wid, WorkflowState.PLANNED)
        s.transition(wid, WorkflowState.FEASIBLE)
        s.transition(wid, WorkflowState.READY)
        assert s.get_workflow(wid)["state"] == "READY"
        # READY cannot jump straight to COMPLETED
        with pytest.raises(InvalidTransition):
            s.transition(wid, WorkflowState.COMPLETED)
        s.close()

    def test_approval_gate_is_a_state_edge(self):
        s = _store()
        wid = s.create_workflow("u1", "notify someone")
        s.transition(wid, WorkflowState.PLANNED)
        s.transition(wid, WorkflowState.FEASIBLE)
        # a plan needing approval parks in AWAITING_APPROVAL, then approve->READY
        s.transition(wid, WorkflowState.AWAITING_APPROVAL)
        with pytest.raises(InvalidTransition):
            s.transition(wid, WorkflowState.RUNNING)   # cannot skip approval
        s.transition(wid, WorkflowState.READY)
        assert s.get_workflow(wid)["state"] == "READY"
        s.close()


class TestRunsAndCheckpointing:
    def test_run_checkpoint_and_resume_anchor(self):
        s = _store()
        wid = s.create_workflow("u1", "3-step plan", trigger="payment.captured")
        run = s.start_run(wid)
        s.checkpoint_step(run, 1, "count_recent_decisions", {"user_id": "u1"}, {"count": 3}, "ok")
        s.checkpoint_step(run, 2, "get_user_history", {"user_id": "u1"}, {"risk": "HIGH"}, "ok")
        # step 3 fails — resume anchor must stay at the last OK step (2)
        s.checkpoint_step(run, 3, "slack_send_message", {"channel": "#x"}, {"error": "boom"}, "error")
        assert s.last_completed_step(run) == 2
        assert len(s.steps_for_run(run)) == 3
        s.finish_run(run, "FAILED")
        assert s.get_run(run)["status"] == "FAILED"
        s.close()

    def test_completed_run_anchor_is_last_step(self):
        s = _store()
        wid = s.create_workflow("u1", "2-step plan")
        run = s.start_run(wid)
        s.checkpoint_step(run, 1, "a", {}, {"ok": 1}, "ok")
        s.checkpoint_step(run, 2, "b", {}, {"ok": 2}, "ok")
        assert s.last_completed_step(run) == 2
        s.finish_run(run, "COMPLETED")
        assert s.latest_run(wid)["run_id"] == run
        s.close()


class TestOutboxAndTriggers:
    def test_outbox_writer_persists(self):
        s = _store()
        write = s.outbox_writer()
        write("slack", {"channel": "#fraud-ops", "text": "hi"})
        rows = s.outbox()
        assert len(rows) == 1 and rows[0]["connector"] == "slack"
        s.close()

    def test_connectors_write_through_the_store(self):
        # wiring check: Phase 1 connectors + Phase 2 store.
        from fraud_platform.workflow_engine.tools_bridge import build_default_registry
        s = _store()
        r = build_default_registry(s.outbox_writer())
        r.execute("slack_send_message", {"channel": "#fraud-ops", "text": "blocked user"})
        assert s.outbox()[0]["connector"] == "slack"
        s.close()

    def test_workflows_for_trigger_matches_only_runnable(self):
        s = _store()
        ready = s.create_workflow("u1", "on capture", trigger="payment.captured",
                                  state=WorkflowState.READY)
        s.create_workflow("u2", "on capture but submitted", trigger="payment.captured")
        s.create_workflow("u3", "other trigger", trigger="refund.created",
                          state=WorkflowState.READY)
        matched = s.workflows_for_trigger("payment.captured")
        assert [w["id"] for w in matched] == [ready]
        s.close()
