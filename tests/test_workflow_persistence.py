# =============================================================
# UNIT TESTS — schedule/template persistence + run idempotency
# =============================================================
# In-memory SQLite; no infra. Covers the new columns, the query
# template store, restart-recovery source, and the duplicate-fire
# idempotency guard.
# =============================================================

import sqlite3

import pytest

from fraud_platform.workflow_engine.state import WorkflowState, WorkflowStore


def _store():
    return WorkflowStore(db_path=":memory:")


class TestSchedulePersistence:
    def test_set_schedule_marks_and_records(self):
        s = _store()
        wid = s.create_workflow("u1", "daily report")
        tid = s.save_template("SELECT 1", title="t", workflow_id=wid)
        s.set_schedule(wid, schedule_json='{"frequency":"daily"}',
                       destination_json='{"connector":"slack","channel":"#fraud-ops"}',
                       template_id=tid)
        wf = s.get_workflow(wid)
        assert wf["trigger_type"] == "schedule"
        assert wf["schedule_json"] == '{"frequency":"daily"}'
        assert wf["template_id"] == tid
        assert wf["schedule_paused"] == 0
        s.close()

    def test_scheduled_workflows_excludes_paused_and_rejected(self):
        s = _store()
        a = s.create_workflow("u", "a"); s.set_schedule(a, "{}")
        b = s.create_workflow("u", "b"); s.set_schedule(b, "{}"); s.set_paused(b, True)
        c = s.create_workflow("u", "c")  # manual, not scheduled
        s.transition(c, WorkflowState.PLANNED); s.transition(c, WorkflowState.REJECTED)
        ids = {w["id"] for w in s.scheduled_workflows()}
        assert a in ids and b not in ids and c not in ids
        assert b in {w["id"] for w in s.scheduled_workflows(include_paused=True)}
        s.close()

    def test_next_and_last_run_roundtrip(self):
        s = _store()
        wid = s.create_workflow("u", "x")
        s.set_next_run(wid, "2026-08-20T02:00:00+00:00")
        s.set_last_run(wid, "2026-08-19T02:00:00+00:00")
        wf = s.get_workflow(wid)
        assert wf["next_run_at"].startswith("2026-08-20")
        assert wf["last_run_at"].startswith("2026-08-19")
        s.close()

    def test_set_field_rejects_unknown_column(self):
        s = _store()
        wid = s.create_workflow("u", "x")
        with pytest.raises(ValueError):
            s._set_field(wid, "state; DROP TABLE workflows", "boom")
        s.close()


class TestTemplateStore:
    def test_save_and_get_template(self):
        s = _store()
        tid = s.save_template("SELECT COUNT(*) FROM DECISIONS.FACT_DECISIONS", title="daily")
        t = s.get_template(tid)
        assert t["sql"].startswith("SELECT COUNT(*)")
        assert t["title"] == "daily"
        assert s.get_template("nope") is None
        s.close()


class TestRunIdempotency:
    def test_duplicate_scheduled_key_is_blocked(self):
        s = _store()
        wid = s.create_workflow("u", "x")
        key = "2026-08-19T22:00:00-04:00"
        assert not s.run_exists_for_key(wid, key)
        s.start_run(wid, scheduled_run_key=key)
        assert s.run_exists_for_key(wid, key)
        # a second run for the SAME fire-time violates the unique index
        with pytest.raises(sqlite3.IntegrityError):
            s.start_run(wid, scheduled_run_key=key)
        s.close()

    def test_manual_runs_have_null_key_and_are_unconstrained(self):
        # Two ad-hoc runs (no scheduled key) must both be allowed — NULLs
        # are not constrained by the unique index.
        s = _store()
        wid = s.create_workflow("u", "x")
        r1 = s.start_run(wid)
        r2 = s.start_run(wid)
        assert r1 != r2
        s.close()
