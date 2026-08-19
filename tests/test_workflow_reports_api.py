# =============================================================
# API TESTS — report preview / schedule / pause / resume
# =============================================================
# A REAL (non-mock) engine built offline: the registry's tool bodies
# are lazy and the planner's LLM is lazy, so no Groq/Snowflake is
# touched by preview/schedule/pause (none of which EXECUTE). The
# scheduler is not started (prebuilt engine => no background thread);
# next_run is still computed from the cron trigger.
# =============================================================

import pytest

# The [workflow] extra (not installed in CI's base test env) provides these;
# skip the whole module cleanly when they're absent, same as test_workflow_api.
pytest.importorskip("fastapi")
pytest.importorskip("apscheduler")   # the schedule endpoints build cron triggers

from fastapi.testclient import TestClient  # noqa: E402

from fraud_platform.workflow_engine.api import build_engine, create_app  # noqa: E402


def _client():
    engine = build_engine(mock=False, db_path=":memory:")
    return TestClient(create_app(engine)), engine


_SCHED = {"frequency": "daily", "hour": 22, "minute": 0, "timezone": "America/New_York"}
_SLACK = {"connector": "slack", "channel": "#fraud-ops"}


class TestPreviewShowsPlanBeforeActivation:
    def test_preview_returns_plan_and_persists_nothing(self):
        client, engine = _client()
        r = client.post("/reports/preview", json={
            "title": "Daily Fraud Operations Report", "report": "fraud_performance",
            "schedule": _SCHED, "destination": _SLACK, "sql": ""})
        assert r.status_code == 200, r.text
        body = r.json()
        tools = [s["tool_name"] for s in body["plan"]["steps"]]
        assert tools == ["run_report_query", "format_report", "slack_send_message"]
        assert "10:00 PM" in body["schedule"] and "America/New_York" in body["schedule"]
        assert body["next_run"] is not None
        assert body["feasibility"]["ok"] is True
        # nothing was created — the preview is not a hidden workflow
        assert engine.store.list_workflows() == []


class TestScheduleReport:
    def test_named_report_creates_ready_scheduled_workflow(self):
        client, engine = _client()
        r = client.post("/reports/schedule", json={
            "title": "Daily Fraud Operations Report", "report": "fraud_performance",
            "schedule": _SCHED, "destination": _SLACK, "sql": ""})
        assert r.status_code == 200, r.text
        body = r.json()
        wid = body["workflow_id"]
        assert body["state"] == "READY" and body["next_run"] is not None
        wf = engine.store.get_workflow(wid)
        assert wf["trigger_type"] == "schedule"
        assert wf["schedule_json"] is not None and wf["destination_json"] is not None
        assert wf["next_run_at"] is not None

    def test_adhoc_select_sql_is_stored_as_template(self):
        client, engine = _client()
        r = client.post("/reports/schedule", json={
            "title": "Blocks by day", "sql":
                "SELECT COUNT(*) AS blocks FROM DECISIONS.FACT_DECISIONS WHERE decision='BLOCK'",
            "schedule": _SCHED, "destination": _SLACK})
        assert r.status_code == 200, r.text
        wf = engine.store.get_workflow(r.json()["workflow_id"])
        assert wf["template_id"] is not None
        assert engine.store.get_template(wf["template_id"])["sql"].upper().startswith("SELECT")

    def test_missing_timezone_is_rejected(self):
        client, _ = _client()
        r = client.post("/reports/schedule", json={
            "title": "R", "report": "fraud_performance",
            "schedule": {"frequency": "daily", "hour": 22}, "destination": _SLACK, "sql": ""})
        assert r.status_code == 400
        assert "timezone" in r.text

    def test_destructive_sql_is_rejected_by_guard(self):
        client, _ = _client()
        r = client.post("/reports/schedule", json={
            "title": "R", "sql": "DELETE FROM DECISIONS.FACT_DECISIONS",
            "schedule": _SCHED, "destination": _SLACK})
        assert r.status_code == 422
        assert "rejected" in r.text.lower()

    def test_bad_destination_is_rejected(self):
        client, _ = _client()
        r = client.post("/reports/schedule", json={
            "title": "R", "report": "fraud_performance", "schedule": _SCHED,
            "destination": {"connector": "slack"}, "sql": ""})   # no channel
        assert r.status_code == 400


class TestPauseResume:
    def test_pause_clears_next_run_resume_restores_it(self):
        client, engine = _client()
        wid = client.post("/reports/schedule", json={
            "title": "R", "report": "fraud_performance", "schedule": _SCHED,
            "destination": _SLACK, "sql": ""}).json()["workflow_id"]

        p = client.post(f"/workflows/{wid}/pause")
        assert p.status_code == 200 and p.json()["schedule_paused"] is True
        assert engine.store.get_workflow(wid)["schedule_paused"] == 1
        assert engine.store.get_workflow(wid)["next_run_at"] is None

        res = client.post(f"/workflows/{wid}/resume")
        assert res.status_code == 200 and res.json()["next_run"] is not None
        assert engine.store.get_workflow(wid)["schedule_paused"] == 0

    def test_resume_non_scheduled_is_conflict(self):
        client, engine = _client()
        wid = engine.store.create_workflow("u", "manual thing")
        r = client.post(f"/workflows/{wid}/resume")
        assert r.status_code == 409
