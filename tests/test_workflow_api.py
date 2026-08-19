# =============================================================
# UNIT TESTS — FastAPI surface (mock engine, in-memory store)
# =============================================================
# Full HTTP round-trips via TestClient against a MOCK engine (fake
# tools + canned planner + in-memory SQLite) — no Groq/Snowflake.
# =============================================================

import pytest

pytest.importorskip("fastapi")  # the [workflow] extra provides fastapi

from fastapi.testclient import TestClient  # noqa: E402

from fraud_platform.workflow_engine.api import build_engine, create_app  # noqa: E402

VALID = ("After every payment capture, if the user has 2 or more BLOCK decisions "
         "in the last 24 hours, send a Slack message with their recent history.")
REFUSAL = "Delete all BLOCK decisions from last week."


@pytest.fixture()
def client():
    app = create_app(engine=build_engine(mock=True, db_path=":memory:"))
    return TestClient(app)


def test_create_valid_workflow_is_ready_with_plan(client):
    r = client.post("/workflows", json={"instruction": VALID, "user_id": "u1"})
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "READY"
    assert body["feasibility"]["ok"] is True
    assert body["plan"]["trigger"] == "payment.captured"
    assert len(body["plan"]["steps"]) == 3


def test_create_destructive_workflow_is_rejected(client):
    r = client.post("/workflows", json={"instruction": REFUSAL})
    body = r.json()
    assert body["state"] == "REJECTED"
    assert body["feasibility"]["ok"] is False


def test_event_dispatch_runs_matching_workflow_and_fills_outbox(client):
    client.post("/workflows", json={"instruction": VALID, "user_id": "u1"})
    # simulated webhook
    r = client.post("/events/payment.captured", json={"user_id": "u-blocks"})
    body = r.json()
    assert body["matched"] == 1
    assert body["runs"][0]["status"] == "COMPLETED"
    # the outbox holds the exact slack payload that would be delivered
    ob = client.get("/outbox").json()["outbox"]
    assert ob and ob[0]["connector"] == "slack" and "u-blocks" in ob[0]["payload_json"]


def test_workflow_is_reusable_across_events(client):
    wid = client.post("/workflows", json={"instruction": VALID}).json()["workflow_id"]
    client.post("/events/payment.captured", json={"user_id": "a"})
    # second event must re-fire the (now COMPLETED) workflow, not skip it
    r2 = client.post("/events/payment.captured", json={"user_id": "b"})
    assert r2.json()["matched"] == 1 and r2.json()["runs"][0]["status"] == "COMPLETED"
    detail = client.get(f"/workflows/{wid}").json()
    assert detail["workflow"]["state"] == "COMPLETED"
    assert len(client.get("/outbox").json()["outbox"]) == 2


def test_on_demand_execute_and_404(client):
    wid = client.post("/workflows", json={"instruction": VALID}).json()["workflow_id"]
    r = client.post(f"/workflows/{wid}/execute", json={"trigger_payload": {"user_id": "z"}})
    assert r.status_code == 200 and r.json()["status"] == "COMPLETED"
    assert client.get("/workflows/does-not-exist").status_code == 404
