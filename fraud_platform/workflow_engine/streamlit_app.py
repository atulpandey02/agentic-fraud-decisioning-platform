# =============================================================
# STREAMLIT — "Workflows" page (natural-language automation)
# =============================================================
# The transparency equivalent of the BI page's "SQL always shown":
# here the PLAN is always shown — numbered steps with tool names and
# the feasibility verdict — before anything runs. Same visual
# language as the Agentic BI app, no new framework.
#
# Uses the engine in-process (build_engine), the same way the BI page
# calls its services directly. Default is --mock (offline: canned
# plan + fake tools) so the page is presentable with no infra;
# untick "Mock mode" to run against the real Snowflake/Groq stack.
#
# Run:  streamlit run fraud_platform/workflow_engine/streamlit_app.py
# =============================================================

from __future__ import annotations

import json

import streamlit as st

from fraud_platform.workflow_engine.api import build_engine
from fraud_platform.workflow_engine.feasibility import check_plan

_STATE_COLORS = {
    "READY": "🟢", "COMPLETED": "✅", "FEASIBLE": "🟢", "AWAITING_APPROVAL": "🟡",
    "RUNNING": "🔵", "REJECTED": "🔴", "FAILED": "🔴", "SUBMITTED": "⚪",
    "PLANNED": "⚪", "PAUSED": "🟠",
}

st.set_page_config(page_title="Workflows", page_icon="⚙️", layout="wide")


@st.cache_resource
def _engine(mock: bool):
    # Cached so the store persists across Streamlit reruns. A file DB for the
    # real stack; an in-memory DB (kept alive by the cache) for mock.
    return build_engine(mock=mock, db_path=None if not mock else ":memory:")


st.title("⚙️ Workflows — Natural-Language Automation")
st.caption("Describe an automation in plain English. The planner decomposes it "
           "into tool steps; feasibility is checked in code before anything runs.")

mock = st.sidebar.toggle("Mock mode (offline, no infra)", value=True)
event_user = st.sidebar.text_input("Simulated event user_id", value="demo-user")
engine = _engine(mock)

# ---- create + plan + feasibility ----
st.subheader("Describe an automation")
instruction = st.text_area(
    "Instruction",
    value="After every payment capture, if the user has 2 or more BLOCK "
          "decisions in the last 24 hours, send a Slack message with their "
          "recent history.",
    height=90,
)
if st.button("Plan & check feasibility", type="primary"):
    from fraud_platform.workflow_engine.state import WorkflowState
    wid = engine.store.create_workflow(event_user, instruction)
    engine.store.transition(wid, WorkflowState.PLANNED)
    plan = engine.planner.plan(instruction)
    engine.store.set_plan(wid, plan.model_dump_json())
    engine.store.set_trigger(wid, plan.trigger)
    report = check_plan(plan, engine.registry)
    if report.ok:
        engine.store.transition(wid, WorkflowState.FEASIBLE)
        engine.store.transition(wid, WorkflowState.AWAITING_APPROVAL
                                if report.requires_approval else WorkflowState.READY)
    else:
        engine.store.transition(wid, WorkflowState.REJECTED)

    st.markdown(f"**Goal:** {plan.goal}  ·  **Trigger:** `{plan.trigger}`")
    if plan.needs_clarification:
        st.warning(f"Needs clarification: {plan.clarification_question}")
    for s in plan.steps:
        st.markdown(f"**{s.step_number}. `{s.tool_name}`** — {s.rationale}")
        if s.when:
            st.caption(f"⤷ runs only when `{s.when}` — evaluated in code, not by the model")
        st.code(json.dumps(s.args), language="json")
    if report.ok:
        st.success("FEASIBILITY: PASS" + ("  (requires approval)" if report.requires_approval else ""))
    else:
        st.error("FEASIBILITY: REJECTED — the guardrail held")
        for e in report.errors:
            st.write(f"- {e}")

# ---- workflow list ----
st.subheader("Workflows")
wfs = engine.store.list_workflows()
if not wfs:
    st.info("No workflows yet — plan one above.")
for wf in wfs:
    badge = _STATE_COLORS.get(wf["state"], "⚪")
    st.write(f"{badge} **{wf['state']}** · `{wf['id'][:8]}` · {wf['instruction'][:80]}")

# ---- fire simulated event ----
st.subheader("Simulated webhook")
if mock:
    st.caption("Tip: in mock mode a `user_id` containing **safe** has count < 2, so the "
               "guard is false and the notify step is **SKIPPED** — that's the negative "
               "branch, where the workflow completes but nothing is sent.")
if st.button(f"Fire  payment.captured  {{user_id: {event_user!r}}}"):
    from fraud_platform.workflow_engine.planner import WorkflowPlan
    from fraud_platform.workflow_engine.state import WorkflowState
    matched = engine.store.workflows_for_trigger("payment.captured")
    if not matched:
        st.info("No READY workflow is triggered by payment.captured.")
    for wf in matched:
        plan = WorkflowPlan.model_validate_json(wf["plan_json"])
        if WorkflowState(wf["state"]) in (WorkflowState.COMPLETED, WorkflowState.FAILED,
                                          WorkflowState.FEASIBLE):
            engine.store.transition(wf["id"], WorkflowState.READY)
        before = len(engine.store.outbox())
        result = engine.executor.execute(wf["id"], plan, trigger_payload={"user_id": event_user})
        n_ok = sum(1 for o in result.steps if o.status == "ok")
        n_skip = sum(1 for o in result.steps if o.status == "skipped")
        st.write(f"Run **{result.status}** for `{wf['id'][:8]}` — {n_ok} ran, {n_skip} skipped")
        st.dataframe([
            {"step": o.step_number, "tool": o.tool_name, "status": o.status, "ms": o.latency_ms,
             "detail": (o.reason if o.status == "skipped"
                        else str(o.result or o.error or ""))[:90]}
            for o in result.steps
        ], use_container_width=True)
        new_rows = len(engine.store.outbox()) - before
        st.caption(f"Outbox: **{new_rows}** new payload(s) this run"
                   + ("  — the guard held, nothing sent" if new_rows == 0 else ""))

# ---- outbox ----
st.subheader("Outbox (the exact payloads that would be delivered)")
ob = engine.store.outbox()
if ob:
    st.dataframe([{"connector": r["connector"], "payload": r["payload_json"], "ts": r["ts"]}
                  for r in ob], use_container_width=True)
else:
    st.info("Outbox is empty — fire an event to populate it.")
