# =============================================================
# STREAMLIT APP — ask the decision audit log questions in English
# =============================================================
# Run:  streamlit run fraud_platform/bi_dashboard/streamlit_app.py
#       (with the package installed: `pip install -e .[bi]`)
#
# Imports are ABSOLUTE, not package-relative, on purpose: `streamlit
# run` executes this file as a top-level script, not as a member of
# its package, so `from .nl2sql_agent import ...` would raise. The
# installed `fraud_platform` package makes the absolute paths resolve.
#
# Thin by design: every piece of intelligence lives in the two
# classes it imports (NL2SQLAgent generates + guards + executes,
# ChartRenderer decides visualization) so they stay testable
# without a browser, and this file stays swappable for any other
# front end. The one UI-policy decision made HERE: the generated
# SQL is always shown, expanded, next to its results. An analyst
# who can't see the SQL can't catch a subtly-wrong query, and
# "trust me" is exactly the wrong posture for a tool that writes
# its own queries.
# =============================================================

import json
import logging

import streamlit as st

from fraud_platform.bi_dashboard.nl2sql_agent import NL2SQLAgent, QueryRejected
from fraud_platform.bi_dashboard.chart_renderer import ChartRenderer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")

st.set_page_config(page_title="Fraud Decisioning BI", page_icon="🛡️", layout="wide")


# st.cache_resource: one agent (one Snowflake connection, one LLM
# client) per server process, reused across reruns — Streamlit
# re-executes this whole script on every interaction, so without
# the cache every button click would open a fresh connection.
@st.cache_resource
def get_agent() -> NL2SQLAgent:
    return NL2SQLAgent()


@st.cache_resource
def get_engine():
    """The workflow engine, in-process and shared across reruns. Its
    scheduler is STARTED here so a report scheduled from this page
    actually fires while the app runs (idempotency makes it safe even
    if the API process is also running a scheduler on the same DB)."""
    from fraud_platform.workflow_engine.api import build_engine
    engine = build_engine(mock=False)
    engine.scheduler.start()
    return engine


renderer = ChartRenderer()

# Common IANA zones for the schedule picker. A schedule REQUIRES an
# explicit zone (code rejects a missing one) — the UI never guesses.
_TIMEZONES = ["America/New_York", "America/Chicago", "America/Denver",
              "America/Los_Angeles", "UTC", "Europe/London", "Asia/Kolkata"]

st.title("🛡️ Fraud Decisioning — Agentic BI")
st.caption(
    "Ask questions about the platform's decisions in plain English. "
    "An LLM writes guarded, read-only SQL against DECISIONS.FACT_DECISIONS "
    "and FEATURES.FACT_FEATURE_SNAPSHOTS — the SQL is always shown."
)

SAMPLE_QUESTIONS = [
    "How many decisions per decision type?",
    "What is the average confidence by governance tier?",
    "Show decisions over time by day",
    "What share of flagged transactions were actually fraud, by fraud pattern?",
    "What is the average judge score and latency per decision type?",
]

with st.sidebar:
    st.header("Sample questions")
    for q in SAMPLE_QUESTIONS:
        if st.button(q, use_container_width=True):
            st.session_state["question"] = q

question = st.text_input(
    "Your question",
    value=st.session_state.get("question", ""),
    placeholder="e.g. How many blocks did the agent issue at high confidence?",
)

def _render_result(res: dict) -> None:
    """Render a stored NL2SQL result (SQL always shown), then the chart/table."""
    st.markdown(f"**What this computes:** {res['explanation']}")
    with st.expander("Generated SQL (always shown — verify before trusting)", expanded=True):
        st.code(res["sql"], language="sql")
    df = renderer.build_dataframe(res["columns"], res["rows"])
    if df.empty:
        st.info("The query ran but returned no rows.")
    elif df.shape == (1, 1):
        st.metric(label=df.columns[0].replace("_", " "), value=str(df.iloc[0, 0]))
    else:
        fig = renderer.render(df, res.get("chart_hint", "table"))
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True)


def _deliver_now(res: dict, dest, title: str) -> None:
    """MODE B — build a deliver plan from the validated SQL and run it now.
    Reuses the SAME feasibility + executor + connectors as any workflow."""
    from fraud_platform.workflow_engine.promotion import build_report_plan
    from fraud_platform.workflow_engine.feasibility import check_plan
    from fraud_platform.workflow_engine.state import WorkflowState

    engine = get_engine()
    safe_sql = get_agent().validate_sql(res["sql"])          # re-validate -> canonical
    tid = engine.store.save_template(safe_sql, title=title)
    plan = build_report_plan(title, tid, dest, windowed=False)
    report = check_plan(plan, engine.registry)
    st.markdown("**Generated workflow** (shown before it runs):")
    _show_plan(plan)
    if not report.ok:
        st.error("Feasibility: REJECTED — " + "; ".join(report.errors))
        return
    st.success("Feasibility: PASS")
    wid = engine.store.create_workflow("bi-user", f"[deliver] {title}")
    engine.store.transition(wid, WorkflowState.PLANNED)
    engine.store.set_plan(wid, plan.model_dump_json())
    engine.store.transition(wid, WorkflowState.FEASIBLE)
    engine.store.transition(wid, WorkflowState.READY)
    with st.spinner("Running the workflow..."):
        result = engine.executor.execute(wid, plan)
    if result.status == "COMPLETED":
        st.success(f"Delivered via {dest.connector}.")
    else:
        st.error(f"Run {result.status}: {result.error}")
    st.caption("Outbox — the exact payload that was delivered:")
    st.dataframe([{"connector": r["connector"], "payload": r["payload_json"]}
                  for r in engine.store.outbox()[:5]], use_container_width=True)


def _show_plan(plan) -> None:
    if plan.schedule is not None:
        from fraud_platform.workflow_engine.schedule import Schedule
        try:
            st.markdown(f"**Trigger:** {Schedule(**plan.schedule.model_dump()).describe()}")
        except Exception:
            st.markdown("**Trigger:** (schedule invalid)")
    for s in plan.steps:
        st.markdown(f"{s.step_number}. `{s.tool_name}` — {s.rationale}")
        st.code(json.dumps(s.args), language="json")


def _activate_schedule(res: dict, schedule, dest, title: str) -> None:
    """MODE C — persist a recurring report and register it with the scheduler.
    Only called after the user has SEEN the plan and clicked Activate."""
    from fraud_platform.workflow_engine.promotion import build_report_plan
    from fraud_platform.workflow_engine.planner import PlannedSchedule
    from fraud_platform.workflow_engine.feasibility import check_plan
    from fraud_platform.workflow_engine.state import WorkflowState

    engine = get_engine()
    safe_sql = get_agent().validate_sql(res["sql"])
    tid = engine.store.save_template(safe_sql, title=title)
    plan = build_report_plan(title, tid, dest, windowed=False,
                             schedule=PlannedSchedule(**schedule.model_dump()))
    report = check_plan(plan, engine.registry)
    if not report.ok:
        st.error("Feasibility: REJECTED — " + "; ".join(report.errors))
        return
    wid = engine.store.create_workflow("bi-user", f"[scheduled] {title}")
    engine.store.transition(wid, WorkflowState.PLANNED)
    engine.store.set_plan(wid, plan.model_dump_json())
    engine.store.set_schedule(wid, schedule.model_dump_json(),
                              destination_json=json.dumps(dest.to_json()), template_id=tid)
    engine.store.transition(wid, WorkflowState.FEASIBLE)
    engine.store.transition(wid, WorkflowState.READY)
    next_run = engine.scheduler.schedule_workflow(wid, schedule)
    st.success(f"Scheduled ✓  ·  {schedule.describe()}")
    st.caption(f"Workflow `{wid[:8]}` is READY. Next run: {next_run}")


def _render_actions(res: dict) -> None:
    """The Slack / Email / Schedule actions offered after a result. Nothing
    is created silently — schedules show the plan + feasibility before an
    explicit Activate."""
    from fraud_platform.workflow_engine.promotion import Destination

    st.divider()
    st.subheader("Act on this result")
    title = st.text_input("Report title", value="Fraud Operations Report")
    send_tab, email_tab, sched_tab = st.tabs(
        ["📤 Send to Slack", "✉️ Email report", "🗓️ Schedule this report"])

    with send_tab:
        channel = st.text_input("Slack channel", value="#fraud-ops", key="slack_ch")
        if st.button("Send to Slack now", type="primary", key="do_slack"):
            _deliver_now(res, Destination("slack", channel=channel), title)

    with email_tab:
        to = st.text_input("Recipient email", value="fraud-ops@example.com", key="email_to")
        if st.button("Email report now", type="primary", key="do_email"):
            _deliver_now(res, Destination("email", to=to, subject=title), title)

    with sched_tab:
        from fraud_platform.workflow_engine.schedule import Schedule
        c1, c2, c3 = st.columns(3)
        freq = c1.selectbox("Frequency", ["daily", "hourly", "weekly"], key="sc_freq")
        hour = c2.number_input("Hour (0-23)", 0, 23, 22, key="sc_hour")
        minute = c3.number_input("Minute", 0, 59, 0, key="sc_min")
        tz = st.selectbox("Timezone (required — no default is assumed)", _TIMEZONES, key="sc_tz")
        dow = None
        if freq == "weekly":
            dow = st.selectbox("Day of week",
                               ["mon", "tue", "wed", "thu", "fri", "sat", "sun"], key="sc_dow")
        d_kind = st.radio("Deliver to", ["Slack", "Email"], horizontal=True, key="sc_dest")
        if d_kind == "Slack":
            dest_val = st.text_input("Slack channel", value="#fraud-ops", key="sc_ch")
        else:
            dest_val = st.text_input("Recipient email", value="fraud-ops@example.com", key="sc_email")

        if st.button("Preview workflow", key="sc_preview"):
            try:
                sched = Schedule(frequency=freq, hour=int(hour), minute=int(minute),
                                 day_of_week=dow, timezone=tz)
                dest = (Destination("slack", channel=dest_val) if d_kind == "Slack"
                        else Destination("email", to=dest_val, subject=title))
                st.session_state["sched_preview"] = {
                    "schedule": sched.model_dump(), "dest": dest.to_json(), "title": title}
            except Exception as e:  # noqa: BLE001 — surfaced to the user
                st.session_state.pop("sched_preview", None)
                st.error(f"Invalid schedule: {e}")

        prev = st.session_state.get("sched_preview")
        if prev:
            from fraud_platform.workflow_engine.promotion import build_report_plan
            from fraud_platform.workflow_engine.planner import PlannedSchedule
            from fraud_platform.workflow_engine.feasibility import check_plan
            sched = Schedule(**prev["schedule"])
            dest = Destination(prev["dest"]["connector"], channel=prev["dest"].get("channel"),
                               to=prev["dest"].get("to"), subject=prev["dest"].get("subject"))
            plan = build_report_plan(prev["title"], "(validated stored query)", dest,
                                     windowed=False, schedule=PlannedSchedule(**sched.model_dump()))
            report = check_plan(plan, get_engine().registry)
            st.markdown(f"### {prev['title'].upper()}")
            _show_plan(plan)
            st.success("Feasibility: PASS" if report.ok
                       else "Feasibility: REJECTED — " + "; ".join(report.errors))
            if report.ok and st.button("✅ Activate", type="primary", key="sc_activate"):
                _activate_schedule(res, sched, dest, prev["title"])
                st.session_state.pop("sched_preview", None)


if st.button("Ask", type="primary") and question.strip():
    agent = get_agent()
    with st.spinner("Generating and validating SQL..."):
        try:
            generated, columns, rows = agent.ask(question.strip())
        except QueryRejected as e:
            # Guardrail rejections are shown as policy, not as bugs —
            # the agent tried something the platform does not permit.
            st.warning(f"Query rejected by guardrails: {e}")
            st.stop()
        except Exception as e:
            st.error(f"Query execution failed: {e}")
            st.stop()
    # Persist so the result (and its actions) survive the reruns that every
    # button click triggers; drop any stale schedule preview.
    st.session_state["last_result"] = {
        "question": question.strip(), "sql": generated.sql,
        "explanation": generated.explanation, "chart_hint": generated.chart_hint,
        "columns": columns, "rows": rows,
    }
    st.session_state.pop("sched_preview", None)

# Render the current result and its actions (from session, so they persist).
_result = st.session_state.get("last_result")
if _result:
    _render_result(_result)
    _render_actions(_result)
