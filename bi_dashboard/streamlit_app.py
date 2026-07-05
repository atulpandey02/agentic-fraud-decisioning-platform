# =============================================================
# STREAMLIT APP — ask the decision audit log questions in English
# =============================================================
# Run from this folder:  streamlit run streamlit_app.py
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

import logging

import streamlit as st

from nl2sql_agent import NL2SQLAgent, QueryRejected
from chart_renderer import ChartRenderer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")

st.set_page_config(page_title="Fraud Decisioning BI", page_icon="🛡️", layout="wide")


# st.cache_resource: one agent (one Snowflake connection, one LLM
# client) per server process, reused across reruns — Streamlit
# re-executes this whole script on every interaction, so without
# the cache every button click would open a fresh connection.
@st.cache_resource
def get_agent() -> NL2SQLAgent:
    return NL2SQLAgent()


renderer = ChartRenderer()

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

    st.markdown(f"**What this computes:** {generated.explanation}")
    with st.expander("Generated SQL (always shown — verify before trusting)", expanded=True):
        st.code(generated.sql, language="sql")

    df = renderer.build_dataframe(columns, rows)
    if df.empty:
        st.info("The query ran but returned no rows.")
    elif df.shape == (1, 1):
        # single number -> a metric, the most honest "chart" there is
        st.metric(label=df.columns[0].replace("_", " "), value=str(df.iloc[0, 0]))
    else:
        fig = renderer.render(df, generated.chart_hint)
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True)
