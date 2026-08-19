# =============================================================
# UNIT TESTS — deterministic promotion + NL2SQL reuse + template replay
# =============================================================
# Covers required cases: (1) the one-time NL2SQL path is REUSED, not
# duplicated; (2)/(3) a query result can be delivered to Slack / email;
# (6) a recurring report REPLAYS stored validated SQL rather than
# regenerating it. No Snowflake / Groq — the NL2SQL agent and the
# report runner are stubbed.
# =============================================================

import fraud_platform.workflow_engine.reports_catalog as catalog
import fraud_platform.workflow_engine.tools_bridge as tb
from fraud_platform.workflow_engine.executor import Executor
from fraud_platform.workflow_engine.feasibility import check_plan
from fraud_platform.workflow_engine.promotion import Destination, build_report_plan
from fraud_platform.workflow_engine.registry import ToolRegistry
from fraud_platform.workflow_engine.state import WorkflowState, WorkflowStore


# ---------------------------------------------------------------- (1) NL2SQL reuse
class _FakeGenerated:
    sql = "SELECT decision, COUNT(*) AS n FROM DECISIONS.FACT_DECISIONS GROUP BY decision"


class _FakeNL2SQLAgent:
    """Records that the EXISTING BI agent was the thing called — the
    workflow engine must reuse it, not grow its own SQL author."""
    called = 0

    def __init__(self):
        type(self).called += 1

    def ask(self, question):
        return _FakeGenerated(), ["decision", "n"], [("BLOCK", 3), ("ALLOW", 9)]


class TestNL2SQLReuse:
    def test_query_decisions_reuses_the_bi_agent_unchanged(self, monkeypatch):
        _FakeNL2SQLAgent.called = 0
        monkeypatch.setattr("fraud_platform.bi_dashboard.nl2sql_agent.NL2SQLAgent",
                            _FakeNL2SQLAgent)
        out = tb._query_decisions("how many decisions per type?")
        # exactly the same shape mode A returns: validated sql + columns + rows
        assert out["sql"].startswith("SELECT decision")
        assert out["columns"] == ["decision", "n"]
        assert out["rows"][0] == ("BLOCK", 3)
        assert _FakeNL2SQLAgent.called == 1     # the BI agent did the work, not a new one


# ---------------------------------------------------------------- (2)/(3) delivery
def _deliver_fixture():
    """A registry whose run_report_query returns canned rows, with the REAL
    format_report + real connectors, so a promotion plan can be executed end
    to end into the outbox with no infra."""
    from pydantic import BaseModel, Field

    from fraud_platform.workflow_engine.connectors import make_email_send, make_slack_send
    from fraud_platform.workflow_engine.registry import ANALYZE, NOTIFY, READ_DATA, ToolSpec
    from fraud_platform.workflow_engine.report import format_report

    class _R(BaseModel):
        report: str = Field(description="x")
        window_start: str = Field(default=None, description="x")
        window_end: str = Field(default=None, description="x")

    class _F(BaseModel):
        title: str = Field(description="x")
        data: dict = Field(description="x")

    class _S(BaseModel):
        channel: str = Field(description="x")
        text: str = Field(description="x")

    class _E(BaseModel):
        to: str = Field(description="x")
        subject: str = Field(description="x")
        body: str = Field(description="x")

    store = WorkflowStore(db_path=":memory:")
    r = ToolRegistry()
    r.register(ToolSpec("run_report_query", "run", _R, READ_DATA,
                        execute=lambda report, window_start=None, window_end=None: {
                            "columns": ["block_count"], "rows": [[102]], "sql": "SELECT 1"}))
    r.register(ToolSpec("format_report", "fmt", _F, ANALYZE, execute=format_report))
    r.register(ToolSpec("slack_send_message", "slack", _S, NOTIFY,
                        execute=make_slack_send(store.outbox_writer()), read_only=False))
    r.register(ToolSpec("email_send", "email", _E, NOTIFY,
                        execute=make_email_send(store.outbox_writer()), read_only=False))
    return store, Executor(r, store), r


def _ready(store, plan):
    wid = store.create_workflow("u", "promoted")
    store.transition(wid, WorkflowState.PLANNED)
    store.set_plan(wid, plan.model_dump_json())
    store.transition(wid, WorkflowState.FEASIBLE)
    store.transition(wid, WorkflowState.READY)
    return wid


class TestDelivery:
    def test_result_can_be_sent_to_slack(self):
        store, ex, reg = _deliver_fixture()
        plan = build_report_plan("Report", "tid", Destination("slack", channel="#fraud-ops"),
                                 windowed=False)
        assert check_plan(plan, reg).ok
        result = ex.execute(_ready(store, plan), plan)
        assert result.status == "COMPLETED"
        ob = store.outbox()
        assert ob[0]["connector"] == "slack"
        assert "Block count: 102" in ob[0]["payload_json"]      # formatted, not raw
        store.close()

    def test_result_can_be_sent_to_email(self):
        store, ex, reg = _deliver_fixture()
        plan = build_report_plan("Report", "tid",
                                 Destination("email", to="ops@x.com", subject="Report"),
                                 windowed=False)
        result = ex.execute(_ready(store, plan), plan)
        assert result.status == "COMPLETED"
        ob = store.outbox()
        assert ob[0]["connector"] == "email"
        assert '"to": "ops@x.com"' in ob[0]["payload_json"]
        assert "SQL used:" in ob[0]["payload_json"]             # detailed email body
        store.close()

    def test_plan_shape_slack_vs_email_and_windowing(self):
        slack = build_report_plan("R", "fraud_performance", Destination("slack", channel="#x"),
                                  windowed=True)
        assert slack.steps[0].args["window_start"] == "$trigger.window_start"
        assert slack.steps[2].tool_name == "slack_send_message"
        email = build_report_plan("R", "tid", Destination("email", to="a@b.com"), windowed=False)
        assert "window_start" not in email.steps[0].args      # one-shot, no window
        assert email.steps[2].tool_name == "email_send"


# ---------------------------------------------------------------- (6) template replay
class TestTemplateReplay:
    def test_named_catalog_report_replays_catalog_sql(self, monkeypatch):
        seen = {}
        monkeypatch.setattr(catalog, "run_validated_report",
                            lambda sql, ws=None, we=None, **k: seen.update(sql=sql, ws=ws) or {})
        tool = tb.make_run_report_query(template_resolver=None)
        tool("fraud_performance", window_start="A", window_end="B")
        assert seen["sql"] is catalog.FRAUD_PERFORMANCE_REPORT   # catalog SQL, not regenerated
        assert seen["ws"] == "A"

    def test_stored_template_is_replayed_by_id_without_regenerating(self, monkeypatch):
        # THE reuse test: a recurring report resolves a STORED template id to
        # its saved SQL and replays it — no LLM, no new SQL authored.
        store = WorkflowStore(db_path=":memory:")
        tid = store.save_template("SELECT COUNT(*) FROM DECISIONS.FACT_DECISIONS", title="daily")
        seen = {}
        monkeypatch.setattr(catalog, "run_validated_report",
                            lambda sql, ws=None, we=None, **k: seen.update(sql=sql) or {})
        tool = tb.make_run_report_query(
            template_resolver=lambda t: (store.get_template(t) or {}).get("sql"))
        tool(tid, window_start="A", window_end="B")
        assert seen["sql"] == "SELECT COUNT(*) FROM DECISIONS.FACT_DECISIONS"
        store.close()

    def test_unknown_report_is_rejected(self):
        tool = tb.make_run_report_query(template_resolver=lambda t: None)
        import pytest
        with pytest.raises(KeyError):
            tool("no_such_report", window_start="A", window_end="B")
