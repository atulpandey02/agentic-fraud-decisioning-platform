# =============================================================
# UNIT TESTS — workflow tool registry + connectors (no LLM/infra)
# =============================================================
# Registry mechanics and the outbox connectors are pure once the
# outbox writer is injected, so this whole file runs with no Redis,
# Snowflake, Weaviate, or Groq. The bridged data tools are NOT
# executed here (that would need live infra); we test the registry
# SHAPE and dispatch guards, and exercise the connectors with a
# fake outbox.
# =============================================================

import pytest
from pydantic import BaseModel, Field, ValidationError

from fraud_platform.workflow_engine import connectors
from fraud_platform.workflow_engine.registry import (
    ANALYZE, NOTIFY, READ_DATA, ToolRegistry, ToolSpec,
)
from fraud_platform.workflow_engine.tools_bridge import build_default_registry


class _Args(BaseModel):
    x: int = Field(description="an int")


def _spec(name="echo", desc="echo the input value back", category=ANALYZE):
    return ToolSpec(name=name, description=desc, args_schema=_Args,
                    category=category, execute=lambda x: {"echoed": x})


class TestRegistry:
    def test_register_get_and_duplicate(self):
        r = ToolRegistry()
        r.register(_spec())
        assert r.get("echo").name == "echo"
        assert r.get("missing") is None
        with pytest.raises(ValueError):
            r.register(_spec())  # duplicate name

    def test_execute_validates_args_and_dispatches(self):
        r = ToolRegistry()
        r.register(_spec())
        assert r.execute("echo", {"x": 7}) == {"echoed": 7}
        with pytest.raises(ValidationError):
            r.execute("echo", {"x": "not-an-int"})

    def test_unknown_tool_is_refused_not_guessed(self):
        r = ToolRegistry()
        with pytest.raises(KeyError):
            r.execute("does_not_exist", {})

    def test_search_ranks_by_term_hits(self):
        r = ToolRegistry()
        r.register(_spec(name="count_blocks", desc="count block decisions"))
        r.register(_spec(name="send_slack", desc="send a slack notification"))
        hits = r.search("count block")
        assert hits and hits[0].name == "count_blocks"
        # empty query returns everything
        assert len(r.search("")) == 2


class TestConnectors:
    def test_slack_writes_outbox_and_does_not_require_webhook(self):
        outbox = []
        slack = connectors.make_slack_send(lambda c, p: outbox.append((c, p)), webhook_url=None)
        result = slack("#fraud-ops", "hello")
        assert outbox == [("slack", {"channel": "#fraud-ops", "text": "hello"})]
        assert result["outbox_written"] is True and result["delivered"] is False

    def test_email_writes_outbox_mock_only(self):
        outbox = []
        email = connectors.make_email_send(lambda c, p: outbox.append((c, p)))
        result = email("ops@example.com", "subj", "body")
        assert outbox[0][0] == "email"
        assert result["delivered"] is False


class TestDefaultRegistry:
    def test_default_registry_shape(self):
        outbox = []
        r = build_default_registry(lambda c, p: outbox.append((c, p)))
        names = r.names()
        assert names == {
            "get_transaction_features", "get_user_history", "query_decisions",
            "count_recent_decisions", "run_report_query", "format_report",
            "slack_send_message", "email_send",
        }
        # categories are correct and within the allowed set
        assert r.get("count_recent_decisions").category == ANALYZE
        assert r.get("run_report_query").category == READ_DATA
        assert r.get("format_report").category == ANALYZE
        assert r.get("get_user_history").category == READ_DATA
        assert r.get("slack_send_message").category == NOTIFY
        # NOTIFY connectors are not read-only; none require approval by default
        assert r.get("slack_send_message").read_only is False
        assert all(not s.requires_approval for s in r.all())

    def test_prompt_listing_names_every_tool(self):
        r = build_default_registry(lambda c, p: None)
        listing = r.listing_for_prompt()
        for name in r.names():
            assert name in listing
