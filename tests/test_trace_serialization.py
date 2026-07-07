# =============================================================
# UNIT TESTS — agent trace serialization (Priority 4)
# =============================================================
# AgentTraceWriter maps a run's messages -> FACT_AGENT_TRACES rows.
# __init__ opens no connection (lazy), so the mapping is testable
# with no Snowflake — only the message->row logic is exercised.
# =============================================================

import json

from langchain_core.messages import HumanMessage, AIMessage, ToolMessage

from fraud_platform.observability.audit_logger import AgentTraceWriter


W = AgentTraceWriter()


class TestClassifyStep:
    def test_human_is_input(self):
        assert W._classify_step(HumanMessage(content="evaluate this")) == "INPUT"

    def test_tool_message_is_tool_output(self):
        assert W._classify_step(ToolMessage(content="{}", tool_call_id="x")) == "TOOL_OUTPUT"

    def test_ai_with_tool_calls_is_tool_call(self):
        msg = AIMessage(content="", tool_calls=[
            {"name": "get_transaction_features", "args": {"user_id": "u1"}, "id": "c1"}
        ])
        assert W._classify_step(msg) == "TOOL_CALL"

    def test_orchestrator_named_ai_is_handoff(self):
        assert W._classify_step(AIMessage(content="routing", name="orchestrator")) == "HANDOFF"

    def test_other_named_ai_is_reasoning(self):
        assert W._classify_step(AIMessage(content="findings", name="feature_agent")) == "REASONING"

    def test_plain_ai_is_reasoning(self):
        assert W._classify_step(AIMessage(content="thinking")) == "REASONING"


class TestRowForMessage:
    def test_input_row_labels_user(self):
        row = W._row_for_message("dec1", 0, HumanMessage(content="evaluate txn"))
        assert row["step_type"] == "INPUT"
        assert row["agent_name"] == "user"
        assert row["reasoning_text"] == "evaluate txn"
        assert row["decision_id"] == "dec1"
        assert row["step_number"] == 0

    def test_handoff_row_keeps_orchestrator_name(self):
        row = W._row_for_message("dec1", 1, AIMessage(content="route to policy", name="orchestrator"))
        assert row["step_type"] == "HANDOFF"
        assert row["agent_name"] == "orchestrator"
        assert row["reasoning_text"] == "route to policy"

    def test_tool_call_row_serializes_calls_to_json(self):
        msg = AIMessage(content="", tool_calls=[
            {"name": "get_user_history", "args": {"user_id": "u9"}, "id": "c1"}
        ])
        row = W._row_for_message("dec1", 2, msg)
        assert row["step_type"] == "TOOL_CALL"
        assert row["tool_name"] == "get_user_history"
        parsed = json.loads(row["tool_input"])
        assert parsed[0]["name"] == "get_user_history"
        assert parsed[0]["args"] == {"user_id": "u9"}

    def test_multiple_tool_calls_joined_in_one_row(self):
        msg = AIMessage(content="", tool_calls=[
            {"name": "a", "args": {}, "id": "1"},
            {"name": "b", "args": {}, "id": "2"},
        ])
        row = W._row_for_message("dec1", 3, msg)
        assert row["tool_name"] == "a, b"
        assert len(json.loads(row["tool_input"])) == 2

    def test_tool_output_row_serializes_content(self):
        row = W._row_for_message("dec1", 4, ToolMessage(content="result-data", tool_call_id="x", name="get_user_history"))
        assert row["step_type"] == "TOOL_OUTPUT"
        assert json.loads(row["tool_output"])["content"] == "result-data"

    def test_unnamed_ai_falls_back_to_single_agent(self):
        # Phase 3 single-agent messages carry no name tag
        row = W._row_for_message("dec1", 5, AIMessage(content="decided"))
        assert row["agent_name"] == "single_agent"

    def test_every_row_has_unique_trace_id(self):
        msgs = [HumanMessage(content="q"), AIMessage(content="a", name="feature_agent")]
        ids = {W._row_for_message("d", i, m)["trace_id"] for i, m in enumerate(msgs)}
        assert len(ids) == 2
