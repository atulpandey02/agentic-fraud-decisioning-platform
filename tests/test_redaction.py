# =============================================================
# SECURITY TESTS — trace PII redaction (Priority 1 item 7 correction)
# =============================================================
# The load-bearing assertion (the audit's requirement): full_name must
# NEVER appear in a persisted tool_output or reasoning_text field.
# Tested both on the redactor directly and through the REAL
# AgentTraceWriter row-building path, so it proves the production path
# redacts — not just a helper in isolation.
# =============================================================

import json

from langchain_core.messages import HumanMessage, AIMessage, ToolMessage

from fraud_platform.observability.redaction import TraceRedactor
from fraud_platform.observability.audit_logger import AgentTraceWriter

# What get_user_history actually returns (json.dumps of the DIM_USERS row).
USER_PROFILE = {
    "full_name": "Jane Q. Public",
    "home_city": "Boston",
    "home_country": "US",
    "risk_tier": "LOW",
    "avg_transaction_amt": 42.5,
    "trusted_device_count": 2,
}
NAME = USER_PROFILE["full_name"]
CITY = USER_PROFILE["home_city"]


class TestTraceRedactor:
    def test_structured_pii_hashed_out(self):
        rows = [{"tool_output": json.dumps({"content": json.dumps(USER_PROFILE)}),
                 "tool_input": None, "reasoning_text": None}]
        out = TraceRedactor().redact_rows(rows)
        blob = out[0]["tool_output"]
        assert NAME not in blob
        assert CITY not in blob
        assert "US" not in json.loads(json.loads(blob)["content"]).get("home_country", "")
        # non-PII fields survive
        assert "LOW" in blob and "42.5" in blob

    def test_reasoning_text_literal_scrubbed(self):
        # an LLM step that quoted the name is caught by pass 2, because
        # pass 1 collected the literal from the structured tool output
        rows = [
            {"tool_output": json.dumps({"content": json.dumps(USER_PROFILE)}),
             "tool_input": None, "reasoning_text": None},
            {"tool_output": None, "tool_input": None,
             "reasoning_text": f"{NAME} in {CITY} is a low-risk regular."},
        ]
        out = TraceRedactor().redact_rows(rows)
        assert NAME not in out[1]["reasoning_text"]
        assert CITY not in out[1]["reasoning_text"]

    def test_same_value_same_pseudonym(self):
        rows = [{"tool_output": json.dumps({"full_name": NAME}), "tool_input": None,
                 "reasoning_text": f"note about {NAME}"},
                {"tool_output": json.dumps({"full_name": NAME}), "tool_input": None,
                 "reasoning_text": None}]
        out = TraceRedactor().redact_rows(rows)
        # correlatable: identical name -> identical token across rows
        tok_a = json.loads(out[0]["tool_output"])["full_name"]
        tok_b = json.loads(out[1]["tool_output"])["full_name"]
        assert tok_a == tok_b and tok_a.startswith("[REDACTED:")
        assert tok_a in out[0]["reasoning_text"]  # scrubbed to the same token

    def test_none_fields_safe(self):
        out = TraceRedactor().redact_rows([{"tool_output": None, "tool_input": None,
                                            "reasoning_text": None}])
        assert out[0]["reasoning_text"] is None

    def test_non_json_text_passes_through_except_literals(self):
        rows = [{"tool_output": json.dumps({"full_name": NAME}), "tool_input": None,
                 "reasoning_text": "plain text mentioning Jane Q. Public here"}]
        out = TraceRedactor().redact_rows(rows)
        assert NAME not in out[0]["reasoning_text"]


class TestAgentTraceWriterRedactsRealPath:
    """Through the actual _row_for_message + redactor path — no DB."""

    def _rows(self):
        writer = AgentTraceWriter()   # opens no connection at init
        messages = [
            HumanMessage(content="Evaluate transaction t1"),
            # what a get_user_history tool call returns
            ToolMessage(content=json.dumps(USER_PROFILE), tool_call_id="c1",
                        name="get_user_history"),
            # a risk-agent reasoning step that happens to quote the name/city
            AIMessage(content=f"{NAME} of {CITY} has a normal spending pattern.",
                      name="risk_agent"),
        ]
        rows = [writer._row_for_message("dec1", i, m) for i, m in enumerate(messages)]
        return writer._redactor.redact_rows(rows)

    def test_full_name_never_in_any_persisted_field(self):
        rows = self._rows()
        for r in rows:
            for field in ("tool_output", "tool_input", "reasoning_text"):
                val = r.get(field)
                if val:
                    assert NAME not in val, f"full_name leaked into {field}: {val}"

    def test_home_city_and_country_not_in_tool_output(self):
        rows = self._rows()
        tool_out = next(r["tool_output"] for r in rows if r.get("tool_output"))
        assert CITY not in tool_out

    def test_join_keys_preserved(self):
        rows = self._rows()
        # decision_id + user/transaction identity survive redaction
        assert all(r["decision_id"] == "dec1" for r in rows)
