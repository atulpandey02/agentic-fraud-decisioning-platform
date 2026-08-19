# =============================================================
# UNIT TESTS — ReportResult formatting + the named report catalog
# =============================================================
# No Snowflake, no LLM: the catalog's execution is exercised with a
# fake connection, and the AST guard is the real one (so a destructive
# template is genuinely rejected in code).
# =============================================================

import pytest

from fraud_platform.bi_dashboard.sql_guard import QueryRejected, SQLValidator
from fraud_platform.workflow_engine.report import (
    build_report, format_report, render_email, render_slack,
)
from fraud_platform.workflow_engine.reports_catalog import (
    FRAUD_PERFORMANCE_REPORT, MissingParameter, REPORT_CATALOG, run_validated_report,
)


def _validator():
    return SQLValidator(
        allowed_tables=["DECISIONS.FACT_DECISIONS", "FEATURES.FACT_FEATURE_SNAPSHOTS"],
        max_rows=200,
    )


# ---- fake Snowflake connection (records the SQL it was asked to run) ----
class _FakeCursor:
    def __init__(self, columns, rows, sink):
        self._columns, self._rows, self._sink = columns, rows, sink
        self.description = [(c,) for c in columns]

    def execute(self, sql, *a, **k):
        self._sink.append(sql)

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConn:
    def __init__(self, columns, rows, sink):
        self._c = _FakeCursor(columns, rows, sink)

    def cursor(self):
        return self._c

    def close(self):
        pass


class TestReportFormatting:
    def test_single_row_becomes_labeled_metrics(self):
        data = {
            "columns": ["transactions_reviewed", "block_count", "escalate_count",
                        "fraud_rate", "top_fraud_pattern"],
            "rows": [[4820, 102, 25, 0.0263, "VELOCITY_SPIKE"]],
            "sql": "SELECT ...",
        }
        r = build_report("Daily Fraud Operations Report", data)
        assert r.metrics["transactions_reviewed"] == 4820
        # thousands separators + rate rendered as a percentage
        assert "Transactions reviewed: 4,820" in r.summary
        assert "Fraud rate: 2.63%" in r.summary
        assert r.sql_used == "SELECT ..."
        assert r.row_count == 1

    def test_format_report_prerenders_both_delivery_forms(self):
        data = {"columns": ["block_count"], "rows": [[102]], "sql": "SELECT 1"}
        out = format_report("Report", data)
        assert out["slack_text"].startswith("*Report*")
        assert out["email_subject"] == "Report"
        assert "SQL used:" in out["email_body"]
        assert out["metrics"] == {"block_count": 102}

    def test_multi_row_summarizes_with_table_in_email(self):
        data = {"columns": ["pattern", "n"], "rows": [["GEO_JUMP", 5], ["NEW_DEVICE", 3]],
                "sql": "SELECT ..."}
        r = build_report("Patterns", data)
        assert r.metrics == {"row_count": 2}
        _, body = render_email(r)
        assert "pattern | n" in body and "GEO_JUMP | 5" in body

    def test_none_values_render_as_dash(self):
        r = build_report("R", {"columns": ["top_fraud_pattern"], "rows": [[None]], "sql": "s"})
        assert "—" in render_slack(r)


class TestNamedReportCatalog:
    def test_fraud_performance_is_registered(self):
        assert "fraud_performance" in REPORT_CATALOG
        title, sql = REPORT_CATALOG["fraud_performance"]
        assert title == "Daily Fraud Operations Report"
        assert sql is FRAUD_PERFORMANCE_REPORT

    def test_template_passes_the_real_ast_guard(self):
        # The parameterized template (with :placeholders) must itself be
        # a legal SELECT over allowlisted tables.
        assert _validator().validate(FRAUD_PERFORMANCE_REPORT)

    def test_run_substitutes_window_and_revalidates_and_executes(self):
        sink: list[str] = []
        conn = _FakeConn(
            columns=["transactions_reviewed", "block_count", "escalate_count",
                     "allow_count", "fraud_rate", "top_fraud_pattern"],
            rows=[[4820, 102, 25, 4693, 0.0263, "VELOCITY_SPIKE"]],
            sink=sink,
        )
        out = run_validated_report(
            FRAUD_PERFORMANCE_REPORT,
            window_start="2026-08-18T00:00:00",
            window_end="2026-08-19T00:00:00",
            validator=_validator(),
            connect=lambda: conn,
        )
        # the executed SQL had the placeholders replaced with code-generated
        # datetime literals (the guard's re-serialization normalizes the
        # TO_TIMESTAMP_NTZ() call to a CAST, which is exactly the point —
        # the FINAL SQL went back through the parser).
        executed = sink[0]
        assert ":window_start" not in executed and ":window_end" not in executed
        assert "2026-08-18T00:00:00" in executed and "2026-08-19T00:00:00" in executed
        assert "TIMESTAMP" in executed.upper()
        # ...and a LIMIT was enforced by the guard on the final SQL
        assert "LIMIT" in executed.upper()
        assert out["columns"][0] == "transactions_reviewed"
        assert out["rows"][0][0] == 4820

    def test_missing_window_parameter_is_rejected(self):
        with pytest.raises(MissingParameter):
            run_validated_report(FRAUD_PERFORMANCE_REPORT, window_start=None,
                                 window_end="2026-08-19T00:00:00", validator=_validator(),
                                 connect=lambda: _FakeConn([], [], []))

    def test_junk_window_parameter_is_rejected_not_injected(self):
        # A non-datetime window must raise, never reach the SQL — the
        # substitution surface only ever accepts real datetimes.
        with pytest.raises(MissingParameter):
            run_validated_report(FRAUD_PERFORMANCE_REPORT,
                                 window_start="'; DROP TABLE FACT_DECISIONS; --",
                                 window_end="2026-08-19T00:00:00", validator=_validator(),
                                 connect=lambda: _FakeConn([], [], []))

    def test_destructive_template_is_rejected_by_the_guard(self):
        # If a bad template ever reached run_validated_report, the guard
        # rejects it before any substitution or execution.
        with pytest.raises(QueryRejected):
            run_validated_report("DELETE FROM DECISIONS.FACT_DECISIONS",
                                 window_start="2026-08-18T00:00:00",
                                 window_end="2026-08-19T00:00:00", validator=_validator(),
                                 connect=lambda: _FakeConn([], [], []))
