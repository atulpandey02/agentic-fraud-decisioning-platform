# =============================================================
# SECURITY TESTS — AST-based NL2SQL validator (Priority 1)
# =============================================================
# Written FIRST, before the implementation. Every case here is a
# KNOWN BYPASS class for a text/regex SQL guard — the reason the
# regex validator is being replaced. The validator is pure (no
# Groq, no Snowflake), so these run in CI with no credentials.
#
# Trust model under test: the LLM is an UNTRUSTED SQL author.
# The validator is the load-bearing security boundary; the LLM
# prompt is only the paved road. Each test asserts the boundary
# holds even when the "author" is hostile.
# =============================================================


import pytest

from fraud_platform.bi_dashboard.sql_guard import SQLValidator, QueryRejected  # noqa: E402


ALLOWED = ["DECISIONS.FACT_DECISIONS", "FEATURES.FACT_FEATURE_SNAPSHOTS"]
MAX_ROWS = 200


@pytest.fixture
def validator():
    return SQLValidator(allowed_tables=ALLOWED, max_rows=MAX_ROWS)


def _rejects(validator, sql):
    with pytest.raises(QueryRejected):
        validator.validate(sql)


# -------------------------------------------------------------
# Statement type — only SELECT / WITH / set-ops
# -------------------------------------------------------------
class TestStatementType:
    @pytest.mark.parametrize("sql", [
        "UPDATE DECISIONS.FACT_DECISIONS SET decision='ALLOW'",
        "DELETE FROM DECISIONS.FACT_DECISIONS",
        "INSERT INTO DECISIONS.FACT_DECISIONS (decision_id) VALUES ('x')",
        "MERGE INTO DECISIONS.FACT_DECISIONS USING FEATURES.FACT_FEATURE_SNAPSHOTS ON TRUE WHEN MATCHED THEN DELETE",
        "DROP TABLE DECISIONS.FACT_DECISIONS",
        "ALTER TABLE DECISIONS.FACT_DECISIONS ADD COLUMN x INT",
        "CREATE TABLE evil AS SELECT * FROM DECISIONS.FACT_DECISIONS",
        "TRUNCATE TABLE DECISIONS.FACT_DECISIONS",
        "GRANT SELECT ON DECISIONS.FACT_DECISIONS TO ROLE PUBLIC",
        "CALL some_proc()",
    ])
    def test_non_select_rejected(self, validator, sql):
        _rejects(validator, sql)

    def test_plain_select_ok(self, validator):
        out = validator.validate("SELECT decision FROM DECISIONS.FACT_DECISIONS")
        assert "FACT_DECISIONS" in out.upper()

    def test_cte_select_ok(self, validator):
        out = validator.validate(
            "WITH x AS (SELECT decision FROM DECISIONS.FACT_DECISIONS) SELECT * FROM x"
        )
        assert "FACT_DECISIONS" in out.upper()

    def test_union_of_allowed_ok(self, validator):
        out = validator.validate(
            "SELECT decision FROM DECISIONS.FACT_DECISIONS "
            "UNION SELECT NULL FROM FEATURES.FACT_FEATURE_SNAPSHOTS"
        )
        assert "UNION" in out.upper()


# -------------------------------------------------------------
# Multi-statement injection
# -------------------------------------------------------------
class TestMultiStatement:
    def test_two_selects_rejected(self, validator):
        _rejects(validator, "SELECT 1 FROM DECISIONS.FACT_DECISIONS; SELECT 2 FROM DECISIONS.FACT_DECISIONS")

    def test_select_then_ddl_rejected(self, validator):
        _rejects(validator, "SELECT decision FROM DECISIONS.FACT_DECISIONS; DROP TABLE DECISIONS.FACT_DECISIONS")

    def test_trailing_semicolon_ok(self, validator):
        out = validator.validate("SELECT decision FROM DECISIONS.FACT_DECISIONS;")
        assert "FACT_DECISIONS" in out.upper()

    def test_comment_hidden_second_statement_rejected(self, validator):
        _rejects(
            validator,
            "SELECT decision FROM DECISIONS.FACT_DECISIONS; -- harmless\nDELETE FROM DECISIONS.FACT_DECISIONS",
        )


# -------------------------------------------------------------
# Table allowlist — RAW (PII) is the boundary that must hold
# -------------------------------------------------------------
class TestAllowlist:
    def test_raw_pii_table_rejected(self, validator):
        _rejects(validator, "SELECT * FROM RAW.FACT_TRANSACTIONS")

    def test_dim_users_rejected(self, validator):
        _rejects(validator, "SELECT full_name, home_city FROM DIM.DIM_USERS")

    def test_union_sneaking_raw_rejected(self, validator):
        # the classic: a legal first branch, PII in the second
        _rejects(
            validator,
            "SELECT decision FROM DECISIONS.FACT_DECISIONS "
            "UNION SELECT full_name FROM RAW.FACT_TRANSACTIONS",
        )

    def test_subquery_reaching_raw_rejected(self, validator):
        _rejects(
            validator,
            "SELECT decision FROM DECISIONS.FACT_DECISIONS "
            "WHERE user_id IN (SELECT user_id FROM RAW.FACT_TRANSACTIONS)",
        )

    def test_join_to_raw_rejected(self, validator):
        _rejects(
            validator,
            "SELECT d.decision FROM DECISIONS.FACT_DECISIONS d "
            "JOIN RAW.FACT_TRANSACTIONS r ON d.transaction_id = r.transaction_id",
        )

    def test_allowed_join_ok(self, validator):
        out = validator.validate(
            "SELECT d.decision, f.risk_score_raw "
            "FROM DECISIONS.FACT_DECISIONS d "
            "JOIN FEATURES.FACT_FEATURE_SNAPSHOTS f ON d.snapshot_id = f.snapshot_id"
        )
        assert "FACT_DECISIONS" in out.upper()


# -------------------------------------------------------------
# Fully-qualified relations required
# -------------------------------------------------------------
class TestQualification:
    def test_unqualified_table_rejected(self, validator):
        _rejects(validator, "SELECT * FROM FACT_DECISIONS")

    def test_three_part_name_ok(self, validator):
        out = validator.validate("SELECT decision FROM FRAUD_DETECTION.DECISIONS.FACT_DECISIONS")
        assert "FACT_DECISIONS" in out.upper()

    def test_wrong_database_in_three_part_rejected(self, validator):
        # right schema.table shape, wrong database -> must not slip through
        _rejects(validator, "SELECT * FROM OTHERDB.DECISIONS.FACT_DECISIONS")


# -------------------------------------------------------------
# Dynamic identifiers, stages, table functions, INFORMATION_SCHEMA
# -------------------------------------------------------------
class TestDynamicAndFunctions:
    def test_dynamic_identifier_rejected(self, validator):
        _rejects(validator, "SELECT COUNT(*) FROM IDENTIFIER('RAW.FACT_TRANSACTIONS')")

    def test_table_function_rejected(self, validator):
        _rejects(validator, "SELECT * FROM TABLE(FLATTEN(input => parse_json('[1,2]')))")

    def test_stage_reference_rejected(self, validator):
        _rejects(validator, "SELECT $1 FROM @my_stage")

    def test_information_schema_rejected(self, validator):
        _rejects(validator, "SELECT table_name FROM DECISIONS.INFORMATION_SCHEMA.TABLES")

    def test_information_schema_unqualified_rejected(self, validator):
        _rejects(validator, "SELECT table_name FROM INFORMATION_SCHEMA.TABLES")

    def test_snowflake_db_functions_via_identifier_rejected(self, validator):
        _rejects(validator, "SELECT COUNT(*) FROM IDENTIFIER('DECISIONS.FACT_DECISIONS')")


# -------------------------------------------------------------
# LIMIT clamping — enforce config cap even when SQL asks for more
# -------------------------------------------------------------
class TestLimitClamp:
    def _limit_of(self, sql):
        import sqlglot
        node = sqlglot.parse_one(sql, read="snowflake")
        lim = node.args.get("limit")
        return int(lim.expression.name) if lim else None

    def test_missing_limit_gets_capped(self, validator):
        out = validator.validate("SELECT decision FROM DECISIONS.FACT_DECISIONS")
        assert self._limit_of(out) == MAX_ROWS

    def test_oversized_limit_clamped_down(self, validator):
        out = validator.validate("SELECT decision FROM DECISIONS.FACT_DECISIONS LIMIT 100000")
        assert self._limit_of(out) == MAX_ROWS

    def test_small_limit_preserved(self, validator):
        out = validator.validate("SELECT decision FROM DECISIONS.FACT_DECISIONS LIMIT 25")
        assert self._limit_of(out) == 25

    def test_union_gets_capped(self, validator):
        out = validator.validate(
            "SELECT decision FROM DECISIONS.FACT_DECISIONS "
            "UNION SELECT NULL FROM FEATURES.FACT_FEATURE_SNAPSHOTS"
        )
        # a set operation with no limit must still be capped
        assert "LIMIT" in out.upper()


# -------------------------------------------------------------
# Parse failure fails closed
# -------------------------------------------------------------
class TestParseFailure:
    def test_garbage_rejected(self, validator):
        _rejects(validator, "this is not sql at all )(")

    def test_empty_rejected(self, validator):
        _rejects(validator, "   ")
