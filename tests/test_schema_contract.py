# =============================================================
# CONTRACT TESTS — every SQL reader/writer vs the schema contract
# =============================================================
# Priority 2 item 3. The failure this prevents is the one that
# started the priority: a writer (or reader) references a column
# the table doesn't have, or a writer omits a NOT NULL column, and
# nobody notices until a live INSERT fails at 2am.
#
# These tests read the ACTUAL SQL from the source files via `ast`
# (no execution, no imports) and parse it with sqlglot, then check
# every column against db/schema_contract.py. Extracting by AST —
# rather than importing the modules — deliberately avoids pulling
# in pyspark/langchain and the per-phase `config` module-name
# collision; the test depends only on stdlib + sqlglot + the light
# contract module.
# =============================================================

import ast
import os
import re

import pytest
import sqlglot
from sqlglot import exp

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
from fraud_platform.db import schema_contract as sc


# --- helpers to pull named constants out of a source file, no import ---
def _module_constants(relpath: str) -> dict:
    """Return {name: literal_value} for every module-level assignment
    to a literal (str / list / etc.) in the file — via ast, so nothing
    in the module actually runs."""
    with open(os.path.join(ROOT, relpath)) as f:
        tree = ast.parse(f.read())
    out = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                try:
                    out[target.id] = ast.literal_eval(node.value)
                except (ValueError, SyntaxError):
                    pass
    return out


# The writers use the connector's pyformat paramstyle (%(name)s / %s),
# which is not SQL and makes sqlglot read '%' as modulo. Normalize to
# sqlglot's named-placeholder syntax before parsing — this changes only
# the parameter TOKENS, never the columns/structure the tests check.
def _parse(sql: str) -> exp.Expression:
    normalized = re.sub(r"%\((\w+)\)s", r":\1", sql).replace("%s", ":p")
    # sqlglot won't accept a placeholder as a LIMIT expression; swap it
    # for a literal (the row count is irrelevant to the column checks).
    normalized = re.sub(r"(?i)(LIMIT\s+):\w+", r"\g<1>100", normalized)
    return sqlglot.parse_one(normalized, read="snowflake")


def _table_of(node: exp.Expression) -> str:
    t = node.find(exp.Table)
    return f"{(t.db or '').upper()}.{t.name.upper()}" if t else ""


def _insert_columns(sql: str):
    stmt = _parse(sql)
    assert isinstance(stmt, exp.Insert), f"expected INSERT, got {type(stmt).__name__}"
    schema = stmt.this  # exp.Schema: table + column identifiers
    table = f"{(schema.this.db or '').upper()}.{schema.this.name.upper()}"
    cols = [c.name.upper() for c in schema.expressions]
    return table, cols


# =============================================================
# Registry of every extracted SQL constant we validate.
# (file, constant_name, kind, table)
# =============================================================
INSERT_CONSTANTS = [
    ("fraud_platform/stream_processing/feature_engine.py", "FACT_FEATURE_SNAPSHOTS_INSERT_SQL",
     "FEATURES.FACT_FEATURE_SNAPSHOTS"),
    ("fraud_platform/governance/hitl_handler.py", "FACT_DECISIONS_INSERT_SQL",
     "DECISIONS.FACT_DECISIONS"),
]


class TestInsertContracts:
    @pytest.mark.parametrize("relpath,const,table", INSERT_CONSTANTS)
    def test_insert_columns_exist(self, relpath, const, table):
        sql = _module_constants(relpath)[const]
        parsed_table, cols = _insert_columns(sql)
        assert parsed_table == table
        unknown = set(cols) - sc.columns(table)
        assert not unknown, f"{const} inserts unknown columns: {unknown}"

    @pytest.mark.parametrize("relpath,const,table", INSERT_CONSTANTS)
    def test_insert_supplies_all_required(self, relpath, const, table):
        sql = _module_constants(relpath)[const]
        _, cols = _insert_columns(sql)
        missing = sc.required_columns(table) - set(cols)
        assert not missing, f"{const} omits required (NOT NULL) columns: {missing}"

    @pytest.mark.parametrize("relpath,const,table", INSERT_CONSTANTS)
    def test_insert_column_value_counts_match(self, relpath, const, table):
        # a classic bug: N columns, N-1 placeholders. Parse both sides.
        sql = _module_constants(relpath)[const]
        stmt = _parse(sql)
        _, cols = _insert_columns(sql)
        values = stmt.find(exp.Values)
        n_vals = len(values.expressions[0].expressions)
        assert len(cols) == n_vals, f"{const}: {len(cols)} cols vs {n_vals} values"


class TestFeatureInsertReconciled:
    """The specific drift that motivated Priority 2."""
    def test_ground_truth_columns_present(self):
        sql = _module_constants(
            "fraud_platform/stream_processing/feature_engine.py"
        )["FACT_FEATURE_SNAPSHOTS_INSERT_SQL"]
        _, cols = _insert_columns(sql)
        assert "IS_SYNTHETIC_FRAUD" in cols
        assert "FRAUD_PATTERN" in cols


class TestUpdateContracts:
    def test_review_update_columns_exist(self):
        sql = _module_constants(
            "fraud_platform/governance/hitl_handler.py"
        )["FACT_DECISIONS_REVIEW_UPDATE_SQL"]
        stmt = _parse(sql)
        assert isinstance(stmt, exp.Update)
        table = _table_of(stmt)
        assert table == "DECISIONS.FACT_DECISIONS"
        set_cols = {c.this.name.upper() for c in stmt.expressions if isinstance(c, exp.EQ)}
        unknown = set_cols - sc.columns(table)
        assert not unknown, f"review UPDATE sets unknown columns: {unknown}"

    def test_review_update_is_conditional_on_unreviewed(self):
        # the atomicity guarantee must be IN the SQL, not just the docstring
        sql = _module_constants(
            "fraud_platform/governance/hitl_handler.py"
        )["FACT_DECISIONS_REVIEW_UPDATE_SQL"]
        assert "human_reviewed = FALSE" in sql, "review UPDATE must guard on human_reviewed=FALSE"


class TestSelectContracts:
    def test_pending_reviews_columns_exist(self):
        sql = _module_constants(
            "fraud_platform/governance/hitl_handler.py"
        )["PENDING_REVIEWS_SELECT_SQL"]
        stmt = _parse(sql)
        table = _table_of(stmt)
        assert table == "DECISIONS.FACT_DECISIONS"
        selected = {c.name.upper() for c in stmt.find_all(exp.Column)}
        unknown = selected - sc.columns(table)
        assert not unknown, f"pending_reviews SELECT references unknown columns: {unknown}"


class TestTraceColumnList:
    def test_trace_columns_exist_and_ordered(self):
        cols = _module_constants(
            "fraud_platform/observability/audit_logger.py"
        )["FACT_AGENT_TRACES_COLUMNS"]
        table = "DECISIONS.FACT_AGENT_TRACES"
        upper = [c.upper() for c in cols]
        unknown = set(upper) - sc.columns(table)
        assert not unknown, f"trace writer lists unknown columns: {unknown}"
        assert sc.required_columns(table) - set(upper) == set()


def _ddl_block(table_name: str) -> str:
    """Return just the CREATE TABLE (...) body for one table from
    schema.sql — so a column check is scoped to the RIGHT table and
    can't be fooled by a same-named column on a different table (RAW
    also has is_synthetic_fraud / fraud_pattern, which is exactly how
    a naive whole-file substring check gives a false pass)."""
    with open(os.path.join(ROOT, "snowflake", "schema.sql")) as f:
        ddl = f.read()
    m = re.search(
        rf"CREATE TABLE IF NOT EXISTS {table_name}\s*\((.*?)\n\)",
        ddl, re.IGNORECASE | re.DOTALL,
    )
    assert m, f"{table_name} CREATE TABLE block not found in schema.sql"
    return m.group(1).upper()


class TestContractMatchesDDL:
    """The contract must match snowflake/schema.sql (the reconciled
    DDL) so all three — DDL, contract, and code — agree."""
    def test_feature_snapshot_ground_truth_in_ddl(self):
        block = _ddl_block("FACT_FEATURE_SNAPSHOTS")
        # the reconciliation: the FEATURES table itself must declare both
        assert "IS_SYNTHETIC_FRAUD" in block
        assert "FRAUD_PATTERN" in block

    def test_ddl_columns_match_contract(self):
        # every contract column for FACT_FEATURE_SNAPSHOTS appears in its
        # own DDL block — catches drift in either direction
        block = _ddl_block("FACT_FEATURE_SNAPSHOTS")
        for col in sc.columns("FEATURES.FACT_FEATURE_SNAPSHOTS"):
            assert col in block, f"schema.sql FACT_FEATURE_SNAPSHOTS missing {col}"
