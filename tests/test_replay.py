# =============================================================
# UNIT TESTS — idempotent replay MERGE (Priority 2 item 4)
# =============================================================
# The MERGE is generated from the schema contract; these tests parse
# it and assert its structure, with no database.
# =============================================================


import sqlglot
from sqlglot import exp

from fraud_platform.db import replay
from fraud_platform.db import schema_contract as sc


class TestMergeShape:
    def test_parses_as_merge(self):
        stmt = sqlglot.parse_one(replay.build_merge_sql(), read="snowflake")
        assert isinstance(stmt, exp.Merge)

    def test_keyed_on_transaction_id(self):
        # idempotency depends on the join key being the STABLE identity;
        # snapshot_id is regenerated per run and would defeat dedup
        sql = replay.build_merge_sql()
        assert "tgt.TRANSACTION_ID = src.TRANSACTION_ID" in sql

    def test_dedups_source_within_batch(self):
        # a MERGE whose source matches a target row twice errors in
        # Snowflake — the QUALIFY keeps one row per key
        sql = replay.build_merge_sql()
        assert "ROW_NUMBER()" in sql and "PARTITION BY TRANSACTION_ID" in sql

    def test_all_contract_columns_inserted(self):
        sql = replay.build_merge_sql()
        cols = sc.columns("FEATURES.FACT_FEATURE_SNAPSHOTS")
        # every contract column must appear in the INSERT column list
        insert_part = sql.split("WHEN NOT MATCHED")[1]
        for c in cols:
            assert c in insert_part, f"MERGE INSERT missing {c}"

    def test_identity_not_in_update_set(self):
        # you don't update the join key
        sql = replay.build_merge_sql()
        update_part = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "tgt.TRANSACTION_ID = src.TRANSACTION_ID" not in update_part
