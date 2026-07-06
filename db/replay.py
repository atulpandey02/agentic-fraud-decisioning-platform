# =============================================================
# REPLAY — idempotent staging -> fact MERGE (Priority 2 item 4)
# =============================================================
# Turns the append-only COPY path into an idempotent upsert. Loads
# land in FEATURES.STG_FEATURE_SNAPSHOTS (V003); this MERGE folds
# them into FACT_FEATURE_SNAPSHOTS keyed on transaction_id, so
# reprocessing the same Kafka offsets updates the existing row
# instead of appending a duplicate.
#
# Two layers of dedup, because replays produce duplicates two ways:
#   1. WITHIN a load — the same transaction can appear more than
#      once in one staging batch. A QUALIFY ROW_NUMBER() keeps only
#      the LATEST (by computed_at) per transaction_id before the
#      MERGE, so the MERGE source has one row per key (Snowflake
#      errors if a MERGE source matches a target row more than once
#      — this is also what prevents that).
#   2. ACROSS loads — a transaction already in the fact table.
#      WHEN MATCHED updates it in place.
#
# The column list is derived from db/schema_contract.py, so this
# MERGE cannot drift from the table it targets — add a column to the
# contract and it flows into both the UPDATE and INSERT clauses.
# =============================================================

import logging

from schema_contract import TABLES

logger = logging.getLogger("replay")

_TARGET = "FRAUD_DETECTION.FEATURES.FACT_FEATURE_SNAPSHOTS"
_STAGING = "FRAUD_DETECTION.FEATURES.STG_FEATURE_SNAPSHOTS"
_IDENTITY = "TRANSACTION_ID"   # stable across replays; snapshot_id is not
_ORDER_BY = "COMPUTED_AT"      # "latest wins" when a batch repeats a txn


def build_merge_sql(target: str = _TARGET, staging: str = _STAGING) -> str:
    """
    Build the idempotent MERGE. Pure/deterministic (no DB), so the
    generated SQL is unit-tested by parsing it.
    """
    cols = list(TABLES["FEATURES.FACT_FEATURE_SNAPSHOTS"].keys())
    non_key = [c for c in cols if c != _IDENTITY]

    dedup_src = (
        f"SELECT * FROM {staging} "
        f"QUALIFY ROW_NUMBER() OVER "
        f"(PARTITION BY {_IDENTITY} ORDER BY {_ORDER_BY} DESC NULLS LAST) = 1"
    )
    set_clause = ", ".join(f"tgt.{c} = src.{c}" for c in non_key)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"src.{c}" for c in cols)

    return (
        f"MERGE INTO {target} AS tgt\n"
        f"USING ({dedup_src}) AS src\n"
        f"ON tgt.{_IDENTITY} = src.{_IDENTITY}\n"
        f"WHEN MATCHED THEN UPDATE SET {set_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def run_merge(conn, target: str = _TARGET, staging: str = _STAGING) -> int:
    """
    Execute the MERGE and return the number of rows affected. Caller
    is expected to TRUNCATE the staging table afterward (the loader
    owns that, so a failed merge leaves the staged data for retry).
    """
    sql = build_merge_sql(target, staging)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        affected = cur.rowcount
        conn.commit()
        logger.info("MERGE %s -> %s affected %s rows", staging, target, affected)
        return affected
    finally:
        cur.close()
