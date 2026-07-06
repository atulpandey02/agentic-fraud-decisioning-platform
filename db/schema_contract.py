# =============================================================
# SCHEMA CONTRACT — the single source of truth for table shape
# =============================================================
# Priority 2 exists because schema.sql drifted from the live table
# (FACT_FEATURE_SNAPSHOTS gained is_synthetic_fraud / fraud_pattern
# by a hand-run ALTER on Day 4; the DDL file never learned about
# them). Drift like that is invisible until a writer references a
# column that isn't there, or a reader selects one that quietly
# went away.
#
# This module makes the shape a DECLARED, testable fact in one
# place. Everything that reads or writes these tables is checked
# against it (tests/test_schema_contract.py parses the actual SQL
# in the code and asserts every column exists here and every
# required column is supplied), and the migrations that build the
# live schema are checked against it too. One list, many checkers —
# so drift becomes a failing test, not a production surprise.
#
# It lives in db/ (not snowflake/) deliberately: a Python module
# under a directory named `snowflake/` would shadow the installed
# snowflake-connector-python package the moment that directory
# landed on sys.path.
# =============================================================


# Per table: ordered column name -> required?  (required == NOT NULL
# in the DDL, i.e. a writer MUST supply it). Ordering matches the
# live DESC so a human diffing this against DESC TABLE reads top to
# bottom. Types are intentionally omitted — this contract governs
# PRESENCE and NULLABILITY, which is where the drift bites; the DDL
# in snowflake/migrations owns the types.
TABLES = {
    "FEATURES.FACT_FEATURE_SNAPSHOTS": {
        "SNAPSHOT_ID": True,
        "TRANSACTION_ID": True,
        "USER_ID": True,
        "USER_SURROGATE_KEY": True,
        "COMPUTED_AT": True,
        "VELOCITY_5MIN": False,
        "VELOCITY_15MIN": False,
        "VELOCITY_1HR": False,
        "VELOCITY_24HR": False,
        "TXN_AMOUNT": False,
        "USER_AVG_AMOUNT": False,
        "USER_STDDEV_AMOUNT": False,
        "AMOUNT_ZSCORE": False,
        "PREV_TRANSACTION_CITY": False,
        "PREV_TRANSACTION_TS": False,
        "GEO_DISTANCE_KM": False,
        "TIME_SINCE_LAST_TXN_MIN": False,
        "IS_NEW_DEVICE": False,
        "DEVICE_ID": False,
        "RISK_SCORE_RAW": False,
        "IS_FLAGGED_FOR_REVIEW": False,
        # The two columns whose absence from schema.sql started this
        # priority. Nullable: a legitimate transaction has no pattern.
        "IS_SYNTHETIC_FRAUD": False,
        "FRAUD_PATTERN": False,
    },
    "DECISIONS.FACT_DECISIONS": {
        "DECISION_ID": True,
        "TRANSACTION_ID": True,
        "USER_ID": True,
        "SNAPSHOT_ID": False,
        "DECISION": True,
        "CONFIDENCE_SCORE": False,
        "REASONING_TEXT": False,
        "IDENTIFIED_PATTERN": False,
        "GOVERNANCE_TIER": False,
        "DECIDED_AT": True,
        "PROCESSING_LATENCY_MS": False,
        "HUMAN_REVIEWED": False,
        "HUMAN_REVIEWER_ID": False,
        "HUMAN_OUTCOME": False,
        "HUMAN_REVIEWED_AT": False,
        "HUMAN_NOTES": False,
        "EVAL_CORRECT": False,
        "EVAL_SCORED_AT": False,
        "LLM_JUDGE_SCORE": False,
        "LLM_JUDGE_NOTES": False,
    },
    "DECISIONS.FACT_AGENT_TRACES": {
        "TRACE_ID": True,
        "DECISION_ID": True,
        "STEP_NUMBER": True,
        "AGENT_NAME": False,
        "STEP_TYPE": False,
        "TOOL_NAME": False,
        "TOOL_INPUT": False,
        "TOOL_OUTPUT": False,
        "REASONING_TEXT": False,
        "TOKENS_USED": False,
        "LATENCY_MS": False,
        "CREATED_AT": False,
    },
    "DIM.DIM_USERS": {
        "SURROGATE_KEY": True,
        "USER_ID": True,
        "FULL_NAME": False,
        "AGE": False,
        "HOME_CITY": False,
        "HOME_COUNTRY": False,
        "HOME_LATITUDE": False,
        "HOME_LONGITUDE": False,
        "AVG_TRANSACTION_AMT": False,
        "STDDEV_TRANSACTION_AMT": False,
        "AVG_DAILY_TXN_COUNT": False,
        "ACCOUNT_CREATED_AT": False,
        "RISK_TIER": False,
        "IS_ACTIVE": False,
        "VALID_FROM": True,
        "VALID_TO": False,
        "IS_CURRENT": False,
        "UPDATED_AT": False,
    },
    "DIM.DIM_DEVICES": {
        "DEVICE_ID": True,
        "USER_ID": True,
        "DEVICE_TYPE": False,
        "DEVICE_OS": False,
        "FIRST_SEEN_AT": False,
        "IS_TRUSTED": False,
        "REGISTERED_AT": False,
    },
}


# Enumerated columns — the value sets that used to live only in
# Snowflake CHECK constraints. Priority 2 item 5 moves enforcement
# into application code (validators.py) so a bad value fails with a
# readable message at the boundary, before the connector turns a
# constraint violation into an opaque error. The CHECK constraints
# stay in the DDL as the last line of defense; this is the first.
ENUMS = {
    "DECISION": {"ALLOW", "BLOCK", "ESCALATE"},
    "GOVERNANCE_TIER": {"AUTO_APPROVE", "SUGGEST", "NOTIFY_ONLY"},
    "HUMAN_OUTCOME": {"CONFIRMED", "OVERRIDDEN", "ESCALATED"},
    "RISK_TIER": {"LOW", "MEDIUM", "HIGH"},
    "FRAUD_PATTERN": {"VELOCITY_SPIKE", "GEO_JUMP", "NEW_DEVICE", "AMOUNT_ANOMALY"},
    # Agents may additionally report "NONE" for identified_pattern
    # (no pattern applies) — that is NOT a fraud_pattern label, so it
    # is kept out of FRAUD_PATTERN and handled explicitly by callers.
}


def columns(table: str) -> set:
    """All valid column names for a table (upper-cased)."""
    return set(TABLES[table].keys())


def required_columns(table: str) -> set:
    """Columns a writer MUST supply (NOT NULL, no usable default)."""
    return {c for c, req in TABLES[table].items() if req}


def is_known_table(table: str) -> bool:
    return table.upper() in TABLES
