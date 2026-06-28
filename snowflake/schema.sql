-- =============================================================
-- FRAUD DETECTION PLATFORM — SNOWFLAKE SCHEMA
-- Account: CZYDFRS-AF06435
-- =============================================================
-- Design principles:
--   - Separate schemas per access domain (security + clarity)
--   - RAW: pipeline writes only, no agent access
--   - FEATURES: pipeline writes, agents read
--   - DECISIONS: agents write, BI agent reads
--   - DIM: pipeline writes, agents read (baseline context)
-- =============================================================

-- -------------------------------------------------------------
-- DATABASE
-- -------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS FRAUD_DETECTION;
USE DATABASE FRAUD_DETECTION;

-- -------------------------------------------------------------
-- SCHEMAS
-- -------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS DIM;        -- user profiles, devices
CREATE SCHEMA IF NOT EXISTS RAW;        -- raw transactions (pipeline only)
CREATE SCHEMA IF NOT EXISTS FEATURES;   -- computed feature snapshots
CREATE SCHEMA IF NOT EXISTS DECISIONS;  -- agent decisions + audit log

-- =============================================================
-- DIM SCHEMA — baseline user profiles and devices
-- Populated once by user_profile_generator.py, updated nightly
-- =============================================================
USE SCHEMA DIM;

CREATE TABLE IF NOT EXISTS DIM_USERS (
    -- SCD Type 2 — full history preserved per user
    -- Why: home_city, home_latitude/longitude drive geo-distance features.
    -- If a user moves, past transactions must still be evaluated against
    -- where they WERE at the time — not where they are today.
    -- Joining on: valid_from <= transaction_ts AND valid_to > transaction_ts

    surrogate_key           VARCHAR(36)     NOT NULL,   -- UUID, PK per version
    user_id                 VARCHAR(36)     NOT NULL,   -- natural key, stable across versions
    full_name               VARCHAR(100),
    age                     INT,
    home_city               VARCHAR(100),
    home_country            VARCHAR(100),
    home_latitude           FLOAT,
    home_longitude          FLOAT,
    avg_transaction_amt     FLOAT,          -- baseline avg spend per transaction
    stddev_transaction_amt  FLOAT,          -- for z-score computation in Spark
    avg_daily_txn_count     FLOAT,          -- baseline transactions per day
    account_created_at      TIMESTAMP_NTZ,
    risk_tier               VARCHAR(10)
        CHECK (risk_tier IN ('LOW', 'MEDIUM', 'HIGH')),
    is_active               BOOLEAN         DEFAULT TRUE,

    -- SCD2 tracking columns
    valid_from              TIMESTAMP_NTZ   NOT NULL,
    valid_to                TIMESTAMP_NTZ   DEFAULT '9999-12-31 00:00:00'::TIMESTAMP_NTZ,
    is_current              BOOLEAN         DEFAULT TRUE,
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (surrogate_key)
);

CREATE TABLE IF NOT EXISTS DIM_DEVICES (
    device_id               VARCHAR(36)     NOT NULL,   -- UUID
    user_id                 VARCHAR(36)     NOT NULL,
    device_type             VARCHAR(20)
        CHECK (device_type IN ('MOBILE', 'WEB', 'POS', 'TABLET')),
    device_os               VARCHAR(50),
    first_seen_at           TIMESTAMP_NTZ,
    is_trusted              BOOLEAN         DEFAULT FALSE,
    registered_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (device_id)
);

-- =============================================================
-- RAW SCHEMA — every transaction, append-only
-- Pipeline writes only. No agent, no BI agent access.
-- =============================================================
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS (
    transaction_id          VARCHAR(36)     NOT NULL,   -- UUID
    user_id                 VARCHAR(36)     NOT NULL,
    device_id               VARCHAR(36),
    amount                  FLOAT           NOT NULL,
    currency                VARCHAR(3)      DEFAULT 'USD',
    merchant_name           VARCHAR(200),
    merchant_category       VARCHAR(100),
    city                    VARCHAR(100),
    country                 VARCHAR(100),
    latitude                FLOAT,
    longitude               FLOAT,
    transaction_ts          TIMESTAMP_NTZ   NOT NULL,
    ingested_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Ground truth labels (set by generator for synthetic fraud)
    -- Used in Phase 6 eval
    is_synthetic_fraud      BOOLEAN         DEFAULT FALSE,
    fraud_pattern           VARCHAR(50),
        -- 'VELOCITY_SPIKE'  : too many transactions in short window
        -- 'GEO_JUMP'        : impossible location change
        -- 'NEW_DEVICE'      : first-time device + high amount
        -- 'AMOUNT_ANOMALY'  : amount >> user historical average
        -- NULL              : legitimate transaction

    PRIMARY KEY (transaction_id)
)
CLUSTER BY (TO_DATE(transaction_ts), user_id);

-- =============================================================
-- FEATURES SCHEMA — Spark-computed feature snapshots
-- One row per transaction at the moment features were computed.
-- Agents read. Pipeline writes.
-- =============================================================
USE SCHEMA FEATURES;

CREATE TABLE IF NOT EXISTS FACT_FEATURE_SNAPSHOTS (
    snapshot_id             VARCHAR(36)     NOT NULL,
    transaction_id          VARCHAR(36)     NOT NULL,
    user_id                 VARCHAR(36)     NOT NULL,
    -- SCD2 reference: which version of the user was active when
    -- these features were computed. Enables point-in-time correctness
    -- for investigations, eval, and future model retraining.
    user_surrogate_key      VARCHAR(36)     NOT NULL,
    computed_at             TIMESTAMP_NTZ   NOT NULL,

    -- Velocity features (sliding window)
    velocity_5min           INT,
    velocity_15min          INT,
    velocity_1hr            INT,
    velocity_24hr           INT,

    -- Amount features
    txn_amount              FLOAT,
    user_avg_amount         FLOAT,
    user_stddev_amount      FLOAT,
    amount_zscore           FLOAT,          -- (txn_amount - avg) / stddev

    -- Geo features
    prev_transaction_city   VARCHAR(100),
    prev_transaction_ts     TIMESTAMP_NTZ,
    geo_distance_km         FLOAT,
    time_since_last_txn_min FLOAT,
    -- geo_distance_km / time_since_last_txn_min = implied travel speed
    -- Boston to Mumbai in 30 min = impossible = flag

    -- Device features
    is_new_device           BOOLEAN,
    device_id               VARCHAR(36),

    -- Composite risk signal (heuristic, pre-agent)
    risk_score_raw          FLOAT,          -- 0.0 to 1.0
    is_flagged_for_review   BOOLEAN         DEFAULT FALSE,

    PRIMARY KEY (snapshot_id)
);

-- =============================================================
-- DECISIONS SCHEMA — agent decisions + full audit trail
-- Agents write. BI agent reads. Compliance reads.
-- =============================================================
USE SCHEMA DECISIONS;

CREATE TABLE IF NOT EXISTS FACT_DECISIONS (
    decision_id             VARCHAR(36)     NOT NULL,
    transaction_id          VARCHAR(36)     NOT NULL,
    user_id                 VARCHAR(36)     NOT NULL,
    snapshot_id             VARCHAR(36),

    -- The decision
    decision                VARCHAR(10)     NOT NULL
        CHECK (decision IN ('ALLOW', 'BLOCK', 'ESCALATE')),
    confidence_score        FLOAT,
    reasoning_text          TEXT,
    identified_pattern      VARCHAR(50),

    -- Governance tier (Phase 5)
    governance_tier         VARCHAR(20)
        CHECK (governance_tier IN ('AUTO_APPROVE', 'SUGGEST', 'NOTIFY_ONLY')),

    -- Timing
    decided_at              TIMESTAMP_NTZ   NOT NULL,
    processing_latency_ms   INT,

    -- Human-in-the-loop (Phase 5)
    human_reviewed          BOOLEAN         DEFAULT FALSE,
    human_reviewer_id       VARCHAR(100),
    human_outcome           VARCHAR(20)
        CHECK (human_outcome IN ('CONFIRMED', 'OVERRIDDEN', 'ESCALATED')),
    human_reviewed_at       TIMESTAMP_NTZ,
    human_notes             TEXT,

    -- Eval fields (Phase 6)
    eval_correct            BOOLEAN,
    eval_scored_at          TIMESTAMP_NTZ,
    llm_judge_score         FLOAT,
    llm_judge_notes         TEXT,

    PRIMARY KEY (decision_id)
);

-- Agent reasoning trace — one row per reasoning step
-- Full observability into HOW the agent reached a decision
CREATE TABLE IF NOT EXISTS FACT_AGENT_TRACES (
    trace_id                VARCHAR(36)     NOT NULL,
    decision_id             VARCHAR(36)     NOT NULL,
    step_number             INT             NOT NULL,
    agent_name              VARCHAR(50),
    step_type               VARCHAR(50),    -- 'TOOL_CALL', 'REASONING', 'HANDOFF'
    tool_name               VARCHAR(100),
    tool_input              VARIANT,        -- JSON
    tool_output             VARIANT,        -- JSON
    reasoning_text          TEXT,
    tokens_used             INT,
    latency_ms              INT,
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (trace_id)
);