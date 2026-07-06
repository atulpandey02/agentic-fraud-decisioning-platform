-- =============================================================
-- V003 — staging table for idempotent feature-snapshot replay
-- =============================================================
-- Problem (Priority 2 item 4): the ingestion path is Kafka ->
-- Spark -> S3 Parquet -> Snowpipe COPY -> FACT_FEATURE_SNAPSHOTS.
-- COPY only ever APPENDS. The feature engine reads Kafka from
-- 'earliest', so a restart / backfill / reprocess re-emits every
-- transaction, each time with a FRESH random snapshot_id — COPY
-- happily appends a second, third, Nth row for the same
-- transaction_id. Replays silently inflate the table and poison
-- every COUNT / rate / eval that groups by transaction.
--
-- Fix: land raw loads in this STAGING table, then MERGE into the
-- fact table keyed on transaction_id (the stable identity across
-- replays — snapshot_id is NOT stable, it is regenerated per run).
-- MERGE makes re-loading the same transaction an UPDATE, not a
-- duplicate INSERT, so the pipeline becomes idempotent under replay.
-- See db/replay.py for the MERGE and REPLAY_STRATEGY.md for the why.
--
-- Same column shape as the fact table so a plain COPY targets it.
-- Idempotent: CREATE ... IF NOT EXISTS.
-- =============================================================

CREATE TABLE IF NOT EXISTS FRAUD_DETECTION.FEATURES.STG_FEATURE_SNAPSHOTS (
    snapshot_id             VARCHAR(36),
    transaction_id          VARCHAR(36),
    user_id                 VARCHAR(36),
    user_surrogate_key      VARCHAR(36),
    computed_at             TIMESTAMP_NTZ,
    velocity_5min           INT,
    velocity_15min          INT,
    velocity_1hr            INT,
    velocity_24hr           INT,
    txn_amount              FLOAT,
    user_avg_amount         FLOAT,
    user_stddev_amount      FLOAT,
    amount_zscore           FLOAT,
    prev_transaction_city   VARCHAR(100),
    prev_transaction_ts     TIMESTAMP_NTZ,
    geo_distance_km         FLOAT,
    time_since_last_txn_min FLOAT,
    is_new_device           BOOLEAN,
    device_id               VARCHAR(36),
    risk_score_raw          FLOAT,
    is_flagged_for_review   BOOLEAN,
    is_synthetic_fraud      BOOLEAN,
    fraud_pattern           VARCHAR(50)
);
