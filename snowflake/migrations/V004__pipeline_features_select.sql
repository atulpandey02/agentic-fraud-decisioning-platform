-- =============================================================
-- V004 — PIPELINE_ROLE SELECT on FEATURES (startup-probe grant)
-- =============================================================
-- The feature engine's startup probe (SnowflakeFeatureWriter.
-- test_connection) runs `SELECT COUNT(*) FROM
-- FEATURES.FACT_FEATURE_SNAPSHOTS`, but rbac.sql only granted
-- PIPELINE_ROLE INSERT on FEATURES — so the pipeline could never
-- actually start under its own least-privilege role, only under
-- ACCOUNTADMIN. This grants the minimal read it needs (its own
-- FEATURES output), on current and future tables. GRANT is
-- idempotent, so re-running is a no-op.
-- =============================================================

USE DATABASE FRAUD_DETECTION;

GRANT SELECT ON ALL TABLES    IN SCHEMA FRAUD_DETECTION.FEATURES TO ROLE PIPELINE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.FEATURES TO ROLE PIPELINE_ROLE;
