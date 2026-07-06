-- =============================================================
-- V001 — reconcile FACT_FEATURE_SNAPSHOTS ground-truth columns
-- =============================================================
-- The columns is_synthetic_fraud / fraud_pattern were added to the
-- LIVE table by a hand-run ALTER on Day 4, but the DDL never learned
-- about them (the drift Priority 2 closes). This migration captures
-- that ALTER as a versioned, IDEMPOTENT step:
--   - On a database that predates the columns  -> adds them.
--   - On the current live table (already has them) -> no-op.
--   - On a fresh database built from the reconciled schema.sql
--     (which now declares them) -> no-op.
-- Safe to run any number of times, in any of those states.
-- =============================================================

ALTER TABLE FRAUD_DETECTION.FEATURES.FACT_FEATURE_SNAPSHOTS
    ADD COLUMN IF NOT EXISTS is_synthetic_fraud BOOLEAN;

ALTER TABLE FRAUD_DETECTION.FEATURES.FACT_FEATURE_SNAPSHOTS
    ADD COLUMN IF NOT EXISTS fraud_pattern VARCHAR(50);
