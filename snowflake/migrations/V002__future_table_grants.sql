-- =============================================================
-- V002 — FUTURE TABLES grants (close the "new table is invisible" gap)
-- =============================================================
-- rbac.sql grants SELECT/INSERT ON ALL TABLES — but ALL TABLES is a
-- snapshot: it covers the tables that existed WHEN it ran. Any table
-- added later (a new fact table, a Priority-2 staging table) is born
-- with no grant, so BI_ROLE / AGENT_ROLE silently cannot see it until
-- someone remembers to re-grant. FUTURE grants fix that class of bug
-- permanently: the privilege attaches to every table created in the
-- schema from now on.
--
-- GRANT is idempotent (re-granting an existing privilege is a no-op),
-- so this migration is safe to re-run. The ON ALL TABLES re-grants are
-- included too, so a database restored from before rbac.sql reaches
-- the same end state from this file alone.
-- =============================================================

USE DATABASE FRAUD_DETECTION;

-- ---- BI_ROLE : read-only on DECISIONS + FEATURES, now and future ----
GRANT SELECT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE BI_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.FEATURES  TO ROLE BI_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE BI_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA FRAUD_DETECTION.FEATURES  TO ROLE BI_ROLE;

-- ---- AGENT_ROLE : reads DIM + FEATURES, writes DECISIONS ----
GRANT SELECT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.DIM       TO ROLE AGENT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.FEATURES  TO ROLE AGENT_ROLE;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE AGENT_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA FRAUD_DETECTION.DIM       TO ROLE AGENT_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA FRAUD_DETECTION.FEATURES  TO ROLE AGENT_ROLE;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE AGENT_ROLE;

-- ---- PIPELINE_ROLE : writes RAW + FEATURES + DIM ----
GRANT INSERT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.RAW      TO ROLE PIPELINE_ROLE;
GRANT INSERT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.FEATURES TO ROLE PIPELINE_ROLE;
GRANT INSERT, UPDATE ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.DIM TO ROLE PIPELINE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FRAUD_DETECTION.DIM      TO ROLE PIPELINE_ROLE;
