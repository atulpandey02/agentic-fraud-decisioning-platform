-- =============================================================
-- FRAUD DETECTION PLATFORM — SNOWFLAKE RBAC
-- =============================================================
-- Three roles, three access domains.
-- Each role gets exactly what it needs — nothing more.
-- =============================================================

USE DATABASE FRAUD_DETECTION;

-- -------------------------------------------------------------
-- ROLES
-- -------------------------------------------------------------
CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;    -- Spark pipeline service account
CREATE ROLE IF NOT EXISTS AGENT_ROLE;       -- Fraud decisioning agents
CREATE ROLE IF NOT EXISTS BI_ROLE;          -- BI agent + analysts

-- -------------------------------------------------------------
-- PIPELINE_ROLE
-- Writes raw transactions, user profiles, feature snapshots.
-- Cannot read DECISIONS — it has no business there.
-- -------------------------------------------------------------
GRANT USAGE ON DATABASE FRAUD_DETECTION TO ROLE PIPELINE_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.DIM TO ROLE PIPELINE_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.RAW TO ROLE PIPELINE_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.FEATURES TO ROLE PIPELINE_ROLE;

GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA FRAUD_DETECTION.DIM TO ROLE PIPELINE_ROLE;
GRANT INSERT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.RAW TO ROLE PIPELINE_ROLE;
GRANT INSERT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.FEATURES TO ROLE PIPELINE_ROLE;
-- Read access on DIM so Spark can join baseline during feature computation
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.DIM TO ROLE PIPELINE_ROLE;

-- -------------------------------------------------------------
-- AGENT_ROLE
-- Reads features + user profiles to make decisions.
-- Writes decisions + traces.
-- CANNOT touch RAW — no agent should ever read raw PII directly.
-- -------------------------------------------------------------
GRANT USAGE ON DATABASE FRAUD_DETECTION TO ROLE AGENT_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.DIM TO ROLE AGENT_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.FEATURES TO ROLE AGENT_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE AGENT_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.DIM TO ROLE AGENT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.FEATURES TO ROLE AGENT_ROLE;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE AGENT_ROLE;
-- Agents can read their own past decisions (for context in RAG / memory)
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE AGENT_ROLE;

-- -------------------------------------------------------------
-- BI_ROLE
-- Read-only on DECISIONS and FEATURES.
-- Analysts and BI agent query the audit log — never raw data.
-- -------------------------------------------------------------
GRANT USAGE ON DATABASE FRAUD_DETECTION TO ROLE BI_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE BI_ROLE;
GRANT USAGE ON SCHEMA FRAUD_DETECTION.FEATURES TO ROLE BI_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.DECISIONS TO ROLE BI_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA FRAUD_DETECTION.FEATURES TO ROLE BI_ROLE;