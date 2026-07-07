-- =============================================================
-- RBAC — LOCAL, ACCOUNT-SPECIFIC role grants (EXAMPLE)
-- =============================================================
-- The shared baseline (snowflake/rbac.sql) creates the roles and
-- grants their schema/warehouse privileges — everything that is the
-- same for every account. Granting those roles to a PARTICULAR USER
-- is account-specific, so it lives here, in an example file with a
-- placeholder, instead of hardcoding one person's username into the
-- shared script.
--
-- Usage:
--   1. Copy this file (e.g. to rbac_local.sql — git-ignored).
--   2. Replace <YOUR_SNOWFLAKE_USER> with your Snowflake login name.
--   3. Run it as ACCOUNTADMIN, after rbac.sql.
-- =============================================================

USE DATABASE FRAUD_DETECTION;

-- Let your user assume the least-privilege application roles. The BI
-- app connects as BI_ROLE and the agents as AGENT_ROLE (see
-- fraud_platform.settings); a user must be granted a role to use it.
GRANT ROLE BI_ROLE    TO USER <YOUR_SNOWFLAKE_USER>;
GRANT ROLE AGENT_ROLE TO USER <YOUR_SNOWFLAKE_USER>;

-- Optional: the pipeline role, if you run feature_engine under it.
-- GRANT ROLE PIPELINE_ROLE TO USER <YOUR_SNOWFLAKE_USER>;
