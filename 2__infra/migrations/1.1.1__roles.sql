-- Requires ACCOUNTADMIN — used only for initial bootstrap of project roles
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
-- =====================================================
-- 1. Create project roles
-- =====================================================
CREATE ROLE IF NOT EXISTS {{ environment }}_ADMIN_FR;
-- Data engineer role: full access to RAW and SILVER layers; no access to GOLD.AI.
-- Intended for pipeline developers who build/debug ingestion and transformation layers.
CREATE ROLE IF NOT EXISTS {{ environment }}_ENGINEER_FR;
CREATE ROLE IF NOT EXISTS {{ environment }}_REPORT_FR;
-- =====================================================
-- 2. Role hierarchy and assignment
-- =====================================================
GRANT ROLE {{ environment }}_ADMIN_FR   TO ROLE SYSADMIN;
GRANT ROLE {{ environment }}_ADMIN_FR   TO USER {{ deploy_user }};
-- ENGINEER_FR inherits from ADMIN_FR in this environment (single-user trial).
-- In production, assign directly to pipeline developer users instead.
GRANT ROLE {{ environment }}_ENGINEER_FR TO ROLE {{ environment }}_ADMIN_FR;

-- =====================================================
-- 3. Account-level permissions (infrastructure bootstrap)
-- =====================================================
GRANT CREATE DATABASE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

