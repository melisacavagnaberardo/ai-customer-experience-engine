-- Requires ACCOUNTADMIN — used only for initial bootstrap of project roles
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
-- =====================================================
-- 1. Create project roles
-- =====================================================
CREATE ROLE IF NOT EXISTS {{ environment }}_ADMIN_FR;
CREATE ROLE IF NOT EXISTS {{ environment }}_REPORT_FR;
-- =====================================================
-- 2. Role hierarchy and assignment
-- =====================================================
GRANT ROLE {{ environment }}_ADMIN_FR TO ROLE SYSADMIN;
GRANT ROLE {{ environment }}_ADMIN_FR TO USER MCAVAGNA;

-- =====================================================
-- 3. Account-level permissions (infrastructure bootstrap)
-- =====================================================
GRANT CREATE DATABASE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

