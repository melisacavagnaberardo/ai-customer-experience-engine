
USE ROLE ACCOUNTADMIN;
-- =====================================================
-- 4. Critical grants for schemachange (change history schema access)
-- =====================================================

-- Access to the DB and schema where the schemachange history table lives
GRANT USAGE ON DATABASE DB_ADMIN_{{ environment }} TO ROLE {{ environment }}_ADMIN_FR;
GRANT USAGE ON SCHEMA DB_ADMIN_{{ environment }}.SCHEMACHANGE TO ROLE {{ environment }}_ADMIN_FR;

GRANT CREATE TABLE ON SCHEMA DB_ADMIN_{{ environment }}.SCHEMACHANGE TO ROLE {{ environment }}_ADMIN_FR;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DB_ADMIN_{{ environment }}.SCHEMACHANGE TO ROLE {{ environment }}_ADMIN_FR;

-- APPS schema (Streamlit objects)
GRANT USAGE ON DATABASE DB_GOLD_{{ environment }} TO ROLE {{ environment }}_ADMIN_FR;
GRANT USAGE ON SCHEMA DB_GOLD_{{ environment }}.APPS TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE STREAMLIT ON SCHEMA DB_GOLD_{{ environment }}.APPS TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE STAGE ON SCHEMA DB_GOLD_{{ environment }}.APPS TO ROLE {{ environment }}_ADMIN_FR;

-- Consumer role (REPORT_FR) — AI layer
GRANT USAGE ON DATABASE DB_GOLD_{{ environment }} TO ROLE {{ environment }}_REPORT_FR;
GRANT USAGE ON SCHEMA DB_GOLD_{{ environment }}.AI TO ROLE {{ environment }}_REPORT_FR;
GRANT SELECT ON ALL TABLES IN SCHEMA DB_GOLD_{{ environment }}.AI TO ROLE {{ environment }}_REPORT_FR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DB_GOLD_{{ environment }}.AI TO ROLE {{ environment }}_REPORT_FR;
GRANT USAGE ON WAREHOUSE WH_ADMIN_{{ environment }} TO ROLE {{ environment }}_REPORT_FR;

-- Consumer role (REPORT_FR) — APPS layer
GRANT USAGE ON SCHEMA DB_GOLD_{{ environment }}.APPS TO ROLE {{ environment }}_REPORT_FR;
GRANT READ ON STAGE DB_GOLD_{{ environment }}.APPS.STREAMLIT_STAGE TO ROLE {{ environment }}_REPORT_FR;
-- FUTURE STREAMLITS covers the app object created in run_app_deploy() (step 4 of deploy pipeline)
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA DB_GOLD_{{ environment }}.APPS TO ROLE {{ environment }}_REPORT_FR;

GRANT USAGE ON DATABASE DB_RAW_{{ environment }} TO ROLE {{ environment }}_ADMIN_FR;
GRANT USAGE ON SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE TABLE ON SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE PROCEDURE ON SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;

GRANT USAGE ON SCHEMA DB_ADMIN_{{ environment }}.LOGS TO ROLE {{ environment }}_ADMIN_FR;
-- ALL TABLES covers TB_LOGS (created in 1.1.4); FUTURE TABLES covers TB_PIPELINE_LOGS (created in 1.1.8)
GRANT SELECT ON ALL TABLES IN SCHEMA DB_ADMIN_{{ environment }}.LOGS TO ROLE {{ environment }}_ADMIN_FR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DB_ADMIN_{{ environment }}.LOGS TO ROLE {{ environment }}_ADMIN_FR;

-- Snowflake ACCOUNT_USAGE access (for cost monitoring in Admin Panel)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE {{ environment }}_ADMIN_FR;