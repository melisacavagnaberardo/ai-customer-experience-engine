
USE ROLE ACCOUNTADMIN;
-- =====================================================
-- 4. PERMISOS CRÍTICOS PARA SCHEMACHANGE (ESTO ES LO QUE FALTABA)
-- =====================================================

-- Acceso al DB y schema donde vive el change history
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

GRANT USAGE ON DATABASE DB_RAW_{{ environment }} TO ROLE {{ environment }}_ADMIN_FR;
GRANT USAGE ON SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE TABLE ON SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE PROCEDURE ON SCHEMA DB_RAW_{{ environment }}.RAW TO ROLE {{ environment }}_ADMIN_FR;
