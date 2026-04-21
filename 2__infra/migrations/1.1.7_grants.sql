
USE ROLE ACCOUNTADMIN;
-- =====================================================
-- 4. PERMISOS CRÍTICOS PARA SCHEMACHANGE (ESTO ES LO QUE FALTABA)
-- =====================================================

-- Acceso al DB y schema donde vive el change history
GRANT USAGE ON DATABASE DB_ADMIN_{{ environment }} TO ROLE {{ environment }}_ADMIN_FR;
GRANT USAGE ON SCHEMA DB_ADMIN_{{ environment }}.SCHEMACHANGE TO ROLE {{ environment }}_ADMIN_FR;

GRANT CREATE TABLE ON SCHEMA DB_ADMIN_{{ environment }}.SCHEMACHANGE TO ROLE {{ environment }}_ADMIN_FR;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DB_ADMIN_{{ environment }}.SCHEMACHANGE TO ROLE {{ environment }}_ADMIN_FR;
