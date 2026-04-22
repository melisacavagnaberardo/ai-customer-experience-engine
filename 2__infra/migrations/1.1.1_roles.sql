-- Ejecutado con el rol de máximo nivel (solo para bootstrap del role)
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
-- =====================================================
-- 1. Crear el Rol del Proyecto
-- =====================================================
CREATE ROLE IF NOT EXISTS {{ environment }}_ADMIN_FR;
-- =====================================================
-- 2. Jerarquía y asignación (OK si lo necesitas)
-- =====================================================
GRANT ROLE {{ environment }}_ADMIN_FR TO ROLE SYSADMIN;
GRANT ROLE {{ environment }}_ADMIN_FR TO USER MCAVAGNA;

-- =====================================================
-- 3. Permisos a nivel de cuenta (infra básica)
-- =====================================================
GRANT CREATE DATABASE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

