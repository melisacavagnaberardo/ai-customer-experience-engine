-- Ejecutado con el rol de máximo nivel
USE ROLE ACCOUNTADMIN;

-- 1. Crear el Rol del Proyecto
CREATE ROLE IF NOT EXISTS {{ environment }}_ADMIN_FR;

-- 2. Jerarquía y Asignación
GRANT ROLE {{ environment }}_ADMIN_FR TO ROLE SYSADMIN;
GRANT ROLE {{ environment }}_ADMIN_FR TO USER MCAVAGNA;

-- 2. Permisos a nivel de Cuenta
GRANT CREATE DATABASE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

-- 3. Permisos generales para que el rol trabaje
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE {{ environment }}_ADMIN_FR;

