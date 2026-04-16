-- 1. Usar el rol con permisos de cuenta
USE ROLE ACCOUNTADMIN;

-- 2. Crear la tabla de eventos (Sin definir columnas, Snowflake lo hace solo)
-- La ubicaremos en el esquema LOGS que parece ser tu intención
CREATE EVENT TABLE IF NOT EXISTS DB_ADMIN_{{ environment }}.LOGS.TB_LOGS;

-- 3. Asociar esta tabla a la cuenta para capturar logs (Paso Vital)
-- Solo puede haber una tabla de eventos activa por cuenta
ALTER ACCOUNT SET EVENT_TABLE = DB_ADMIN_{{ environment }}.LOGS.TB_LOGS;