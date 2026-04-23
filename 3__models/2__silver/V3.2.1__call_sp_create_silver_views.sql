-- =========================================================
-- RAW → SILVER LAYER
-- =========================================================
-- Objetivo:
--   Generar la capa SILVER como vistas sobre tablas RAW.
--   Aplica cuando SILVER es una capa de limpieza/estandarización
--   sin lógica de negocio compleja.
--
-- Fuente: DB_RAW_DES.RAW
-- Destino: DB_SILVER_DES.SILVER
-- =========================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA DB_ADMIN_{{ environment }}.PLATFORM;

CALL DB_ADMIN_DES.PLATFORM.SP_CREATE_VIEWS(
    'DB_RAW_DES',        -- SOURCE_DB: base RAW
    'RAW',               -- SOURCE_SCHEMA: capa RAW
    'DB_SILVER_DES',     -- TARGET_DB: base SILVER
    'SILVER',            -- TARGET_SCHEMA: capa SILVER
    'DES'                -- ENV
);