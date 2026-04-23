-- =========================================================
-- SILVER → GOLD LAYER
-- =========================================================
-- Objetivo:
--   Generar la capa GOLD como vistas sobre SILVER.
--   GOLD es la capa semántica (business layer).
--
-- IMPORTANTE:
--   - Si SILVER contiene TABLAS → crea vistas GOLD
--   - Si SILVER contiene VISTAS → también crea vistas GOLD
--   - El SP es agnóstico al tipo de objeto origen
--
-- Fuente: DB_SILVER_DES.SILVER
-- Destino: DB_GOLD_DES.GOLD
-- =========================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA DB_ADMIN_{{ environment }}.PLATFORM;

CALL DB_ADMIN_DES.PLATFORM.SP_CREATE_VIEWS(
    'DB_SILVER_DES',     -- SOURCE_DB: base SILVER
    'SILVER',            -- SOURCE_SCHEMA: capa SILVER
    'DB_GOLD_DES',       -- TARGET_DB: base GOLD
    'GOLD',              -- TARGET_SCHEMA: capa GOLD
    'DES'                -- ENV
);