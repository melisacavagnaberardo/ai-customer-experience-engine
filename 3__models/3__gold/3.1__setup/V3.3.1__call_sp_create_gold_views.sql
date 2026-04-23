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
-- Fuente: DB_SILVER_{{ environment }}.SILVER
-- Destino: DB_GOLD_{{ environment }}.GOLD
-- =========================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA DB_ADMIN_{{ environment }}.PLATFORM;

CALL DB_ADMIN_{{ environment }}.PLATFORM.SP_CREATE_VIEWS(
    'DB_SILVER_{{ environment }}',     -- SOURCE_DB: base SILVER
    'SILVER',                          -- SOURCE_SCHEMA: capa SILVER
    'DB_GOLD_{{ environment }}',       -- TARGET_DB: base GOLD
    'GOLD',                            -- TARGET_SCHEMA: capa GOLD
    '{{ environment }}'                -- ENV
);