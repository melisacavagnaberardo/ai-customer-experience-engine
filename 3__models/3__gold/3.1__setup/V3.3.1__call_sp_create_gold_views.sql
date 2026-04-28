-- =========================================================
-- SILVER → GOLD LAYER
-- =========================================================
-- Purpose:
--   Generate the GOLD layer as views over SILVER.
--   GOLD is the semantic (business) layer exposed to end users.
--
-- Note:
--   - If SILVER contains TABLES → views over tables are created
--   - If SILVER contains VIEWS  → views over views are created
--   - SP is agnostic to the source object type
--
-- Source:  DB_SILVER_{{ environment }}.SILVER
-- Target:  DB_GOLD_{{ environment }}.GOLD
-- =========================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA DB_ADMIN_{{ environment }}.PLATFORM;

CALL DB_ADMIN_{{ environment }}.PLATFORM.SP_CREATE_VIEWS(
    'DB_SILVER_{{ environment }}',     -- SOURCE_DB: SILVER database
    'SILVER',                          -- SOURCE_SCHEMA: SILVER layer
    'DB_GOLD_{{ environment }}',       -- TARGET_DB: GOLD database
    'GOLD',                            -- TARGET_SCHEMA: GOLD layer
    '{{ environment }}'                -- ENV
);