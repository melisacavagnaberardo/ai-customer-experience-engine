-- =========================================================
-- RAW → SILVER LAYER
-- =========================================================
-- Purpose:
--   Generate the SILVER layer as views over RAW tables.
--   Applies when SILVER is a cleansing/standardisation layer
--   with no complex business logic.
--
-- Source:  DB_RAW_DES.RAW
-- Target:  DB_SILVER_DES.SILVER
-- =========================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA DB_ADMIN_{{ environment }}.PLATFORM;

CALL DB_ADMIN_DES.PLATFORM.SP_CREATE_VIEWS(
    'DB_RAW_DES',        -- SOURCE_DB: RAW database
    'RAW',               -- SOURCE_SCHEMA: RAW layer
    'DB_SILVER_DES',     -- TARGET_DB: SILVER database
    'SILVER',            -- TARGET_SCHEMA: SILVER layer
    'DES'                -- ENV
);