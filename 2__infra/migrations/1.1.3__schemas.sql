-- =====================================================
-- 1.1.3__schemas.sql
-- Layer   : Infrastructure — Schemas & Stages
-- Purpose : Create all schemas inside the project
--           databases and the STREAMLIT_STAGE internal
--           stage used by Streamlit in Snowflake.
-- Depends : 1.1.2__databases.sql
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;

CREATE SCHEMA IF NOT EXISTS DB_SOURCE_{{ environment }}.STAGING;
CREATE SCHEMA IF NOT EXISTS DB_ADMIN_{{ environment }}.SCHEMACHANGE;
CREATE SCHEMA IF NOT EXISTS DB_ADMIN_{{ environment }}.PLATFORM;
CREATE SCHEMA IF NOT EXISTS DB_ADMIN_{{ environment }}.LOGS;
CREATE SCHEMA IF NOT EXISTS DB_RAW_{{ environment }}.RAW;
CREATE SCHEMA IF NOT EXISTS DB_SILVER_{{ environment }}.SILVER;
CREATE SCHEMA IF NOT EXISTS DB_GOLD_{{ environment }}.GOLD;
CREATE SCHEMA IF NOT EXISTS DB_GOLD_{{ environment }}.AI;
CREATE SCHEMA IF NOT EXISTS DB_GOLD_{{ environment }}.APPS;

CREATE STAGE IF NOT EXISTS DB_GOLD_{{ environment }}.APPS.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit in Snowflake app files';