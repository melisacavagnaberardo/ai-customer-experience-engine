-- =====================================================
-- 1.1.2__databases.sql
-- Layer   : Infrastructure — Databases
-- Purpose : Create the five project databases for the
--           given environment (DES / PRE / PRO).
--           Databases are idempotent (IF NOT EXISTS).
-- Depends : 1.1.1__roles.sql (role must exist first)
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE WAREHOUSE COMPUTE_WH;
CREATE DATABASE IF NOT EXISTS DB_SOURCE_{{ environment }};
CREATE DATABASE IF NOT EXISTS DB_ADMIN_{{ environment }};
-- RAW
CREATE DATABASE IF NOT EXISTS DB_RAW_{{ environment }};
-- SILVER
CREATE DATABASE IF NOT EXISTS DB_SILVER_{{ environment }};
-- GOLD
CREATE DATABASE IF NOT EXISTS DB_GOLD_{{ environment }};

