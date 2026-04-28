-- =====================================================
-- 1.1.5__warehouses.sql
-- Layer   : Infrastructure — Warehouses
-- Purpose : Create the project warehouse (XSMALL,
--           auto-suspend 60 s, auto-resume). One
--           warehouse per environment.
-- Depends : 1.1.1__roles.sql
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
CREATE WAREHOUSE IF NOT EXISTS WH_ADMIN_{{ environment }}
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;