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
    WAREHOUSE_SIZE = 'XSMALL'   -- batch-sequential workload; no concurrency benefit from larger size
    AUTO_SUSPEND   = 60         -- 60-second idle timeout minimises credit burn between pipeline stages
    AUTO_RESUME    = TRUE
    COMMENT        = 'Shared project warehouse (XSMALL). Workload is batch-sequential: migrations, seed load, schemachange models, hourly enrichment tasks, Streamlit queries. Scale to SMALL only if multiple users query concurrently.';