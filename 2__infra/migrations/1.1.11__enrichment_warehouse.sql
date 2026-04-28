-- =====================================================
-- 1.1.11__enrichment_warehouse.sql
-- Layer   : Infrastructure — Warehouses
-- Purpose : Dedicated XSMALL warehouse for Cortex AI enrichment tasks.
--
-- Isolation rationale:
--   WH_ADMIN is shared by migrations, seed load, schemachange, and
--   Streamlit queries. Pinning enrichment tasks to their own warehouse
--   prevents an accidental resize of WH_ADMIN from running
--   CORTEX.COMPLETE on a LARGE (or bigger) warehouse and generating
--   unexpected credit spend.
--
--   AUTO_SUSPEND = 60s ensures the warehouse idles for at most one
--   billing minute after the hourly task batch finishes.
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;

CREATE WAREHOUSE IF NOT EXISTS WH_ENRICHMENT_{{ environment }}
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Dedicated warehouse for Cortex AI enrichment tasks (TSK_AI_SENTIMENT, TSK_AI_KEYWORDS). Fixed at XSMALL — do not resize without reviewing Cortex credit impact.';

-- Grant usage to the project admin role (task owner) and consumer role
GRANT USAGE ON WAREHOUSE WH_ENRICHMENT_{{ environment }} TO ROLE {{ environment }}_ADMIN_FR;
GRANT USAGE ON WAREHOUSE WH_ENRICHMENT_{{ environment }} TO ROLE {{ environment }}_REPORT_FR;
