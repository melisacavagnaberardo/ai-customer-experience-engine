-- =====================================================
-- V3.4.2__add_error_handling_cols.sql
-- Layer   : GOLD / AI
-- Purpose : Add error-tracking columns to TB_REVIEWS_ENRICHED
--           required by SP_AI_KEYWORDS_RETRY.
--
-- New columns:
--   RETRY_COUNT   — number of CORTEX.COMPLETE attempts made (0 = first try succeeded)
--   ERROR_MESSAGE — last exception message when ENRICHMENT_STATUS = 'FAILED'
--   FAILED_AT     — timestamp of the final failed attempt
--
-- ENRICHMENT_STATUS extended values (no CHECK constraint — Snowflake enforces via app):
--   'FAST_ONLY'       set by TSK_AI_SENTIMENT / V3.3.3
--   'FULLY_ENRICHED'  set by SP_AI_KEYWORDS_RETRY on success
--   'FAILED'          set by SP_AI_KEYWORDS_RETRY after MAX_RETRIES exhausted
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

ALTER TABLE TB_REVIEWS_ENRICHED
    ADD COLUMN IF NOT EXISTS RETRY_COUNT   NUMBER        DEFAULT 0;

ALTER TABLE TB_REVIEWS_ENRICHED
    ADD COLUMN IF NOT EXISTS ERROR_MESSAGE VARCHAR(2000);

ALTER TABLE TB_REVIEWS_ENRICHED
    ADD COLUMN IF NOT EXISTS FAILED_AT     TIMESTAMP_NTZ;
