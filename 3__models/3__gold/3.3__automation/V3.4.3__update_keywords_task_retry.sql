-- =====================================================
-- V3.4.3__update_keywords_task_retry.sql
-- Layer   : GOLD / AUTOMATION
-- Purpose : Replace the raw CORTEX.COMPLETE UPDATE in TSK_AI_KEYWORDS
--           with a call to SP_AI_KEYWORDS_RETRY, which adds per-row
--           retry with exponential backoff and failure marking.
--
-- Order:
--   1. Suspend child task before DROP/RECREATE (Snowflake requirement).
--   2. Recreate TSK_AI_KEYWORDS pointing to the SP.
--   3. Resume children first, then root (Snowflake DAG rule).
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- Suspend before recreating (required when task is part of a DAG)
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_KEYWORDS  SUSPEND;
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT SUSPEND;

-- Recreate child task — dedicated enrichment warehouse + SP call with retry
CREATE OR REPLACE TASK DB_GOLD_{{ environment }}.AI.TSK_AI_KEYWORDS
    WAREHOUSE = WH_ENRICHMENT_{{ environment }}
    AFTER     DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT
AS
CALL DB_ADMIN_{{ environment }}.PLATFORM.SP_AI_KEYWORDS_RETRY('{{ environment }}');

-- Resume children first, then root (required by Snowflake DAG rules)
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_KEYWORDS  RESUME;
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT RESUME;
