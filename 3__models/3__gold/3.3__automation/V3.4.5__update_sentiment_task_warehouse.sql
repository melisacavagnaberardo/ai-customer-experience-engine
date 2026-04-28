-- =====================================================
-- V3.4.5__update_sentiment_task_warehouse.sql
-- Layer   : GOLD / AUTOMATION
-- Purpose : Move TSK_AI_SENTIMENT to the dedicated enrichment warehouse
--           so that both tasks in the DAG use WH_ENRICHMENT instead of
--           the shared WH_ADMIN.
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_KEYWORDS  SUSPEND;
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT SUSPEND;

CREATE OR REPLACE TASK DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT
    WAREHOUSE = WH_ENRICHMENT_{{ environment }}
    SCHEDULE  = 'USING CRON 0 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('DB_GOLD_{{ environment }}.AI.STR_REVIEWS_NEW')
AS
INSERT INTO DB_GOLD_{{ environment }}.AI.TB_REVIEWS_ENRICHED (
    ID, ASIN, BODY, STARS, ROW_HASH, SENTIMENT, ENRICHMENT_STATUS, CREATED_AT
)
SELECT
    s.ID,
    s.ASIN,
    s.BODY,
    s.STARS,
    s.ROW_HASH,
    SNOWFLAKE.CORTEX.SENTIMENT(s.BODY),
    'FAST_ONLY',
    CURRENT_TIMESTAMP()
FROM DB_GOLD_{{ environment }}.AI.STR_REVIEWS_NEW s
LEFT JOIN DB_GOLD_{{ environment }}.AI.TB_REVIEWS_ENRICHED t ON s.ID = t.ID
WHERE t.ID IS NULL
  AND s.BODY IS NOT NULL
  AND TRIM(s.BODY) <> '';

-- Resume children first, then root (Snowflake DAG rule)
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_KEYWORDS  RESUME;
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT RESUME;
