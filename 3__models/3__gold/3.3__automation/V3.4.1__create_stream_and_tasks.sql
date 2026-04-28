-- =====================================================
-- V3.4.1__create_stream_and_tasks.sql
-- Layer   : GOLD / AUTOMATION
-- Purpose : Incremental AI enrichment via Snowflake Stream + Task DAG
--
-- Architecture:
--   STR_REVIEWS_NEW  (APPEND_ONLY stream on DB_RAW.RAW.TB_REVIEWS)
--     → TSK_AI_SENTIMENT  (root task, hourly, WHEN stream has data)
--         → TSK_AI_KEYWORDS  (child task, LLM on strategic subset)
--
-- Stream offset advances automatically each time TSK_AI_SENTIMENT
-- DMLs against it, so the DAG only fires when new reviews exist.
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- ── STREAM ────────────────────────────────────────────────────────────────────
-- APPEND_ONLY: captures INSERT operations only (SP_UNIVERSAL_BATCH_RAW_INGEST
-- merges new hashes, so all changes to TB_REVIEWS are inserts).
CREATE STREAM IF NOT EXISTS DB_GOLD_{{ environment }}.AI.STR_REVIEWS_NEW
    ON TABLE DB_RAW_{{ environment }}.RAW.TB_REVIEWS
    APPEND_ONLY = TRUE;

-- ── ROOT TASK: SENTIMENT ──────────────────────────────────────────────────────
-- Fires every hour; skipped automatically when stream has no new data.
-- Reads FROM the stream (advances offset) and inserts into TB_REVIEWS_ENRICHED.
CREATE OR REPLACE TASK DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT
    WAREHOUSE = WH_ADMIN_{{ environment }}
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

-- ── CHILD TASK: KEYWORDS ──────────────────────────────────────────────────────
-- Runs after TSK_AI_SENTIMENT. No WHEN condition — UPDATE is a no-op
-- if no strategic rows are pending, so cost is negligible on empty runs.
CREATE OR REPLACE TASK DB_GOLD_{{ environment }}.AI.TSK_AI_KEYWORDS
    WAREHOUSE = WH_ADMIN_{{ environment }}
    AFTER     DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT
AS
UPDATE DB_GOLD_{{ environment }}.AI.TB_REVIEWS_ENRICHED t

SET
    KEYWORDS          = SNOWFLAKE.CORTEX.COMPLETE(
                            'llama3.1-8b',
                            CONCAT(
                                'Extract 3 to 5 key drivers or topics from this customer review. ',
                                'Return ONLY a comma-separated list.\n\nReview:\n',
                                t.BODY
                            )
                        ),
    ENRICHMENT_STATUS = 'FULLY_ENRICHED'

FROM (
    SELECT t2.ID
    FROM DB_GOLD_{{ environment }}.AI.TB_REVIEWS_ENRICHED t2
    JOIN DB_GOLD_{{ environment }}.GOLD.VW_TB_REVIEWS r ON t2.ID = r.ID
    WHERE t2.KEYWORDS IS NULL
      AND t2.BODY IS NOT NULL
      AND TRIM(t2.BODY) <> ''
      AND (
             r.VERIFIED_PURCHASE = 'TRUE'
          OR r.FOUND_HELPFUL > 5
          OR t2.STARS IN (1, 5)
      )
    ORDER BY r.FOUND_HELPFUL DESC NULLS LAST, t2.STARS ASC
    LIMIT 500
) sub

WHERE t.ID = sub.ID;

-- ── RESUME (children first, then root — required by Snowflake DAG rules) ──────
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_KEYWORDS  RESUME;
ALTER TASK DB_GOLD_{{ environment }}.AI.TSK_AI_SENTIMENT RESUME;
