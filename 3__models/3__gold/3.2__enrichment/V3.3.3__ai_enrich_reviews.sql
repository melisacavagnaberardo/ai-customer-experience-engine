-- =====================================================
-- V3.3.3__ai_enrich_reviews.sql
-- Layer   : GOLD / AI
-- Purpose : FAST enrichment — run Cortex SENTIMENT on
--           every review that has not been processed yet.
--
-- Strategy (FAST_ONLY):
--   * Scores the full review corpus using SENTIMENT(),
--     which is inexpensive and fast (<1 credit / 1 M chars).
--   * KEYWORDS and SUMMARY are left NULL at this stage
--     to control LLM costs.  KEYWORDS are backfilled in
--     V3.3.4 on a strategic subset only.
--   * Idempotent: the LEFT JOIN / WHERE t.ID IS NULL
--     ensures already-enriched rows are never re-processed.
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- Insert only new reviews (not yet present in TB_REVIEWS_ENRICHED)
INSERT INTO TB_REVIEWS_ENRICHED (
    ID,
    ASIN,
    BODY,
    STARS,
    ROW_HASH,
    SENTIMENT,
    ENRICHMENT_STATUS,
    CREATED_AT
)

SELECT
    r.ID,
    r.ASIN,
    r.BODY,
    r.STARS,
    r.ROW_HASH,
    SNOWFLAKE.CORTEX.SENTIMENT(r.BODY),  -- Cortex built-in; returns -1.0 to +1.0
    'FAST_ONLY',                          -- Status updated to FULLY_ENRICHED by V3.3.4
    CURRENT_TIMESTAMP()

FROM DB_GOLD_{{ environment }}.GOLD.VW_TB_REVIEWS r
LEFT JOIN TB_REVIEWS_ENRICHED t ON r.ID = t.ID  -- anti-join: skip already-processed rows

WHERE t.ID IS NULL          -- only unprocessed reviews
  AND r.BODY IS NOT NULL    -- SENTIMENT() requires non-null input
  AND TRIM(r.BODY) <> '';   -- skip blank-body rows (would score 0.0 artificially)