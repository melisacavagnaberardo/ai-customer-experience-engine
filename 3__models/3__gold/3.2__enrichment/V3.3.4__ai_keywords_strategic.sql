-- =====================================================
-- FILE: V3.3.4__ai_keywords_strategic.sql
-- LAYER: GOLD / AI
-- PURPOSE: Deep LLM enrichment on a strategic subset to control Cortex costs
-- =====================================================
--
-- DESIGN:
-- COMPLETE is applied only where business value is highest:
--   - Verified purchases (quality signal)
--   - Reviews with helpful votes (peer-validated relevance)
--   - Extreme ratings (1★ or 5★ = highest actionable signal)
--
-- Model: llama3.1-8b (free in trial; sufficient for keyword extraction)
-- Limit: 500 rows → estimated run time ~5 min
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- DEEP ENRICHMENT — strategic subset with LIMIT to control Cortex costs
UPDATE TB_REVIEWS_ENRICHED t

SET
    KEYWORDS = SNOWFLAKE.CORTEX.COMPLETE(
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
    FROM TB_REVIEWS_ENRICHED t2
    JOIN DB_GOLD_{{ environment }}.GOLD.VW_TB_REVIEWS r ON t2.ID = r.ID
    WHERE t2.KEYWORDS IS NULL
      AND t2.BODY IS NOT NULL
      AND TRIM(t2.BODY) <> ''
      AND (
          r.VERIFIED_PURCHASE = 'TRUE'
       OR r.FOUND_HELPFUL > 5
       OR t2.STARS IN (1, 5)
      )
    LIMIT 500
) sub

WHERE t.ID = sub.ID;
