-- =====================================================
-- V3.3.3__ai_enrich_reviews.sql
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- FAST ENRICHMENT — todos los reviews
-- SUMMARY y KEYWORDS quedan NULL; SUMMARY removido por costo, KEYWORDS se completa en V3.3.4
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
    SNOWFLAKE.CORTEX.SENTIMENT(r.BODY),
    'FAST_ONLY',
    CURRENT_TIMESTAMP()

FROM DB_GOLD_{{ environment }}.GOLD.VW_TB_REVIEWS r
LEFT JOIN TB_REVIEWS_ENRICHED t ON r.ID = t.ID

WHERE t.ID IS NULL
  AND r.BODY IS NOT NULL
  AND TRIM(r.BODY) <> '';