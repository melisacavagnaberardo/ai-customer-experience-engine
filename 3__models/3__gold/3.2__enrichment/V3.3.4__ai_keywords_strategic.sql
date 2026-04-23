-- =====================================================
-- FILE: V3.3.4__ai_keywords_strategic.sql
-- LAYER: GOLD / AI
-- PURPOSE: Deep enrichment con LLM sobre subconjunto estratégico
-- =====================================================
--
-- DISEÑO:
-- Se aplica COMPLETE solo donde el valor de negocio es mayor:
--   - Reviews verificadas (señal de calidad)
--   - Reviews con votos útiles (relevancia validada por usuarios)
--   - Reviews extremas (1★ o 5★ = más información accionable)
--
-- Modelo: llama3.1-8b (sin costo en trial, suficiente para keyword extraction)
-- Límite: 500 filas → tiempo estimado ~5 min
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- DEEP ENRICHMENT — subconjunto estratégico con LIMIT para control de costos
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
