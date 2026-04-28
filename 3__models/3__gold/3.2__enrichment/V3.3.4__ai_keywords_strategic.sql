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
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA PLATFORM;

-- Delegate to SP_AI_KEYWORDS_RETRY: row-level retry with backoff,
-- FAILED marking, and pipeline log alerts replace the raw UPDATE.
CALL SP_AI_KEYWORDS_RETRY('{{ environment }}');
