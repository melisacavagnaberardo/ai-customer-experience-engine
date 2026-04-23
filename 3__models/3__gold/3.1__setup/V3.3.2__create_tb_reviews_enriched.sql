-- ============================================================
-- OBJECT: FCT_REVIEWS_ENRICHED
-- LAYER: GOLD
-- TYPE: FACT TABLE (AI-READY)
-- ============================================================
--
-- DESCRIPTION:
-- ------------------------------------------------------------
-- This table consolidates customer reviews with product metadata
-- to create a unified, AI-ready dataset.
--
-- It is designed as the foundational dataset for:
--   - Sentiment analysis (rule-based + AI)
--   - Topic extraction (LLM / NLP)
--   - Customer experience analytics
--   - Feature engineering for ML models
--
-- Each row represents a single review enriched with product context.
--
-- ------------------------------------------------------------
-- DATA SOURCES:
--   - DB_GOLD_{{ environment }}.GOLD.VW_TB_REVIEWS
--   - DB_GOLD_{{ environment }}.GOLD.VW_TB_PRODUCTS
--
-- ------------------------------------------------------------
-- DESIGN DECISIONS:
--   - Denormalized model → optimized for analytical workloads
--   - BASE_SENTIMENT → baseline heuristic (for validation vs AI)
--   - REVIEW_LENGTH → proxy for signal quality
--
-- ------------------------------------------------------------
-- IMPORTANT:
--   This table is intended as the INPUT layer for AI processing.
--   Do NOT embed heavy transformations here.
--   Advanced enrichment will be handled in downstream layers.
--
-- ============================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- ============================================================
-- Tabla de reviews enriquecidas con IA (Cortex)
-- DDL puro: la carga la hace V3.3.3
-- ============================================================
CREATE TABLE IF NOT EXISTS TB_REVIEWS_ENRICHED (
    ID                 STRING        NOT NULL,
    ASIN               STRING,
    BODY               STRING,
    STARS              NUMBER,
    ROW_HASH           STRING,
    SENTIMENT          FLOAT,
    KEYWORDS           STRING,
    ENRICHMENT_STATUS  VARCHAR(20),  -- 'FAST_ONLY' | 'FULLY_ENRICHED'
    CREATED_AT         TIMESTAMP_TZ
);