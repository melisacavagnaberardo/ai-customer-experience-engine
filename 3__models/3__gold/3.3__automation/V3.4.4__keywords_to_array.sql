-- =====================================================
-- V3.4.4__keywords_to_array.sql
-- Layer   : GOLD / AI
-- Purpose : Migrate KEYWORDS column from VARCHAR (comma-separated)
--           to native Snowflake ARRAY.
--
-- Strategy:
--   1. Add KEYWORDS_ARRAY ARRAY column.
--   2. Backfill from existing STRING data via STRTOK_TO_ARRAY.
--   3. Drop legacy STRING column.
--   4. Rename KEYWORDS_ARRAY → KEYWORDS.
--
-- Downstream benefit: ARRAY_CONTAINS, FLATTEN, and any ML pipeline
-- can consume keywords without manual string parsing.
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_GOLD_{{ environment }};
USE SCHEMA AI;

-- Step 1: staging column
ALTER TABLE TB_REVIEWS_ENRICHED
    ADD COLUMN IF NOT EXISTS KEYWORDS_ARRAY ARRAY;

-- Step 2: backfill — trim each token, skip nulls
UPDATE TB_REVIEWS_ENRICHED
SET KEYWORDS_ARRAY = STRTOK_TO_ARRAY(TRIM(KEYWORDS), ',')
WHERE KEYWORDS IS NOT NULL;

-- Step 3: drop legacy VARCHAR column
ALTER TABLE TB_REVIEWS_ENRICHED DROP COLUMN IF EXISTS KEYWORDS;

-- Step 4: rename to canonical name
ALTER TABLE TB_REVIEWS_ENRICHED RENAME COLUMN KEYWORDS_ARRAY TO KEYWORDS;
