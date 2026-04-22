/*
================================================================================
RAW INGESTION PIPELINE - PRODUCTS & REVIEWS
================================================================================

Purpose:
    Executes the universal ingestion stored procedure to load external
    Amazon datasets into the RAW layer of the Snowflake architecture.

Description:
    This script triggers the SP_UNIVERSAL_RAW_INGEST stored procedure twice:

    1. Loads product master data from the external dataset
    2. Loads product reviews data from the same source

    Both datasets are ingested into the DB_RAW_<environment>.RAW schema,
    ensuring standardized raw layer persistence before downstream transformations.

Parameters:
    - Source: External Snowflake marketplace dataset
    - Target: RAW schema tables
    - Environment: Injected dynamically via {{ environment }}

Notes:
    - Idempotent at procedure level (depends on SP implementation)
    - Part of RAW ingestion layer in medallion architecture (RAW → SILVER → GOLD → IA)
*/

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_RAW_{{ environment }};
USE SCHEMA RAW;

CALL DB_RAW_{{ environment }}.RAW.SP_UNIVERSAL_BATCH_RAW_INGEST(
    'AMAZON_BEST_SELLERS_RATINGS_AND_REVIEWS.PUBLIC.REVIEWS',
    'REVIEWS',
    '{{ environment }}'
);
