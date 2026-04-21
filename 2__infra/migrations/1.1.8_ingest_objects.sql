USE ROLE {{ environment }}_ADMIN_FR;

USE DATABASE DB_SOURCE_{{ environment }};

USE SCHEMA STAGING;

-- =====================================================
-- FILE FORMAT
-- =====================================================
CREATE OR REPLACE FILE FORMAT FF_CSV
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

-- =====================================================
-- STAGES
-- =====================================================
CREATE OR REPLACE STAGE STG_PRODUCTS;

CREATE OR REPLACE STAGE STG_REVIEWS;

-- =====================================================
-- TABLES (STAGING / SOURCE LAYER)
-- =====================================================

-- PRODUCTS (alineado con PRODUCTS.csv)
CREATE OR REPLACE TABLE TB_PRODUCTS_SRC (
    ASIN STRING,
    TITLE STRING,
    URL STRING,
    MAIN_IMAGE STRING,
    ALT_IMAGES STRING,
    BREADCRUMBS STRING,
    RATING NUMBER,
    NUMBER_OF_RATINGS NUMBER,
    DESCRIPTION STRING,
    CHARACTERISTICS STRING
);

-- REVIEWS (alineado con REVIEWS.csv)
CREATE OR REPLACE TABLE TB_REVIEWS_SRC (
    ID STRING,
    ASIN STRING,
    URL STRING,
    BODY STRING,
    DATE STRING,
    TITLE STRING,
    IMAGES STRING,
    VIDEOS STRING,
    STARS NUMBER,
    USER_ID STRING,
    VARIATION STRING,
    VERIFIED_PURCHASE STRING,
    FOUND_HELPFUL NUMBER
);