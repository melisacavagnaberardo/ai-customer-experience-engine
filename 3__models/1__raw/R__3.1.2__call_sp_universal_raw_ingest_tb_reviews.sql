USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_RAW_{{ environment }};
USE SCHEMA RAW;

//CALL DB_RAW_{{ environment }}.RAW.SP_UNIVERSAL_RAW_INGEST(
//    'AMAZON_BEST_SELLERS_RATINGS_AND_REVIEWS.PUBLIC.PRODUCTS',
//    'PRODUCTS',
//    '{{ environment }}'
//);
//
//CALL DB_RAW_{{ environment }}.RAW.SP_UNIVERSAL_RAW_INGEST(
//    'AMAZON_BEST_SELLERS_RATINGS_AND_REVIEWS.PUBLIC.PRODUCTS',
//    'REVIEWS',
//    '{{ environment }}'
//);


USE SCHEMA RAW;