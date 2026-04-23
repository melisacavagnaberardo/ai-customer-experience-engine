-- ============================================================
-- FILE: 1.11__create_users.sql
-- LAYER: SECURITY / ACCESS CONTROL
-- PURPOSE: Create consumer user for Streamlit demo
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS U_REPORT_CONSUMER_{{ environment }}
    PASSWORD = 'DemoUser123!'
    DEFAULT_ROLE = {{ environment }}_REPORT_FR
    DEFAULT_WAREHOUSE = 'WH_ADMIN_{{ environment }}'
    DEFAULT_NAMESPACE = 'DB_GOLD_{{ environment }}.AI'
    COMMENT = 'Streamlit consumer user for AI Customer Experience Engine';

GRANT ROLE {{ environment }}_REPORT_FR TO USER U_REPORT_CONSUMER_{{ environment }};
