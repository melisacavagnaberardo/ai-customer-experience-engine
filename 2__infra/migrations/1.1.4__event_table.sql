-- 1. Switch to ACCOUNTADMIN — required for EVENT TABLE and account-level settings
USE ROLE ACCOUNTADMIN;

-- 2. Create the event table (column schema is fixed by Snowflake; do not define columns)
CREATE EVENT TABLE IF NOT EXISTS DB_ADMIN_{{ environment }}.LOGS.TB_LOGS;

-- 3. Associate this table with the account to capture SYSTEM$LOG events (required step)
-- Only one event table can be active per Snowflake account at a time
ALTER ACCOUNT SET EVENT_TABLE = DB_ADMIN_{{ environment }}.LOGS.TB_LOGS;

-- 4. Enable INFO log level — without this, SYSTEM$LOG('INFO', ...) writes nothing
ALTER ACCOUNT SET LOG_LEVEL = INFO;