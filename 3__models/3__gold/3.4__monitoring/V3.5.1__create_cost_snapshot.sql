-- =====================================================
-- V3.5.1__create_cost_snapshot.sql
-- Layer   : ADMIN / MONITORING
-- Purpose : Pre-aggregate ACCOUNT_USAGE cost metrics into
--           DB_ADMIN.LOGS.TB_COST_SNAPSHOT via a daily task.
--
-- Design rationale (see ARCHITECTURE.md ADR-7):
--   ACCOUNT_USAGE views have ~45-minute latency and are
--   expensive to aggregate on each Admin Panel page load.
--   A daily snapshot read is instantaneous and decouples
--   the UI from ACCOUNT_USAGE availability.
--
-- The Admin Panel reads the most recent snapshot row.
-- Falls back to live ACCOUNT_USAGE when no snapshot exists.
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;

-- ── TABLE ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DB_ADMIN_{{ environment }}.LOGS.TB_COST_SNAPSHOT (
    SNAPSHOT_DATE  DATE          NOT NULL,
    WH_CREDITS     FLOAT,
    AI_CREDITS     FLOAT,
    STORAGE_GB     FLOAT,
    CREATED_AT     TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ── TASK ──────────────────────────────────────────────────────────────────
-- Fires daily at 06:00 UTC. Reads 30-day aggregates from ACCOUNT_USAGE and
-- inserts one summary row. INSERT-only; the app always reads ORDER BY SNAPSHOT_DATE DESC.
CREATE OR REPLACE TASK DB_ADMIN_{{ environment }}.PLATFORM.TSK_COST_SNAPSHOT
    WAREHOUSE = WH_ADMIN_{{ environment }}
    SCHEDULE  = 'USING CRON 0 6 * * * UTC'
AS
INSERT INTO DB_ADMIN_{{ environment }}.LOGS.TB_COST_SNAPSHOT
    (SNAPSHOT_DATE, WH_CREDITS, AI_CREDITS, STORAGE_GB)
SELECT
    CURRENT_DATE(),
    COALESCE((
        SELECT ROUND(SUM(CREDITS_USED), 4)
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
          AND WAREHOUSE_NAME = 'WH_ADMIN_{{ environment }}'
    ), 0),
    COALESCE((
        SELECT ROUND(SUM(CREDITS_USED), 4)
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
          AND SERVICE_TYPE = 'AI_SERVICES'
    ), 0),
    COALESCE((
        SELECT ROUND(SUM(AVERAGE_DATABASE_BYTES) / POWER(1024, 3), 4)
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
        WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
          AND DATABASE_NAME ILIKE '%{{ environment }}%'
    ), 0);

ALTER TASK DB_ADMIN_{{ environment }}.PLATFORM.TSK_COST_SNAPSHOT RESUME;
