-- =====================================================
-- 1.1.6__resource_monitors.sql
-- Layer   : Infrastructure — Resource Monitors
-- Purpose : Create a resource monitor capped at
--           1 000 credits. Notifies at 90 % and
--           suspends at 100 %. Attached to the project
--           warehouse to prevent runaway costs.
-- Requires: ACCOUNTADMIN (only role that can create
--           resource monitors).
-- =====================================================

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS "RM_ADMIN_{{ environment }}" WITH CREDIT_QUOTA = 1000
    TRIGGERS ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE WH_ADMIN_{{ environment }}
SET RESOURCE_MONITOR = RM_ADMIN_{{ environment }};