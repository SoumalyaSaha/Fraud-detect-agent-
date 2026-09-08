-- 04_actions_log.sql
-- Creates the ACTIONS_LOG table used by the poller to record every action
-- taken when processing an alert (Slack notification, simulated account lock).
--
-- Run this once in your Snowflake worksheet:
--   @sql/04_actions_log.sql

CREATE OR REPLACE TABLE FRAUD_AGENT.PUBLIC.ACTIONS_LOG (
    logged_at       TIMESTAMP_NTZ,
    alert_type      VARCHAR,
    txn_ts          TIMESTAMP_NTZ,
    observed_amount FLOAT,
    flag            VARCHAR,
    action          VARCHAR,  -- e.g. "SLACK_NOTIFICATION", "ACCOUNT_LOCKED_SIMULATED"
    status          VARCHAR   -- "SUCCESS" or "FAILED"
);
