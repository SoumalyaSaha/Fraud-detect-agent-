-- 03_tasks.sql
-- Creates two Snowflake Tasks:
--   STREAM_SIM_TASK  — calls stream_simulator() every 60 seconds
--   FRAUD_CHECK_TASK — calls run_fraud_check()      every 60 seconds
--
-- Run this AFTER executing 01_run_fraud_check.sql and 02_stream_simulator.sql.

-- ── Stream simulator task ─────────────────────────────────────────────────────
CREATE OR REPLACE TASK FRAUD_AGENT.PUBLIC.STREAM_SIM_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '60 SECONDS'
AS
CALL FRAUD_AGENT.PUBLIC.stream_simulator(50);

-- ── Fraud check task ──────────────────────────────────────────────────────────
CREATE OR REPLACE TASK FRAUD_AGENT.PUBLIC.FRAUD_CHECK_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '60 SECONDS'
AS
CALL FRAUD_AGENT.PUBLIC.run_fraud_check();

-- ── Resume both tasks ─────────────────────────────────────────────────────────
ALTER TASK FRAUD_AGENT.PUBLIC.STREAM_SIM_TASK RESUME;
ALTER TASK FRAUD_AGENT.PUBLIC.FRAUD_CHECK_TASK RESUME;

-- ── (Optional) Suspend on demand:
-- ALTER TASK FRAUD_AGENT.PUBLIC.STREAM_SIM_TASK SUSPEND;
-- ALTER TASK FRAUD_AGENT.PUBLIC.FRAUD_CHECK_TASK SUSPEND;
