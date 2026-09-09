-- ============================================================
-- 11_final_two_signal_ensemble.sql
-- Corrected version of run_final_ensemble_vote().
--
-- Root cause of the invalid-identifier error: ALERTS has no
-- NAMEORIG column. Its real schema (confirmed via DESCRIBE) is:
--   DETECTED_AT, TYPE, TXN_TS, OBSERVED_AMOUNT,
--   LOWER_BOUND, UPPER_BOUND, FLAG
-- One row per (TYPE, hour-bucket) -- Agent 1 operates on hourly
-- averages, not individual transactions, so it has no concept of
-- a single account/NAMEORIG.
--
-- Fix: join on TYPE + the hour bucket a transaction's TXN_TS
-- falls into (DATE_TRUNC('HOUR', ...)). Any transaction in a
-- flagged (TYPE, hour) combination inherits HOURLY_AGG_FLAG = 1.
-- Also: FLAG is text ('OVER'/'UNDER'/etc), not a 0/1 -- treat
-- anything other than a clean/normal value as a flag. Adjust the
-- exact FLAG values below if your normal-case value differs from
-- 'NORMAL'.
-- ============================================================

-- ------------------------------------------------------------
-- FINAL ensemble table (two-signal OR logic):
--   HOURLY_AGG_FLAG — existing fraud_model / ALERTS (hourly avg)
--   ZSCORE_FLAG     — new per-type z-score check (per-transaction)
--   FINAL_VERDICT   — 1 if EITHER signal fires (OR logic)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FINAL_ANOMALY_VOTES (
    NAMEORIG         STRING,
    TXN_TS           TIMESTAMP_NTZ,
    AMOUNT           FLOAT,
    HOURLY_AGG_FLAG  NUMBER(1,0),
    ZSCORE_FLAG      NUMBER(1,0),
    FINAL_VERDICT    NUMBER(1,0),  -- 1 = flagged (OR of both signals)
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- Stored proc: run_final_ensemble_vote()
-- Joins ZSCORE_VOTES (per-transaction) with ALERTS (hourly agg)
-- on TYPE + hour bucket. Uses OR logic: flag if EITHER fires.
-- Additive only — does NOT modify FRAUD_CHECK_TASK, ALERTS,
-- or the existing hourly pipeline.
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE run_final_ensemble_vote()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO FINAL_ANOMALY_VOTES
        (NAMEORIG, TXN_TS, AMOUNT, HOURLY_AGG_FLAG, ZSCORE_FLAG, FINAL_VERDICT)
    SELECT
        z.NAMEORIG,
        z.TXN_TS,
        z.AMOUNT,
        COALESCE(a.ML_FLAG, 0)   AS HOURLY_AGG_FLAG,
        z.AGENT_VOTE_ZSCORE      AS ZSCORE_FLAG,
        CASE
            WHEN COALESCE(a.ML_FLAG, 0) = 1 OR z.AGENT_VOTE_ZSCORE = 1
            THEN 1 ELSE 0
        END AS FINAL_VERDICT
    FROM ZSCORE_VOTES z
    LEFT JOIN (
        SELECT
            TYPE,
            TXN_TS AS ALERT_HOUR,
            1 AS ML_FLAG
        FROM ALERTS
        WHERE FLAG IN ('OVER', 'UNDER')   -- adjust if your "flagged" values differ
    ) a
        ON z.TYPE = a.TYPE
       AND DATE_TRUNC('HOUR', z.TXN_TS) = a.ALERT_HOUR
    WHERE z.TXN_TS NOT IN (SELECT TXN_TS FROM FINAL_ANOMALY_VOTES);

    RETURN 'Final two-signal ensemble vote complete';
END;
$$;

-- ------------------------------------------------------------
-- Run it
-- ------------------------------------------------------------
-- CALL run_final_ensemble_vote();
-- SELECT * FROM FINAL_ANOMALY_VOTES ORDER BY CREATED_AT DESC LIMIT 20;

-- ------------------------------------------------------------
-- If FLAG values in ALERTS aren't 'OVER'/'UNDER', check first:
-- ------------------------------------------------------------
-- SELECT DISTINCT FLAG FROM ALERTS;