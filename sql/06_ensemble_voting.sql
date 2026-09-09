-- ============================================================
-- 06_ensemble_voting.sql
-- Three-signal fraud ensemble:
--   1. Hourly-aggregate anomaly (existing fraud_model / ALERTS)
--   2. Per-transaction z-score deviation (NEW)
--   3. Balance-consistency check (NEW)
-- Combined via 2-of-3 majority vote into ANOMALY_VOTES.
-- Includes a precision/recall query against isFraud ground truth.
--
-- Assumes RAW_TRANSACTIONS / HISTORICAL / LIVE_TRANSACTIONS have:
--   NAMEORIG, TXN_TS, TYPE, AMOUNT,
--   OLDBALANCEORG, NEWBALANCEORIG, OLDBALANCEDEST, NEWBALANCEDEST,
--   ISFRAUD   (ground truth, PaySim column)
-- Adjust names if yours differ.
-- ============================================================

-- ------------------------------------------------------------
-- STEP 1: Per-type mean/stddev stats table, computed once from
-- HISTORICAL. Recompute periodically if you retrain.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE TYPE_STATS AS
SELECT
    TYPE,
    AVG(AMOUNT)     AS TYPE_MEAN,
    STDDEV(AMOUNT)  AS TYPE_STDDEV
FROM HISTORICAL
GROUP BY TYPE;

-- ------------------------------------------------------------
-- STEP 2: Z-score view — per-transaction deviation check.
-- Flags a transaction if it's more than 4 standard deviations
-- from its type's historical mean. Tune the threshold (4-5) if
-- it over/under-flags on your trimmed dataset.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW ZSCORE_VOTES AS
SELECT
    lt.NAMEORIG,
    lt.TXN_TS,
    lt.TYPE,
    lt.AMOUNT,
    ts.TYPE_MEAN,
    ts.TYPE_STDDEV,
    CASE
        WHEN ts.TYPE_STDDEV = 0 OR ts.TYPE_STDDEV IS NULL THEN 0
        ELSE ABS( (lt.AMOUNT - ts.TYPE_MEAN) / ts.TYPE_STDDEV )
    END AS Z_SCORE,
    CASE
        WHEN ts.TYPE_STDDEV > 0
             AND ABS( (lt.AMOUNT - ts.TYPE_MEAN) / ts.TYPE_STDDEV ) > 4
        THEN 1 ELSE 0
    END AS AGENT_VOTE_ZSCORE
FROM LIVE_TRANSACTIONS lt
LEFT JOIN TYPE_STATS ts
    ON lt.TYPE = ts.TYPE;

-- ------------------------------------------------------------
-- STEP 3: Balance-consistency view (accounting-logic check,
-- independent of amount size). Flags mismatches on either the
-- origin or destination side.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW BALANCE_CONSISTENCY_VOTES AS
SELECT
    NAMEORIG,
    TXN_TS,
    AMOUNT,
    OLDBALANCEORG,
    NEWBALANCEORIG,
    OLDBALANCEDEST,
    NEWBALANCEDEST,
    ABS( (OLDBALANCEORG - AMOUNT) - NEWBALANCEORIG )   AS ORIG_ERROR,
    ABS( (OLDBALANCEDEST + AMOUNT) - NEWBALANCEDEST )  AS DEST_ERROR,
    CASE
        WHEN ABS( (OLDBALANCEORG - AMOUNT) - NEWBALANCEORIG ) > 1.0
          OR ABS( (OLDBALANCEDEST + AMOUNT) - NEWBALANCEDEST ) > 1.0
        THEN 1 ELSE 0
    END AS AGENT_VOTE_BALANCE
FROM LIVE_TRANSACTIONS;

-- ------------------------------------------------------------
-- STEP 4: Voting table — one row per transaction, one column
-- per signal, plus the combined verdict.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ANOMALY_VOTES (
    NAMEORIG         STRING,
    TXN_TS           TIMESTAMP_NTZ,
    AMOUNT           FLOAT,
    HOURLY_AGG_FLAG  NUMBER(1,0),  -- from existing ALERTS (fraud_model)
    ZSCORE_FLAG      NUMBER(1,0),
    BALANCE_FLAG     NUMBER(1,0),
    VOTE_COUNT       NUMBER(1,0),
    FINAL_VERDICT    NUMBER(1,0),  -- 1 = flagged, 2-of-3 majority
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- STEP 5: Stored proc — run this on the same schedule as
-- FRAUD_CHECK_TASK (or chain it right after).
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE run_ensemble_vote()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO ANOMALY_VOTES
        (NAMEORIG, TXN_TS, AMOUNT, HOURLY_AGG_FLAG, ZSCORE_FLAG,
         BALANCE_FLAG, VOTE_COUNT, FINAL_VERDICT)
    SELECT
        z.NAMEORIG,
        z.TXN_TS,
        z.AMOUNT,
        COALESCE(a.ML_FLAG, 0)   AS HOURLY_AGG_FLAG,
        z.AGENT_VOTE_ZSCORE      AS ZSCORE_FLAG,
        b.AGENT_VOTE_BALANCE     AS BALANCE_FLAG,
        (COALESCE(a.ML_FLAG, 0) + z.AGENT_VOTE_ZSCORE + b.AGENT_VOTE_BALANCE) AS VOTE_COUNT,
        CASE
            WHEN (COALESCE(a.ML_FLAG, 0) + z.AGENT_VOTE_ZSCORE + b.AGENT_VOTE_BALANCE) >= 2
            THEN 1 ELSE 0
        END AS FINAL_VERDICT
    FROM ZSCORE_VOTES z
    JOIN BALANCE_CONSISTENCY_VOTES b
        ON z.NAMEORIG = b.NAMEORIG AND z.TXN_TS = b.TXN_TS
    LEFT JOIN (
        SELECT NAMEORIG, TXN_TS, 1 AS ML_FLAG FROM ALERTS
    ) a
        ON z.NAMEORIG = a.NAMEORIG AND z.TXN_TS = a.TXN_TS
    WHERE z.TXN_TS NOT IN (SELECT TXN_TS FROM ANOMALY_VOTES);

    RETURN 'Ensemble vote complete';
END;
$$;

-- ============================================================
-- VALIDATION: precision / recall against PaySim's real ISFRAUD
-- ground-truth labels. Run this against HISTORICAL (which has
-- ISFRAUD) to get real, defensible numbers for your PPT/judges.
-- This does NOT touch LIVE_TRANSACTIONS or the tasks above —
-- it's a one-time backtest against labeled data.
-- ============================================================

-- Backtest each signal individually + the ensemble, all in one pass
CREATE OR REPLACE VIEW ENSEMBLE_BACKTEST AS
SELECT
    h.NAMEORIG,
    h.TXN_TS,
    h.TYPE,
    h.AMOUNT,
    h.ISFRAUD,
    CASE
        WHEN ts.TYPE_STDDEV > 0
             AND ABS( (h.AMOUNT - ts.TYPE_MEAN) / ts.TYPE_STDDEV ) > 4
        THEN 1 ELSE 0
    END AS ZSCORE_FLAG,
    CASE
        WHEN ABS( (h.OLDBALANCEORG - h.AMOUNT) - h.NEWBALANCEORIG ) > 1.0
          OR ABS( (h.OLDBALANCEDEST + h.AMOUNT) - h.NEWBALANCEDEST ) > 1.0
        THEN 1 ELSE 0
    END AS BALANCE_FLAG
FROM HISTORICAL h
LEFT JOIN TYPE_STATS ts
    ON h.TYPE = ts.TYPE;

-- Precision/recall for the z-score signal alone
SELECT
    'ZSCORE_ONLY' AS SIGNAL,
    SUM(CASE WHEN ZSCORE_FLAG = 1 AND ISFRAUD = 1 THEN 1 ELSE 0 END) AS TRUE_POSITIVES,
    SUM(CASE WHEN ZSCORE_FLAG = 1 AND ISFRAUD = 0 THEN 1 ELSE 0 END) AS FALSE_POSITIVES,
    SUM(CASE WHEN ZSCORE_FLAG = 0 AND ISFRAUD = 1 THEN 1 ELSE 0 END) AS FALSE_NEGATIVES,
    DIV0(SUM(CASE WHEN ZSCORE_FLAG = 1 AND ISFRAUD = 1 THEN 1 ELSE 0 END),
         NULLIF(SUM(CASE WHEN ZSCORE_FLAG = 1 THEN 1 ELSE 0 END), 0))     AS PRECISION,
    DIV0(SUM(CASE WHEN ZSCORE_FLAG = 1 AND ISFRAUD = 1 THEN 1 ELSE 0 END),
         NULLIF(SUM(CASE WHEN ISFRAUD = 1 THEN 1 ELSE 0 END), 0))         AS RECALL
FROM ENSEMBLE_BACKTEST

UNION ALL

-- Precision/recall for the balance-consistency signal alone
SELECT
    'BALANCE_ONLY',
    SUM(CASE WHEN BALANCE_FLAG = 1 AND ISFRAUD = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN BALANCE_FLAG = 1 AND ISFRAUD = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN BALANCE_FLAG = 0 AND ISFRAUD = 1 THEN 1 ELSE 0 END),
    DIV0(SUM(CASE WHEN BALANCE_FLAG = 1 AND ISFRAUD = 1 THEN 1 ELSE 0 END),
         NULLIF(SUM(CASE WHEN BALANCE_FLAG = 1 THEN 1 ELSE 0 END), 0)),
    DIV0(SUM(CASE WHEN BALANCE_FLAG = 1 AND ISFRAUD = 1 THEN 1 ELSE 0 END),
         NULLIF(SUM(CASE WHEN ISFRAUD = 1 THEN 1 ELSE 0 END), 0))
FROM ENSEMBLE_BACKTEST

UNION ALL

-- Precision/recall for "either signal fires" (OR logic) —
-- shows the ceiling of combining just these two new signals
SELECT
    'ZSCORE_OR_BALANCE',
    SUM(CASE WHEN (ZSCORE_FLAG = 1 OR BALANCE_FLAG = 1) AND ISFRAUD = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN (ZSCORE_FLAG = 1 OR BALANCE_FLAG = 1) AND ISFRAUD = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN (ZSCORE_FLAG = 0 AND BALANCE_FLAG = 0) AND ISFRAUD = 1 THEN 1 ELSE 0 END),
    DIV0(SUM(CASE WHEN (ZSCORE_FLAG = 1 OR BALANCE_FLAG = 1) AND ISFRAUD = 1 THEN 1 ELSE 0 END),
         NULLIF(SUM(CASE WHEN (ZSCORE_FLAG = 1 OR BALANCE_FLAG = 1) THEN 1 ELSE 0 END), 0)),
    DIV0(SUM(CASE WHEN (ZSCORE_FLAG = 1 OR BALANCE_FLAG = 1) AND ISFRAUD = 1 THEN 1 ELSE 0 END),
         NULLIF(SUM(CASE WHEN ISFRAUD = 1 THEN 1 ELSE 0 END), 0))
FROM ENSEMBLE_BACKTEST;

-- NOTE: to also include the hourly-aggregate signal in this same
-- backtest, you'd need historical ALERTS-equivalent flags computed
-- against HISTORICAL rather than LIVE_TRANSACTIONS. If time allows,
-- ask for that extension — otherwise these two numbers alone
-- (z-score + balance, individually and combined) are already a
-- legitimate, defensible metric to put in front of judges.