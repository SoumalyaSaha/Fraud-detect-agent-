-- 01_run_fraud_check.sql
-- Stored procedure that runs DETECT_ANOMALIES against LIVE_TRANSACTIONS
-- and APPENDS new anomalies to ALERTS (never recreates the table).
--
-- Run this in Snowflake SQL worksheet to create:
--   1. The ALERTS table (first time only — safe to run again, uses CREATE OR REPLACE)
--   2. The run_fraud_check() Python stored procedure

-- ── ALERTS table ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE FRAUD_AGENT.PUBLIC.ALERTS (
    DETECTED_AT    TIMESTAMP_NTZ,
    TYPE           VARCHAR,
    TXN_TS         TIMESTAMP_NTZ,
    OBSERVED_AMOUNT FLOAT,
    LOWER_BOUND    FLOAT,
    UPPER_BOUND    FLOAT,
    FLAG           VARCHAR  -- 'OVER', 'UNDER', or 'ANOMALY'
);

-- ── Stored procedure ──────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE FRAUD_AGENT.PUBLIC.run_fraud_check()
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
AS $$
from datetime import datetime

def main(session):
    result = session.sql("""
        SELECT *
        FROM TABLE(FRAUD_AGENT.PUBLIC.fraud_model!DETECT_ANOMALIES(
            INPUT_DATA => TABLE(
                SELECT TYPE, TXN_TS, AVG(AMOUNT) AS AMOUNT
                FROM FRAUD_AGENT.PUBLIC.LIVE_TRANSACTIONS
                GROUP BY TYPE, TXN_TS
            ),
            SERIES_COLNAME => 'TYPE',
            TIMESTAMP_COLNAME => 'TXN_TS',
            TARGET_COLNAME => 'AMOUNT'
        ))
        WHERE Y < LOWER_BOUND OR Y > UPPER_BOUND
    """).collect()

    if not result:
        return 0

    detected_at = datetime.utcnow()
    rows = []
    for row in result:
        tx_type   = row["SERIES"]
        txn_ts    = row["TS"]
        observed  = row["Y"]
        lower     = row["LOWER_BOUND"]
        upper     = row["UPPER_BOUND"]

        if observed is not None and upper is not None and observed > upper:
            flag = "OVER"
        elif observed is not None and lower is not None and observed < lower:
            flag = "UNDER"
        else:
            flag = "ANOMALY"

        rows.append({
            "DETECTED_AT":     detected_at,
            "TYPE":            tx_type,
            "TXN_TS":          txn_ts,
            "OBSERVED_AMOUNT": observed,
            "LOWER_BOUND":     lower,
            "UPPER_BOUND":     upper,
            "FLAG":            flag
        })

    if not rows:
        return 0

    session.create_dataframe(rows, schema=[
        "DETECTED_AT", "TYPE", "TXN_TS", "OBSERVED_AMOUNT", "LOWER_BOUND", "UPPER_BOUND", "FLAG"
    ]).write.mode("append").save_as_table("ALERTS")

    return len(rows)
$$;




