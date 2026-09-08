-- 02_stream_simulator.sql
-- Stored procedure that streams batches of rows from STREAM_SOURCE into
-- LIVE_TRANSACTIONS, paced by the 'step' column so transactions appear
-- to arrive over time.
--
-- Uses a watermark table (STREAM_WATERMARK) to track the last inserted step,
-- avoiding a full-table scan of LIVE_TRANSACTIONS on every run.

-- ── Watermark table ───────────────────────────────────────────────────────────
-- Stores the highest 'step' value already streamed into LIVE_TRANSACTIONS.
-- Only ever one row.
CREATE OR REPLACE TABLE FRAUD_AGENT.PUBLIC.STREAM_WATERMARK (
    last_step NUMBER PRIMARY KEY
);

-- Seed watermark to 180 (the boundary between HISTORICAL and STREAM_SOURCE)
INSERT INTO FRAUD_AGENT.PUBLIC.STREAM_WATERMARK (last_step)
VALUES (180);

CREATE OR REPLACE PROCEDURE FRAUD_AGENT.PUBLIC.stream_simulator(BATCH_SIZE NUMBER)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
AS $$
def main(session, batch_size=50):
    wm_row = session.sql("SELECT last_step FROM FRAUD_AGENT.PUBLIC.STREAM_WATERMARK").collect()
    last_step = wm_row[0]["LAST_STEP"] if wm_row else 180

    df = session.sql(f"""
        SELECT *
        FROM FRAUD_AGENT.PUBLIC.STREAM_SOURCE
        WHERE STEP > {last_step}
        ORDER BY STEP
        LIMIT {batch_size}
    """)

    rows = df.collect()
    if not rows:
        return 0

    n = len(rows)

    df.write.mode("append").save_as_table("FRAUD_AGENT.PUBLIC.LIVE_TRANSACTIONS")

    new_last_step = max(row["STEP"] for row in rows)
    session.sql("DELETE FROM FRAUD_AGENT.PUBLIC.STREAM_WATERMARK").collect()
    session.sql(f"INSERT INTO FRAUD_AGENT.PUBLIC.STREAM_WATERMARK (last_step) VALUES ({int(new_last_step)})").collect()

    return n
$$;