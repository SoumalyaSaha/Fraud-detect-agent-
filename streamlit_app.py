"""
streamlit_app.py — Fraud & Anomaly Response Agent Dashboard
============================================================
Streamlit in Snowflake (SiS) compatible dashboard.
Uses only packages available in the SiS environment: streamlit, pandas,
snowflake.snowpark.

Run locally:  streamlit run streamlit_app.py
Run in SiS:   deploy as a Streamlit app in your Snowflake account.

Environment variables (same as poller.py):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

import os
from dotenv import load_dotenv
load_dotenv() 
import streamlit as st
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col


# ── Connection ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_snowpark_session() -> Session:
    params = {
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.getenv("SNOWFLAKE_DATABASE", "FRAUD_AGENT"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }
    return Session.builder.configs(params).create()


def load_live_transactions(session: Session, limit: int = 50) -> pd.DataFrame:
    df = (
        session.table("LIVE_TRANSACTIONS")
        .select(col("STEP"), col("TYPE"), col("AMOUNT"), col("TXN_TS"))
        .order_by(col("TXN_TS").desc())
        .limit(limit)
        .to_pandas()
    )
    return df


def load_alerts(session: Session, limit: int = 100, type_filter: list = None, flag_filter: list = None) -> pd.DataFrame:
    q = (
        session.table("ALERTS")
        .select(
            col("DETECTED_AT"), col("TYPE"), col("TXN_TS"),
            col("OBSERVED_AMOUNT"), col("LOWER_BOUND"), col("UPPER_BOUND"), col("FLAG"),
        )
        .order_by(col("DETECTED_AT").desc())
        .limit(limit)
    )
    if type_filter:
        q = q.filter(col("TYPE").isin(type_filter))
    if flag_filter:
        q = q.filter(col("FLAG").isin(flag_filter))
    return q.to_pandas()


def load_actions_log(session: Session, limit: int = 100, action_filter: list = None, status_filter: list = None, alert_type_filter: list = None) -> pd.DataFrame:
    q = (
        session.table("ACTIONS_LOG")
        .select(
            col("logged_at"), col("alert_type"), col("txn_ts"),
            col("observed_amount"), col("flag"), col("action"), col("status"),
        )
        .order_by(col("logged_at").desc())
        .limit(limit)
    )
    if action_filter:
        q = q.filter(col("action").isin(action_filter))
    if status_filter:
        q = q.filter(col("status").isin(status_filter))
    if alert_type_filter:
        q = q.filter(col("alert_type").isin(alert_type_filter))
    return q.to_pandas()


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Fraud & Anomaly Agent", layout="wide", page_icon="🛡️")

st.title("🛡️ Fraud & Anomaly Response Agent")
st.caption("MLH Hack Days 2026  ·  Powered by Snowflake ML")

session = get_snowpark_session()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_txns, tab_alerts, tab_audit, tab_info = st.tabs(["📡 Live Transactions", "🚨 Alert Log", "📋 Audit Log", "ℹ️ About"])

# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_task_state(session: Session, task_name: str) -> str:
    """Return the current STATE of a Snowflake task (RUNNING | SUSPENDED | …)."""
    rows = session.sql(f"SHOW TASKS LIKE '{task_name}'").collect()
    return rows[0]["state"] if rows else "UNKNOWN"


def _toggle_task(session: Session, task_name: str, target: str) -> str:
    """Suspend or resume a task. Returns the new state, or 'ERROR: …' on failure."""
    try:
        session.sql(f"ALTER TASK {task_name} {target}").collect()
        return target.upper()
    except Exception as e:
        msg = str(e)
        if "already" in msg.lower() or "not found" in msg.lower():
            return "ALREADY " + target.upper()
        return f"ERROR: {msg}"


# ── Initialise mode in session state ─────────────────────────────────────────
if "mode" not in st.session_state:
    st.session_state["mode"] = "Simulated"

_current_mode = st.session_state["mode"]
_sim_task_state = _get_task_state(session, "STREAM_SIM_TASK")

# ── Tab 1: Live Transactions ─────────────────────────────────────────────────
with tab_txns:
    col_toggle, col_state = st.columns([3, 2])
    with col_toggle:
        st.radio(
            "Mode",
            ["Simulated", "Live"],
            horizontal=True,
            key="mode_picker",
            label_visibility="collapsed",
        )
    with col_state:
        if _sim_task_state == "RUNNING":
            st.caption(f"Stream task: **RUNNING**  ⚙️")
        elif _sim_task_state == "SUSPENDED":
            st.caption(f"Stream task: **SUSPENDED**  ⏸️")
        else:
            st.caption(f"Stream task: {_sim_task_state}")

    # Sync session_state when the radio value changes
    if st.session_state.mode_picker != st.session_state.mode:
        new_mode = st.session_state.mode_picker
        old_mode = st.session_state.mode
        st.session_state.mode = new_mode
        if new_mode == "Simulated" and old_mode == "Live":
            result = _toggle_task(session, "STREAM_SIM_TASK", "RESUME")
            if result.startswith("ERROR"):
                st.info(result)
            elif result != "ALREADY RUNNING":
                st.toast("Simulated feed resumed ⚙️")
        elif new_mode == "Live" and old_mode == "Simulated":
            result = _toggle_task(session, "STREAM_SIM_TASK", "SUSPEND")
            if result.startswith("ERROR"):
                st.info(result)
            elif result != "ALREADY SUSPENDED":
                st.toast("Simulated feed paused ⏸️")
        st.rerun()

    if st.session_state.mode == "Live":
        st.caption("Simulated data feed paused. Enter a transaction below to test live fraud detection.")

        # ── Manual-insert form ────────────────────────────────────────────
        with st.form("insert_txn"):
            c1, c2, c3 = st.columns(3)
            with c1:
                _type = st.selectbox(
                    "Type",
                    ["CASH_OUT", "CASH_IN", "DEBIT", "PAYMENT", "TRANSFER"],
                )
            with c2:
                _amount = st.number_input("Amount ($)", min_value=0.01, step=1.00)
            with c3:
                _txn_ts = st.text_input("Transaction TS (optional, defaults to now)", placeholder="e.g. 2026-09-09 12:00:00")
            submitted = st.form_submit_button("Insert Transaction", use_container_width=True)

            if submitted:
                ts_val = f"'{_txn_ts}'" if _txn_ts.strip() else "CURRENT_TIMESTAMP()"
                session.sql(
                    f"INSERT INTO LIVE_TRANSACTIONS (TYPE, AMOUNT, TXN_TS) "
                    f"VALUES ('{_type}', {_amount}, {ts_val})"
                ).collect()
                st.success(f"Inserted {_type} — ${_amount:,.2f}")
                st.rerun()

    # ── Table + metrics (shown in both modes) ──────────────────────────────
    st.subheader("Recent Transactions" if st.session_state.mode == "Simulated" else "Live Transactions")
    df_txns = load_live_transactions(session, limit=50)

    if not df_txns.empty:
        if "AMOUNT" in df_txns.columns:
            df_txns["AMOUNT"] = df_txns["AMOUNT"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        if "TXN_TS" in df_txns.columns:
            df_txns["TXN_TS"] = pd.to_datetime(df_txns["TXN_TS"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        st.dataframe(df_txns, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(df_txns)} most recent transactions")
    else:
        st.info("No transactions in LIVE_TRANSACTIONS yet."
                " Switch to Simulated mode or insert one manually above.")

    col1, col2, col3 = st.columns(3)
    total = session.table("LIVE_TRANSACTIONS").count()
    col1.metric("Total Transactions", f"{total:,}")
    col2.metric("Anomaly Types", "5")
    if _sim_task_state == "RUNNING":
        col3.metric("Stream Task", "Running ✅")
    else:
        col3.metric("Stream Task", "Paused ⏸️")

# ── Tab 2: Alert Log ─────────────────────────────────────────────────────────
with tab_alerts:
    st.subheader("Anomaly Alerts")
    st.caption("Flagged by SNOWFLAKE.ML.ANOMALY_DETECTION.fraud_model — appended in real time")

    _initial = load_alerts(session, limit=1)
    f_type  = st.multiselect("Filter by TYPE", sorted(_initial["TYPE"].unique().tolist()) if not _initial.empty else [], default=[], key="alert_type")
    f_flag  = st.multiselect("Filter by FLAG", sorted(_initial["FLAG"].unique().tolist()) if not _initial.empty else [], default=[], key="alert_flag")
    df_alerts = load_alerts(session, limit=100, type_filter=f_type or None, flag_filter=f_flag or None)

    if not df_alerts.empty:
        for col_name in ("OBSERVED_AMOUNT", "LOWER_BOUND", "UPPER_BOUND"):
            if col_name in df_alerts.columns:
                df_alerts[col_name] = df_alerts[col_name].apply(
                    lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A"
                )
        if "DETECTED_AT" in df_alerts.columns:
            df_alerts["DETECTED_AT"] = pd.to_datetime(df_alerts["DETECTED_AT"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        if "TXN_TS" in df_alerts.columns:
            df_alerts["TXN_TS"] = pd.to_datetime(df_alerts["TXN_TS"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        st.dataframe(df_alerts, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(df_alerts)} most recent alerts")
    else:
        st.info("No alerts yet. Anomalies will appear here once the FRAUD_CHECK_TASK flags them.")

    if not df_alerts.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Alerts", len(df_alerts))
        flag_counts = df_alerts["FLAG"].value_counts().to_dict() if "FLAG" in df_alerts.columns else {}
        c2.metric("OVER range", flag_counts.get("OVER", 0))
        c3.metric("UNDER range", flag_counts.get("UNDER", 0))

# ── Tab 3: Audit Log ─────────────────────────────────────────────────────────
with tab_audit:
    st.subheader("Audit Log")
    st.caption("Every action taken by the poller — Slack posts and simulated account locks")

    f_action, f_status, f_atype = st.multiselect("Filter by Action", ["SLACK_NOTIFICATION", "ACCOUNT_LOCKED_SIMULATED"], default=[], key="audit_action"), st.multiselect("Filter by Status", ["SUCCESS", "FAILED"], default=[], key="audit_status"), st.multiselect("Filter by Alert Type", [], default=[], key="audit_alert_type")
    df_audit = load_actions_log(session, limit=100, action_filter=f_action or None, status_filter=f_status or None, alert_type_filter=f_atype or None)

    if not df_audit.empty:
        for col_name in ("observed_amount",):
            if col_name in df_audit.columns:
                df_audit[col_name] = df_audit[col_name].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        if "logged_at" in df_audit.columns:
            df_audit["logged_at"] = pd.to_datetime(df_audit["logged_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        if "txn_ts" in df_audit.columns:
            df_audit["txn_ts"] = pd.to_datetime(df_audit["txn_ts"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        st.dataframe(df_audit, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(df_audit)} most recent actions")
    else:
        st.info("No actions logged yet. Start the poller to populate this table.")

# ── Tab 3: About ─────────────────────────────────────────────────────────────
with tab_info:
    st.markdown("""
    ### Architecture
    1. **PaySim Dataset** — ~3M synthetic transactions loaded into Snowflake (`RAW_TRANSACTIONS`).
       Split into `HISTORICAL` (step ≤ 180) for training and `STREAM_SOURCE` (step > 180) for simulating live flow.
    2. **ML Anomaly Detection** — `SNOWFLAKE.ML.ANOMALY_DETECTION.fraud_model` trained on `HISTORICAL_AGG`
       with `SERIES_COLNAME=TYPE`, `TIMESTAMP_COLNAME=TXN_TS`, `TARGET_COLNAME=AMOUNT`.
    3. **Snowflake Tasks** — Two scheduled tasks (every 60 s):
       - `STREAM_SIM_TASK` → calls `stream_simulator()` to batch-insert transactions into `LIVE_TRANSACTIONS`
       - `FRAUD_CHECK_TASK` → calls `run_fraud_check()` to run `DETECT_ANOMALIES` and append to `ALERTS`
    4. **External Poller** (`agent/poller.py`) — connects *into* Snowflake, polls `ALERTS` for new rows,
       posts to Slack, and logs account-lock actions.
    5. **Streamlit Dashboard** — real-time visualization of live transactions, the alert log, and an Audit Log tab showing every action taken by the poller.

    ### Why the Poller Runs Outside Snowflake
    **Snowflake trial accounts do not support External Access Integrations (EAI).**
    EAI is the mechanism that allows stored procedures and tasks to make outbound
    network calls (e.g. to Slack webhooks or APIs). Our solution runs the poller as a
    local Python process that connects *into* Snowflake using `snowflake-connector-python`
    — a one-way (inbound) connection fully supported on trial accounts.
    """)
