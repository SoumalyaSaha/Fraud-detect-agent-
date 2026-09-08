# 🛡️ Fraud & Anomaly Response Agent

> MLH Hack Days 2026

A real-time fraud detection and response pipeline built on **Snowflake** — from live transaction streaming through ML anomaly detection to Slack incident notifications.

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ STREAM_SOURCE   │────▶│ LIVE_TRANSACTIONS │────▶│    ALERTS       │
│ (PaySim step>180)│     │ (Snowflake table) │     │ (anomalies)     │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
       ▲                       ▲                           │
       │ Task (60s)            │ Task (60s)                │
  ┌────┴────┐           ┌─────┴──────┐            ┌───────┴───────┐
  │STREAM_  │           │run_fraud_  │            │  poller.py    │
  │SIM_TASK │           │check()     │            │ (local Python)│
  └─────────┘           └────────────┘            └───────┬───────┘
                                                           │
                                                    ┌──────┴──────┐
                                                    │  Slack      │
                                                    │  webhook    │
                                                    │  + log      │
                                                    └─────────────┘
                                                           ▲
                                                    ┌──────┴──────┐
                                                    │ Streamlit   │
                                                    │ Dashboard   │
                                                    └─────────────┘
```

### Data Flow

1. **PaySim Dataset** (~3M synthetic transactions) is loaded into `FRAUD_AGENT.PUBLIC.RAW_TRANSACTIONS` with a `TXN_TS` timestamp column.
2. The data is split:
   - `HISTORICAL` (step ≤ 180) — used to train the ML model
   - `STREAM_SOURCE` (step > 180) — used to simulate live transaction flow
3. A **Snowflake ML anomaly detection model** (`fraud_model`) is trained on `HISTORICAL_AGG` using `SERIES_COLNAME=TYPE`, `TIMESTAMP_COLNAME=TXN_TS`, `TARGET_COLNAME=AMOUNT`.
4. **Snowflake Tasks** (scheduled every 60 seconds) drive the pipeline:
   - `STREAM_SIM_TASK` → calls `stream_simulator()` to batch-insert transactions from `STREAM_SOURCE` into `LIVE_TRANSACTIONS`
   - `FRAUD_CHECK_TASK` → calls `run_fraud_check()` to run `DETECT_ANOMALIES` on live data and append results to `ALERTS`
5. An **external Python poller** (`agent/poller.py`) connects *into* Snowflake, polls `ALERTS` for new rows, posts to Slack, and logs account-lock actions.
6. A **Streamlit dashboard** (`streamlit_app.py`) provides a real-time view of live transactions and the alert log.

---

## Why the Poller Runs Outside Snowflake

**Snowflake trial accounts do not support External Access Integrations (EAI).** EAI is the mechanism that allows stored procedures and tasks to make outbound network calls (e.g., to Slack webhooks, APIs, etc.).

Our solution: the poller runs as a **local Python process** that connects *into* Snowflake using `snowflake-connector-python`. This is a one-way (inbound) connection and is fully supported on trial accounts. The alternative — trying to post from inside a Snowflake stored procedure — would fail on a trial account.

This design decision is intentional and demonstrated in the code: all Snowflake-side code makes zero external network calls.

---

## Project Structure

```
fraud-agent/
├── sql/
│   ├── 01_run_fraud_check.sql   # ALERTS table + run_fraud_check() stored proc
│   ├── 02_stream_simulator.sql  # STREAM_WATERMARK table + stream_simulator() proc
│   ├── 03_tasks.sql             # STREAM_SIM_TASK + FRAUD_CHECK_TASK definitions
│   └── 04_actions_log.sql       # ACTIONS_LOG table for poller audit trail
├── agent/
│   └── poller.py                # Local poller: Snowflake → Slack + account lock log + ACTIONS_LOG
├── streamlit_app.py             # SiS-compatible dashboard
├── requirements.txt             # Python dependencies for poller
├── .env.example                 # Environment variable template
├── .gitignore
└── README.md
```

---

## Setup

### 1. Snowflake — Run the SQL scripts in order

```sql
-- In your Snowflake worksheet, run these in order:
@sql/01_run_fraud_check.sql
@sql/02_stream_simulator.sql
@sql/03_tasks.sql
@sql/04_actions_log.sql
```

This creates:
- `ALERTS` table (appended to by `run_fraud_check()`)
- `STREAM_WATERMARK` table (tracks last streamed `step`)
- `ACTIONS_LOG` table (written by the poller after each alert action)
- `run_fraud_check()` and `stream_simulator()` stored procedures
- `STREAM_SIM_TASK` and `FRAUD_CHECK_TASK` (both running every 60 seconds)

### 2. Local — Configure environment

```bash
cd fraud-agent
cp .env.example .env
# Edit .env and fill in your actual Snowflake credentials and Slack webhook URL
```

### 3. Local — Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the poller

```bash
python agent/poller.py
```

The poller will:
- Connect to Snowflake
- Poll `ALERTS` every 60 seconds for new rows
- Post formatted alerts to Slack
- Log `[ACTION] ACCOUNT_LOCKED` for each new anomaly
- Persist state locally to avoid re-posting duplicates

### 5. Run the Streamlit dashboard

```bash
# Local:
streamlit run streamlit_app.py

# Streamlit in Snowflake (SiS):
# Deploy via Snowflake's Streamlit UI — the app uses only SiS-compatible packages.
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| `ALERTS` is appended, not recreated | Stable rows allow TS-based dedup in the poller |
| Watermark table for stream simulation | Faster than scanning `LIVE_TRANSACTIONS` on every batch |
| Watermark update via DELETE+INSERT | Simplest approach for a single-row table; avoids MERGE edge cases |
| Poller runs outside Snowflake | Trial accounts block outbound EAI; inbound connector is fine |
| Composite dedup key (`TYPE\|TXN_TS\|amount`) | Guards against re-posting the same anomaly across runs |
| Streamlit uses Snowpark only | No pip installs needed for SiS deployment |

---

## Snowflake Schema Reference

| Object | Type | Purpose |
|--------|------|---------|
| `RAW_TRANSACTIONS` | Table | Original PaySim data (~3M rows) |
| `HISTORICAL` | Table | step ≤ 180 (model training) |
| `STREAM_SOURCE` | Table | step > 180 (live simulation) |
| `HISTORICAL_AGG` | View | Aggregated historical data |
| `STREAM_AGG` | View | Aggregated stream data |
| `LIVE_TRANSACTIONS` | Table | Actively streamed transactions |
| `ALERTS` | Table | Flagged anomalies (appended) |
| `ACTIONS_LOG` | Table | Poller audit trail — every Slack post and simulated lock |
| `STREAM_WATERMARK` | Table | Tracks last streamed `step` value |
| `fraud_model` | ML Model | Anomaly detection model |
| `STREAM_SIM_TASK` | Task | 60s schedule → `stream_simulator()` |
| `FRAUD_CHECK_TASK` | Task | 60s schedule → `run_fraud_check()` |

---

*Built for MLH Hack Days 2026*
