"""
poller.py — Fraud & Anomaly Response Agent
==========================================
Connects to Snowflake, polls ALERTS for new anomalies every ~60 seconds,
posts to Slack via webhook, and logs account-lock actions.

Environment variables (see .env.example):
  SNOWFLAKE_ACCOUNT     e.g. abc12345.us-east-1
  SNOWFLAKE_USER        your Snowflake username
  SNOWFLAKE_PASSWORD    your Snowflake password
  SNOWFLAKE_WAREHOUSE   e.g. COMPUTE_WH
  SNOWFLAKE_DATABASE    FRAUD_AGENT (default)
  SNOWFLAKE_SCHEMA      PUBLIC     (default)
  SLACK_WEBHOOK_URL     https://hooks.slack.com/services/...

Dedup & state tracking:
  Progress is stored in agent/.poller_state.json (local file).
  The file tracks:
    - last_detected_at  : max DETECTED_AT seen so far
    - seen_keys         : set of "TYPE|TXN_TS|observed_amount_rounded" already posted
  This avoids re-posting the same anomaly across runs, even if the ALERTS
  table is refreshed or rows are reordered.
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
import snowflake.connector

# ── Paths ─────────────────────────────────────────────────────────────────────
STATE_DIR  = Path(__file__).parent
STATE_FILE = STATE_DIR / ".poller_state.json"

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv(STATE_DIR.parent / ".env")  # look one level up for .env

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "FRAUD_AGENT")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL")

POLL_INTERVAL_SEC = 60


# ── State helpers ─────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"last_detected_at": None, "seen_keys": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def parse_state_timestamp(value):
    """Convert the stored string timestamp back into a datetime for comparison.
    Without this, comparing a str (loaded from JSON) to a datetime (from Snowflake)
    on the next run would raise a TypeError."""
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def dedup_key(row: tuple) -> str:
    """Composite key for deduplication across runs."""
    _, tx_type, txn_ts, observed, *_ = row
    amt = round(float(observed or 0), 2)
    return f"{tx_type}|{txn_ts}|{amt}"


# ── Slack ─────────────────────────────────────────────────────────────────────
def post_to_slack(row: tuple) -> bool:
    """Post a single alert row to Slack. Returns True on success."""
    if not SLACK_WEBHOOK_URL:
        print("[SLACK] SKIPPED — SLACK_WEBHOOK_URL not set")
        return False

    detected_at, tx_type, txn_ts, observed, lower, upper, flag = row

    color = "#ff4444" if flag in ("OVER", "ANOMALY") else "#ffaa00"

    payload = {
        "attachments": [{
            "color": color,
            "title": f"🚨 Fraud Alert — {tx_type}",
            "fields": [
                {"title": "Detected At",    "value": str(detected_at),         "short": True},
                {"title": "Transaction TS", "value": str(txn_ts),              "short": True},
                {"title": "Observed Amt",   "value": f"${float(observed or 0):,.2f}", "short": True},
                {"title": "Expected Range", "value": f"[${float(lower or 0):,.2f}, ${float(upper or 0):,.2f}]", "short": True},
                {"title": "Flag",           "value": flag,                     "short": True},
            ],
            "footer": "Fraud & Anomaly Response Agent  ·  MLH Hack Days 2026",
        }]
    }

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        print(f"[SLACK] Posted alert for {tx_type} @ {txn_ts}")
        return True
    except requests.RequestException as e:
        print(f"[SLACK] Failed to post: {e}")
        return False


# ── Account lock (mocked) ─────────────────────────────────────────────────────
def mock_lock_account(row: tuple) -> None:
    _, tx_type, txn_ts, observed, _, _, flag = row
    print(f"[ACTION] ACCOUNT_LOCKED — Anomaly detected for {tx_type} txn at {txn_ts} "
          f"(observed: ${float(observed or 0):,.2f}, flag: {flag}). "
          f"Account would be flagged for manual review in production.")


# ── Audit logger ──────────────────────────────────────────────────────────────
def log_action(conn, row: tuple, action: str, status: str) -> None:
    """Insert a row into ACTIONS_LOG. On failure print and swallow — never crash the poller."""
    try:
        detected_at, tx_type, txn_ts, observed, lower, upper, flag = row
        conn.cursor().execute(
            f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ACTIONS_LOG "
            f"(logged_at, alert_type, txn_ts, observed_amount, flag, action, status) "
            f"VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?, ?, ?)",
            (tx_type, txn_ts, float(observed or 0), flag, action, status),
        )
        conn.commit()
    except Exception as e:
        print(f"[AUDIT LOG] Failed to write: {e}")


# ── Snowflake connector ───────────────────────────────────────────────────────
def get_connection():
    return snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database  = SNOWFLAKE_DATABASE,
        schema    = SNOWFLAKE_SCHEMA,
    )


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Fraud & Anomaly Response Agent — Poller Starting")
    print("=" * 60)
    print(f"  Snowflake: {SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
    print(f"  Warehouse: {SNOWFLAKE_WAREHOUSE}")
    print(f"  Slack:     {'✅ configured' if SLACK_WEBHOOK_URL else '❌ not configured'}")
    print(f"  Poll every: {POLL_INTERVAL_SEC}s")
    print("=" * 60)

    state = load_state()
    last_detected_at = parse_state_timestamp(state.get("last_detected_at"))
    seen_keys        = set(state.get("seen_keys", []))

    while True:
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()

            # Build WHERE clause for new rows only
            if last_detected_at:
                where = f"WHERE DETECTED_AT > '{last_detected_at}' ORDER BY DETECTED_AT ASC"
            else:
                where = "ORDER BY DETECTED_AT ASC"

            cursor.execute(
                f"SELECT DETECTED_AT, TYPE, TXN_TS, OBSERVED_AMOUNT, LOWER_BOUND, UPPER_BOUND, FLAG "
                f"FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ALERTS {where}"
            )
            rows = cursor.fetchall()

            if not rows:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No new alerts — waiting {POLL_INTERVAL_SEC}s…")
                time.sleep(POLL_INTERVAL_SEC)
                continue

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(rows)} candidate row(s)")

            newly_posted = 0
            for row in rows:
                key = dedup_key(row)

                if key in seen_keys:
                    print(f"  [SKIP] Duplicate: {key}")
                    continue

                seen_keys.add(key)
                newly_posted += 1

                # Update running max timestamp
                det_at = row[0]  # DETECTED_AT
                if last_detected_at is None or det_at > last_detected_at:
                    last_detected_at = det_at

                # Post to Slack and log outcome
                slack_ok = post_to_slack(row)
                log_action(conn, row, "SLACK_NOTIFICATION", "SUCCESS" if slack_ok else "FAILED")

                # Mock account lock and log
                mock_lock_account(row)
                log_action(conn, row, "ACCOUNT_LOCKED_SIMULATED", "SUCCESS")

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Posted {newly_posted} new alert(s)")

            # Persist state
            state["last_detected_at"] = last_detected_at.isoformat() if last_detected_at else None
            state["seen_keys"]        = list(seen_keys)
            save_state(state)

        except Exception as e:
            print(f"[ERROR] {e}")
        finally:
            if conn:
                conn.close()

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()