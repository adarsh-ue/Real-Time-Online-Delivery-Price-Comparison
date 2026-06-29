"""
pipeline_trigger.py  —  Auto-trigger for PySpark Pipeline
==========================================================
Listens for new rows in user_searches table (SQLite).
When a new search_id is detected → automatically fires SparkProcessor.

Run this in a SEPARATE terminal:
    python pipeline_trigger.py

It stays running in the background. Every time the user does a search
in the Streamlit app, this trigger detects the new search_id and
fires the PySpark pipeline automatically.

Architecture:
    app.py (user searches)
         ↓  writes to SQLite
    pipeline_trigger.py (detects new search_id)
         ↓  fires automatically
    spark_processor.py (PySpark transforms)
         ↓  writes processed_data to SQLite
    Streamlit reads processed_data and displays results
"""

import sqlite3
import time
import os
import sys
from datetime import datetime

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from spark_processor import SparkProcessor

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "grocery_prices.db")
POLL_INTERVAL = 2   # seconds between checks — keep low for snappy response

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def get_latest_search_id() -> int:
    """Returns the highest search_id currently in user_searches."""
    try:
        conn = sqlite3.connect(DB_PATH)
        row  = conn.execute("SELECT MAX(id) FROM user_searches").fetchone()
        conn.close()
        return row[0] if row[0] else 0
    except Exception:
        return 0

def is_raw_data_ready(search_id: int) -> bool:
    """
    Returns True only when raw_scraped_data rows exist for this search_id.
    This prevents the pipeline from firing before scraping is done.
    """
    try:
        conn   = sqlite3.connect(DB_PATH)
        count  = conn.execute(
            "SELECT COUNT(*) FROM raw_scraped_data WHERE search_id = ?",
            (search_id,)
        ).fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False

def already_processed(search_id: int) -> bool:
    """Returns True if this search_id already has processed_data rows."""
    try:
        conn  = sqlite3.connect(DB_PATH)
        count = conn.execute(
            "SELECT COUNT(*) FROM processed_data WHERE search_id = ?",
            (search_id,)
        ).fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False

# ── Main loop ─────────────────────────────────────────────────────────────────
def run_trigger():
    log("🚀  Pipeline trigger started — watching grocery_prices.db")
    log(f"📂  DB path: {DB_PATH}")
    log(f"⏱️   Polling every {POLL_INTERVAL}s for new searches\n")

    last_seen_id = get_latest_search_id()
    log(f"📌  Starting from search_id={last_seen_id} (existing data ignored)\n")

    while True:
        try:
            latest_id = get_latest_search_id()

            if latest_id > last_seen_id:
                # New search detected
                new_ids = list(range(last_seen_id + 1, latest_id + 1))
                log(f"🔔  New search detected! search_id(s): {new_ids}")

                for search_id in new_ids:

                    # Wait until raw data is actually written
                    log(f"⏳  Waiting for raw data → search_id={search_id}")
                    waited = 0
                    while not is_raw_data_ready(search_id):
                        time.sleep(1)
                        waited += 1
                        if waited >120:   # timeout after 120s
                            log(f"⚠️   Timeout waiting for raw data → search_id={search_id}. Skipping.")
                            break
                    else:
                        # raw data is ready — skip if already processed
                        if already_processed(search_id):
                            log(f"ℹ️   search_id={search_id} already processed. Skipping.")
                            continue

                        log(f"⚡  Firing PySpark pipeline → search_id={search_id}")
                        try:
                            processor = SparkProcessor(log_fn=log, db_path=DB_PATH)
                            results   = processor.process(search_id=search_id)
                            log(f"✅  Pipeline complete → {len(results)} rows processed for search_id={search_id}\n")
                        except Exception as e:
                            log(f"❌  Pipeline error for search_id={search_id}: {e}\n")

                last_seen_id = latest_id

        except KeyboardInterrupt:
            log("\n🛑  Trigger stopped by user.")
            break
        except Exception as e:
            log(f"⚠️   Trigger loop error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run_trigger()