"""
database_storage.py  —  Data Storage Layer
===========================================
Stores only:
  1. user_searches     → what the user typed (query, pincode, city)
  2. raw_scraped_data  → products scraped by Selenium

Usage in app.py:
    from database_storage import DatabaseStorage
    db = DatabaseStorage()
    search_id = db.store_search(query="eggs", pincode="560001", city="Bangalore")
    db.store_raw_data(raw_data, search_id=search_id)
"""

import sqlite3
import os
from datetime import datetime
from typing import List, Dict, Optional

# ── Database file path (same folder as app.py) ────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "grocery_prices.db")


class DatabaseStorage:

    def __init__(self, db_path: str = DB_PATH, log_fn=None):
        self.db_path = db_path
        # Use Streamlit terminal logger if passed, else print
        self.log = log_fn if log_fn else print
        self._create_tables()

    # ──────────────────────────────────────────────────────────────────────────
    # PRIVATE: Create tables
    # ──────────────────────────────────────────────────────────────────────────
    def _create_tables(self):
        """Creates user_searches and raw_scraped_data tables if not exist."""
        with self._connect() as conn:
            cursor = conn.cursor()

            # ── Table 1: User Searches ─────────────────────────────────────
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_searches (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    query       TEXT    NOT NULL,
                    pincode     TEXT    NOT NULL,
                    city        TEXT,
                    searched_at TEXT    DEFAULT (datetime('now'))
                )
            """)

            # ── Table 2: Raw Scraped Data ──────────────────────────────────
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_scraped_data (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    search_id     INTEGER NOT NULL,
                    platform      TEXT,
                    product_name  TEXT,
                    price         REAL,
                    mrp           REAL,
                    discount_pct  REAL,
                    size_label    TEXT,
                    brand         TEXT,
                    delivery_mins INTEGER,
                    in_stock      INTEGER DEFAULT 1,
                    product_url   TEXT,
                    scraped_at    TEXT,
                    FOREIGN KEY (search_id) REFERENCES user_searches(id)
                )
            """)

            conn.commit()
            self.log("✅  DB tables ready: user_searches, raw_scraped_data")

    # ──────────────────────────────────────────────────────────────────────────
    # PRIVATE: Connection helper
    # ──────────────────────────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC: Store user search
    # ──────────────────────────────────────────────────────────────────────────
    def store_search(self, query: str, pincode: str, city: Optional[str] = None) -> int:
        """
        Saves what the user typed into the search bar.

        Returns:
            search_id (int) — use this to link raw_scraped_data rows
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_searches (query, pincode, city)
                VALUES (?, ?, ?)
            """, (query, pincode, city))
            conn.commit()
            search_id = cursor.lastrowid
            self.log(f"🔍  Search stored → id={search_id} query='{query}' pincode={pincode} city={city}")
            return search_id

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC: Store raw scraped data
    # ──────────────────────────────────────────────────────────────────────────
    def store_raw_data(self, raw_data: List[Dict], search_id: int) -> int:
        """
        Saves scraped product list linked to a search.

        Parameters:
            raw_data  : list of product dicts from ScraperManager
            search_id : id returned by store_search()

        Returns:
            Number of rows inserted
        """
        if not raw_data:
            self.log("⚠️  store_raw_data: nothing to store.")
            return 0

        rows_inserted = 0
        with self._connect() as conn:
            cursor = conn.cursor()
            for product in raw_data:
                try:
                    cursor.execute("""
                        INSERT INTO raw_scraped_data (
                            search_id, platform, product_name, price,
                            mrp, discount_pct, size_label, brand,
                            delivery_mins, in_stock, product_url, scraped_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        search_id,
                        product.get("platform"),
                        product.get("product_name"),
                        product.get("price"),
                        product.get("mrp"),
                        product.get("discount_pct"),
                        product.get("size_label"),
                        product.get("brand"),
                        product.get("delivery_mins"),
                        int(product.get("in_stock", True)),
                        product.get("product_url"),
                        product.get("scraped_at", datetime.now().isoformat()),
                    ))
                    rows_inserted += 1
                except Exception as e:
                    self.log(f"⚠️  Row insert error: {e}")

            conn.commit()

        self.log(f"🗄️  Stored {rows_inserted} products for search_id={search_id}")
        return rows_inserted

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC: Read search history (for Streamlit history tab)
    # ──────────────────────────────────────────────────────────────────────────
    def get_search_history(self, limit: int = 50) -> List[Dict]:
        """Returns all past user searches."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    s.id,
                    s.query,
                    s.pincode,
                    s.city,
                    s.searched_at,
                    COUNT(r.id) AS total_products
                FROM user_searches s
                LEFT JOIN raw_scraped_data r ON r.search_id = s.id
                GROUP BY s.id
                ORDER BY s.searched_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC: Read raw data for a search (Databricks reads this)
    # ──────────────────────────────────────────────────────────────────────────
    def get_raw_data(self, search_id: int) -> List[Dict]:
        """
        Fetches raw scraped products for a given search_id.
        Databricks can read this to clean and transform.
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM raw_scraped_data
                WHERE search_id = ?
                ORDER BY id ASC
            """, (search_id,))
            return [dict(row) for row in cursor.fetchall()]