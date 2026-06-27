"""
spark_processor.py  —  Real PySpark Transformation Pipeline
============================================================
Replaces the Pandas-based SparkProcessor in src/pipeline.

What it does:
    1. Reads raw_scraped_data from SQLite for a given search_id
    2. Runs PySpark transformations (clean, enrich, rank)
    3. Writes processed results back to SQLite → processed_data table
    4. Returns results as list of dicts (for Streamlit to display)

Usage in app.py:
    from spark_processor import SparkProcessor
    processor = SparkProcessor(log_fn=log)
    results   = processor.process(search_id=search_id)
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Callable

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, BooleanType

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "grocery_prices.db")


class SparkProcessor:

    def __init__(self, log_fn: Optional[Callable] = None, db_path: str = DB_PATH):
        self.db_path = db_path
        self.log = log_fn if log_fn else print

    # ──────────────────────────────────────────────────────────────────────────
    # PRIVATE: Get or create SparkSession (local mode)
    # ──────────────────────────────────────────────────────────────────────────
    def _get_spark(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("GroceryPricePipeline")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")   # small data, keep it fast
            .config("spark.ui.enabled", "false")            # no Spark UI clutter
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    # ──────────────────────────────────────────────────────────────────────────
    # PRIVATE: Read raw data from SQLite for a search_id
    # ──────────────────────────────────────────────────────────────────────────
    def _read_raw(self, search_id: int) -> pd.DataFrame:
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            "SELECT * FROM raw_scraped_data WHERE search_id = ?",
            conn,
            params=(search_id,)
        )
        conn.close()
        return df

    # ──────────────────────────────────────────────────────────────────────────
    # PRIVATE: Write processed data back to SQLite
    # ──────────────────────────────────────────────────────────────────────────
    def _write_processed(self, pdf: pd.DataFrame, search_id: int):
        """
        Writes to processed_data table.
        Deletes old rows for this search_id first (idempotent re-runs).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_data (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                search_id       INTEGER NOT NULL,
                platform        TEXT,
                product_name    TEXT,
                price           REAL,
                mrp             REAL,
                discount_pct    REAL,
                size_label      TEXT,
                brand           TEXT,
                delivery_mins   INTEGER,
                in_stock        INTEGER,
                product_url     TEXT,
                price_per_unit  REAL,
                unit_norm       TEXT,
                value_score     REAL,
                rank            INTEGER,
                is_best_deal    INTEGER DEFAULT 0,
                savings         REAL,
                processed_at    TEXT,
                FOREIGN KEY (search_id) REFERENCES user_searches(id)
            )
        """)

        # Delete old rows for this search_id (safe re-run)
        cursor.execute("DELETE FROM processed_data WHERE search_id = ?", (search_id,))
        conn.commit()

        pdf["processed_at"] = datetime.now().isoformat()
        allowed_cols = [
            "search_id", "platform", "product_name", "price", "mrp",
            "discount_pct", "size_label", "brand", "delivery_mins",
            "in_stock", "product_url", "price_per_unit", "unit_norm",
            "value_score", "rank", "is_best_deal", "savings", "processed_at"
        ]
        pdf = pdf[[col for col in allowed_cols if col in pdf.columns]]
        pdf.to_sql("processed_data", conn, if_exists="append", index=False)
        conn.close()

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC: Main process method — called from app.py
    # ──────────────────────────────────────────────────────────────────────────
    def process(self, search_id: int) -> List[Dict]:
        """
        Runs full PySpark transformation pipeline for a search_id.

        Returns:
            List of processed product dicts for Streamlit to display.
        """
        self.log(f"⚡  [Spark] Starting pipeline for search_id={search_id}")
        t_start = datetime.now()

        # ── Step 1: Load raw data ─────────────────────────────────────────────
        raw_pdf = self._read_raw(search_id)
        if raw_pdf.empty:
            self.log("⚠️  [Spark] No raw data found — pipeline skipped.")
            return []

        self.log(f"⚡  [Spark] Loaded {len(raw_pdf)} raw rows from SQLite")

        spark = self._get_spark()

        try:
            # ── Step 2: Create Spark DataFrame ───────────────────────────────
            df = spark.createDataFrame(raw_pdf)

            # ── Step 3: Cast column types ─────────────────────────────────────
            df = (
                df
                .withColumn("price",        F.col("price").cast(DoubleType()))
                .withColumn("mrp",          F.col("mrp").cast(DoubleType()))
                .withColumn("discount_pct", F.col("discount_pct").cast(DoubleType()))
                .withColumn("delivery_mins",F.col("delivery_mins").cast(IntegerType()))
                .withColumn("in_stock",     F.col("in_stock").cast(BooleanType()))
            )

            # ── Step 4: Drop nulls on critical columns ────────────────────────
            df = df.filter(
                F.col("product_name").isNotNull() &
                F.col("price").isNotNull() &
                (F.col("price") > 0)
            )

            # ── Step 5: Recalculate discount_pct from price + mrp ─────────────
            df = df.withColumn(
                "discount_pct",
                F.when(
                    (F.col("mrp").isNotNull()) & (F.col("mrp") > F.col("price")),
                    F.round((1 - F.col("price") / F.col("mrp")) * 100, 2)
                ).otherwise(F.col("discount_pct"))
            )

            # ── Step 6: Calculate savings (mrp - price) ───────────────────────
            df = df.withColumn(
                "savings",
                F.when(
                    (F.col("mrp").isNotNull()) & (F.col("mrp") > F.col("price")),
                    F.round(F.col("mrp") - F.col("price"), 2)
                ).otherwise(F.lit(0.0))
            )

            # ── Step 7: Parse size_label → numeric quantity + unit ────────────
            # e.g. "500g" → qty=500, unit="g"  |  "1 kg" → qty=1000, unit="g"
            df = df.withColumn(
                "qty_grams",
                F.when(
                    F.lower(F.col("size_label")).rlike(r"\d+\s*kg"),
                    F.regexp_extract(F.lower(F.col("size_label")), r"(\d+\.?\d*)\s*kg", 1).cast(DoubleType()) * 1000
                ).when(
                    F.lower(F.col("size_label")).rlike(r"\d+\s*g"),
                    F.regexp_extract(F.lower(F.col("size_label")), r"(\d+\.?\d*)\s*g", 1).cast(DoubleType())
                ).when(
                    F.lower(F.col("size_label")).rlike(r"\d+\s*ml"),
                    F.regexp_extract(F.lower(F.col("size_label")), r"(\d+\.?\d*)\s*ml", 1).cast(DoubleType())
                ).when(
                    F.lower(F.col("size_label")).rlike(r"\d+\s*l"),
                    F.regexp_extract(F.lower(F.col("size_label")), r"(\d+\.?\d*)\s*l", 1).cast(DoubleType()) * 1000
                ).when(
                    F.lower(F.col("size_label")).rlike(r"\d+\s*pc"),
                    F.regexp_extract(F.lower(F.col("size_label")), r"(\d+\.?\d*)\s*pc", 1).cast(DoubleType())
                ).otherwise(F.lit(None).cast(DoubleType()))
            )

            df = df.withColumn(
                "unit_norm",
                F.when(F.col("qty_grams").isNotNull(), "per 100g")
                .otherwise(F.lit("per pc"))
            )

            # ── Step 8: Price per unit (per 100g or per piece) ────────────────
            df = df.withColumn(
                "price_per_unit",
                F.when(
                    F.col("qty_grams").isNotNull() & (F.col("qty_grams") > 0),
                    F.round(F.col("price") / F.col("qty_grams") * 100, 2)
                ).otherwise(F.lit(None).cast(DoubleType()))
            )

            # ── Step 9: Value score (lower = better deal) ─────────────────────
            # Formula: price × (1 + delivery_penalty)
            # delivery_penalty = delivery_mins / 60 × 0.15
            df = df.withColumn(
                "value_score",
                F.when(
                    F.col("price").isNotNull(),
                    F.round(
                        F.col("price") * (
                            1 + (F.coalesce(F.col("delivery_mins"), F.lit(30)) / 60.0) * 0.15
                        ),
                        2
                    )
                ).otherwise(F.lit(None).cast(DoubleType()))
            )

            # ── Step 10: Rank by value_score (lower = rank 1) ─────────────────
            window_rank = Window.orderBy(F.col("value_score").asc_nulls_last())
            df = df.withColumn("rank", F.rank().over(window_rank))

            # ── Step 11: Flag best deal ───────────────────────────────────────
            min_score = df.agg(F.min("value_score")).collect()[0][0]
            df = df.withColumn(
                "is_best_deal",
                F.when(F.col("value_score") == min_score, F.lit(True))
                .otherwise(F.lit(False))
            )

            # ── Step 12: Drop helper column, keep final schema ────────────────
            df = df.drop("qty_grams")

            # ── Step 13: Collect back to Pandas → SQLite ──────────────────────
            result_pdf = df.toPandas()

            elapsed = round((datetime.now() - t_start).total_seconds(), 2)
            self.log(f"⚡  [Spark] Transformations done — {len(result_pdf)} rows · {elapsed}s")

            # ── Step 14: Write to processed_data table ────────────────────────
            self._write_processed(result_pdf, search_id)
            self.log(f"🗄️  [Spark] Saved {len(result_pdf)} processed rows → processed_data table")

            return result_pdf.to_dict(orient="records")
        
        except Exception as e:
            import traceback
            self.log(f"spark error:{e}")
            self.log(traceback.format_exc())
            return [] 
        
            
        finally:
            spark.stop()
            self.log("⚡  [Spark] Session closed.")