# 🛒 Real-Time Indian Grocery Price Comparison

A full **Data Engineering pipeline** that scrapes live grocery prices from India's top quick-commerce platforms, processes them through Apache Kafka and Apache Spark, stores results in SQLite, and visualises everything on an interactive Streamlit dashboard.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                      DATA ENGINEERING PIPELINE                        │
│                                                                        │
│  ┌───────────┐    ┌──────────┐    ┌──────────────┐    ┌───────────┐  │
│  │ Selenium  │───▶│  Apache  │───▶│    Apache    │───▶│  SQLite   │  │
│  │  Scraper  │    │  Kafka   │    │ Spark/Pandas │    │ Database  │  │
│  │ (Phase 1) │    │(Phase 2) │    │  (Phase 3)   │    │ (Phase 4) │  │
│  └───────────┘    └──────────┘    └──────────────┘    └───────────┘  │
│        │                                                     │         │
│        ▼                                                     ▼         │
│   6 Platforms                                         Streamlit UI     │
│   · BigBasket         ┌──────────────────────────────────────────┐    │
│   · JioMart           │  Tab 1: Price Table  (ranked results)    │    │
│   · Amazon Fresh      │  Tab 2: Charts       (5 chart types)     │    │
│   · Blinkit           │  Tab 3: Projections  (savings + radar)   │    │
│   · Zepto             │  Tab 4: Pipeline     (terminal + JSON)   │    │
│   · Swiggy Instamart  │  Tab 5: History      (SQLite log)        │    │
│                       └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

| Phase | Technology | What it does |
|---|---|---|
| **1 — Scraping** | Selenium + headless Chrome | Opens a real Chrome browser, visits each platform's search page, extracts product names and prices from rendered HTML |
| **2 — Kafka** | Apache Kafka *(in-memory fallback)* | Serialises products to JSON, pushes to `raw-prices` topic, consumer reads back. Falls back to in-memory if no broker |
| **3 — Processing** | Apache Spark / Pandas | Ranks products cheapest → costliest, calculates savings, flags best deal, normalises ₹/unit |
| **4 — Storage** | SQLite (built into Python) | Writes a `searches` record + all `prices` rows. No MySQL setup needed |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10 or later
- Google Chrome installed (Selenium drives it)
- Windows / macOS / Linux

### Install

```bash
git clone <repo-url>
cd Real-Time-Online-Delivery-Price-Comparison
pip install -r requirements.txt
```

### Run

```powershell
# Windows PowerShell
$env:PYTHONUTF8 = "1"
streamlit run app.py
```

```bash
# macOS / Linux
PYTHONUTF8=1 streamlit run app.py
```

Opens at **http://localhost:8501**

> `PYTHONUTF8=1` is required on Windows so the ₹ symbol displays correctly.

---

## 📁 Project Structure

```
Real-Time-Online-Delivery-Price-Comparison/
│
├── app.py                      ← Streamlit dashboard  (entry point)
├── run.py                      ← One-click launcher: python run.py
├── requirements.txt            ← pip dependencies
│
├── src/
│   ├── scraper/                ─── PHASE 1: Selenium scrapers ───
│   │   ├── base.py             ← BaseScraper + Chrome driver factory
│   │   ├── bigbasket.py        ← BigBasket  (✅ live)
│   │   ├── jiomart.py          ← JioMart    (⚠ sometimes blocked)
│   │   ├── amazon.py           ← Amazon Fresh (✅ live)
│   │   ├── blinkit.py          ← Blinkit    (⚠ needs location auth)
│   │   ├── zepto.py            ← Zepto      (⚠ anti-bot wall)
│   │   ├── instamart.py        ← Swiggy Instamart (⚠ needs login)
│   │   ├── manager.py          ← Runs all scrapers, one Chrome instance
│   │   └── delivery.py         ← Pincode → delivery time lookup
│   │
│   ├── pipeline/               ─── PHASES 2 & 3 ───
│   │   ├── kafka_pipeline.py   ← Kafka producer + consumer
│   │   └── spark_processor.py  ← Rank · savings · ₹/unit (Spark or Pandas)
│   │
│   └── database/               ─── PHASE 4 ───
│       └── db.py               ← SQLite: searches + prices tables
│
└── data/
    └── prices.db               ← SQLite DB (auto-created on first run)
```

---

## 🌐 Platform Status

| Platform | Method | Status |
|---|---|---|
| **BigBasket** | HTML price-walk | ✅ Returns live data |
| **Amazon Fresh** | BeautifulSoup card parse | ✅ Returns live data |
| **JioMart** | __NEXT_DATA__ + HTML | ⚠️ Sometimes times out |
| **Blinkit** | Selenium | ⚠️ Requires location session |
| **Zepto** | Selenium | ⚠️ Captcha / anti-bot wall |
| **Swiggy Instamart** | Selenium | ⚠️ Requires Swiggy login |
| ~~Dunzo~~ | — | 🚫 Consumer app shut down (2024) |

Blocked platforms return an empty list and log the exact reason — **no fake data is ever shown**.

---

## 📍 Supported Pincodes

Delivery times are looked up from pincode. Recognised cities:

| City | Prefix | Example |
|---|---|---|
| Mumbai | 400xxx | `400001` |
| Delhi | 110xxx | `110001` |
| Bangalore | 560xxx | `560001` |
| Chennai | 600xxx | `600001` |
| Hyderabad | 500xxx | `500001` |
| Kolkata | 700xxx | `700001` |
| Pune | 411xxx | `411001` |
| Ahmedabad | 380xxx | `380001` |
| Gurugram | 122xxx | `122001` |
| Noida | 201xxx | `201301` |
| Jaipur | 302xxx | `302001` |
| Lucknow | 226xxx | `226001` |
| Chandigarh | 160xxx | `160001` |
| Indore | 452xxx | `452001` |

Any unrecognised pincode uses tier-2 city estimates.

---

## 📊 Dashboard Overview

### Sidebar
- Pincode + product search
- Live city detection from pincode
- Database stats (total searches, total records, DB size)
- Pipeline technology reference

### Tab 1 — 📋 Price Table
- Platform filter (multi-select)
- Ranked table: Product · Brand · Platform · Price · Delivery
- Cheapest and fastest delivery highlighted
- CSV download

### Tab 2 — 📈 Charts
| Chart | What it shows |
|---|---|
| Horizontal bar | All products ranked by price, coloured by platform |
| Box plot | Price spread / variance per platform |
| Scatter | Price vs delivery time — value-for-time view |
| Treemap | Products nested inside platforms, sized by price |
| Grouped bar | Min / Avg / Max price per platform side-by-side |

### Tab 3 — 🎯 Projections & Insights
- **Savings calculator** — select purchase frequency (daily → monthly), see monthly + annual savings vs most expensive option
- **Platform performance radar** — composite score = 50% price rank + 30% delivery speed + 20% product variety
- **Price distribution histogram** — how prices cluster per platform
- **Smart recommendation** — best platform for this specific product

### Tab 4 — 🔧 Pipeline
- Live operations terminal (collapsible)
- Per-phase timing table + bar chart
- Raw JSON side-by-side: before Spark vs after Spark

### Tab 5 — 📚 History
- All past searches from SQLite
- Searches-over-time line chart

---

## 🗄️ Database Schema

```sql
CREATE TABLE searches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    query        TEXT    NOT NULL,
    pincode      TEXT    NOT NULL,
    result_count INTEGER,
    searched_at  TEXT
);

CREATE TABLE prices (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id      INTEGER REFERENCES searches(id),
    platform       TEXT,
    product_name   TEXT,
    brand          TEXT,
    price          REAL,
    delivery_mins  INTEGER,
    rank           INTEGER,
    savings        REAL,
    is_best_deal   INTEGER,
    price_per_unit REAL,
    unit_norm      TEXT,
    source         TEXT,   -- always "LIVE"
    scraped_at     TEXT,
    processed_at   TEXT
);
```

---

## ⚡ Optional: Real Kafka + Spark

The app works without either — both fall back gracefully.

### Apache Kafka (Docker)
```bash
docker-compose up zookeeper kafka -d
```

### Apache Spark (requires Java 11)
```bash
# 1. Install Java 11 (https://adoptium.net)
# 2. Set JAVA_HOME
# 3. Then:
pip install pyspark
```

---

## 🛠️ Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Scraping | Selenium | 4.x |
| Driver management | webdriver-manager | 4.x |
| HTML parsing | BeautifulSoup4 | 4.12 |
| Message queue | Apache Kafka / kafka-python | 2.x |
| Stream processing | Apache Spark / Pandas | 3.5 / 2.x |
| Storage | SQLite | built-in |
| Dashboard | Streamlit | 1.32+ |
| Charts | Plotly Express | 5.x |
| Language | Python | 3.10+ |

---

## ⚠️ Disclaimer

This project is for **educational purposes** — demonstrating a real-time data engineering pipeline. Scraping websites may violate their Terms of Service. Use responsibly, respect rate limits, and check each platform's robots.txt before running in production.

---

*Data Engineering project — Selenium · Apache Kafka · Apache Spark · SQLite · Streamlit*
