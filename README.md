# 🛒 Real-Time Indian Grocery Price Comparison

A complete **Data Engineering pipeline** that scrapes live grocery prices from India's top quick-commerce platforms in real time, processes them through **Apache Kafka** and **Apache Spark**, stores results in **SQLite**, and presents everything on an interactive **Streamlit** dashboard.

> ✅ **100% real-time data** — no cached datasets, no fake prices. Every search hits live websites via headless Chrome.

---

## 🏗️ Pipeline Architecture

```
User enters product + pincode
         │
         ▼
┌─────────────────────┐
│  PHASE 1: SCRAPING  │  Selenium headless Chrome
│  ─────────────────  │  visits 4 platforms simultaneously
│  Amazon Fresh       │  using one shared browser instance
│  Blinkit            │  BeautifulSoup parses HTML / __NEXT_DATA__
│  BigBasket          │  localStorage/cookie injection for location
│  Zepto              │
└────────┬────────────┘
         │  raw product dicts (name, price, MRP, size, platform)
         ▼
┌─────────────────────┐
│  PHASE 2: KAFKA     │  Real broker at localhost:9092 (KRaft mode)
│  ─────────────────  │  Producer → "raw-prices" topic
│  Producer           │  Offset snapshot before produce
│  Consumer           │  Consumer seeks to exact offset (no stale msgs)
│  Topic: raw-prices  │  Graceful in-memory fallback if broker down
└────────┬────────────┘
         │  JSON-serialised messages consumed back
         ▼
┌─────────────────────┐
│  PHASE 3: SPARK     │  Apache Spark 4.1.2  local[2]
│  ─────────────────  │  Window functions: rank(), max()
│  Rank cheapest→     │  price_per_unit (₹/100g, ₹/100mL, ₹/pc)
│  ₹/unit normalise   │  savings vs most expensive option
│  Flag best deal     │  is_best_deal flag per product group
│  Pandas fallback    │  Same logic if PySpark not installed
└────────┬────────────┘
         │  processed records
         ▼
┌─────────────────────┐
│  PHASE 4: SQLITE    │  Built into Python — no setup needed
│  ─────────────────  │  Table: searches (query, pincode, timestamp)
│  searches table     │  Table: prices   (all product records)
│  prices table       │  Auto-created on first run
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  STREAMLIT UI       │  http://localhost:8501
│  ─────────────────  │  Live terminal · Results table · Bar chart
│  Results table      │  Pipeline timings · Raw JSON viewer
│  Price bar chart    │  Search history (SQLite log)
│  Pipeline timings   │  CSV + JSON download
│  Raw JSON viewer    │
└─────────────────────┘
```

---

## 🖥️ Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Python** | 3.10 or later | [python.org](https://python.org) |
| **Java (JDK)** | 11 or later | [adoptium.net](https://adoptium.net) — required for Apache Spark |
| **Google Chrome** | Any recent | Selenium drives it in headless mode |
| **Apache Kafka** | 3.9+ | Only needed for real Kafka (see setup below) |

> **Windows note:** Set `JAVA_HOME` to your JDK folder after installing Java.

---

## 🚀 Installation — Fresh Device

### Step 1 — Clone the repo

```bash
git clone https://github.com/<your-username>/Real-Time-Online-Delivery-Price-Comparison.git
cd Real-Time-Online-Delivery-Price-Comparison
```

### Step 2 — Install Python dependencies

```bash
pip install -r requirements.txt
```

> This installs: `streamlit`, `selenium`, `beautifulsoup4`, `kafka-python`, `pyspark`, `plotly`, and others.

### Step 3 — Install Apache Kafka (Windows, no Docker)

```powershell
# Download Kafka 3.9.2
Invoke-WebRequest -Uri "https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz" `
    -OutFile "$env:USERPROFILE\Downloads\kafka.tgz" -UseBasicParsing

# Extract to home folder
tar -xzf "$env:USERPROFILE\Downloads\kafka.tgz" -C "$env:USERPROFILE"
Rename-Item "$env:USERPROFILE\kafka_2.13-3.9.2" "$env:USERPROFILE\kafka"
```

Fix the Windows 11 `wmic` issue in the Kafka startup script:

```powershell
# Open the file and replace the wmic block:
# Find this:
#   wmic os get osarchitecture | find /i "32-bit" >nul 2>&1
# Replace the whole IF block with:
#   set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
```

Format Kafka storage (KRaft mode — no ZooKeeper needed):

```powershell
$uuid = & "$env:USERPROFILE\kafka\bin\windows\kafka-storage.bat" random-uuid
# Edit config: set log.dirs=C:/Users/<you>/kafka-logs in:
#   %USERPROFILE%\kafka\config\kraft\server.properties
& "$env:USERPROFILE\kafka\bin\windows\kafka-storage.bat" format `
    --config "$env:USERPROFILE\kafka\config\kraft\server.properties" `
    --cluster-id $uuid
```

### Step 4 — Install winutils (for Spark on Windows)

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\hadoop\bin"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" `
    -OutFile "$env:USERPROFILE\hadoop\bin\winutils.exe" -UseBasicParsing
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll" `
    -OutFile "$env:USERPROFILE\hadoop\bin\hadoop.dll" -UseBasicParsing
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "$env:USERPROFILE\hadoop", "User")
```

### Step 5 — Run the app

**Option A — One-click launcher (recommended):**

```
Double-click  START_APP.bat
```

This automatically starts Kafka, waits for the broker, sets env vars, then launches Streamlit at `http://localhost:8501`.

**Option B — Manual:**

```powershell
# Terminal 1 — Start Kafka
$env:KAFKA_HEAP_OPTS = "-Xmx1G -Xms1G"
& "$env:USERPROFILE\kafka\bin\windows\kafka-server-start.bat" `
  "$env:USERPROFILE\kafka\config\kraft\server.properties"

# Terminal 2 — Start app
$env:PYTHONIOENCODING = "utf-8"
$env:HADOOP_HOME = "$env:USERPROFILE\hadoop"
streamlit run app.py
```

---

## 📁 Project Structure

```
Real-Time-Online-Delivery-Price-Comparison/
│
├── app.py                        ← Streamlit dashboard (entry point)
├── START_APP.bat                 ← One-click launcher (Kafka + Streamlit)
├── requirements.txt              ← pip dependencies
├── README.md                     ← This file
│
├── src/
│   ├── scraper/                  ── PHASE 1: Selenium scrapers ──────────
│   │   ├── base.py               ← BaseScraper + Chrome driver factory
│   │   │                           size/unit parser, _build(), price cleaner
│   │   ├── amazon.py             ← Amazon Fresh  ✅ ACTIVE
│   │   ├── blinkit.py            ← Blinkit        ✅ ACTIVE
│   │   ├── bigbasket.py          ← BigBasket       ✅ ACTIVE (__NEXT_DATA__)
│   │   ├── zepto.py              ← Zepto           ✅ ACTIVE (localStorage)
│   │   ├── flipkart.py           ← Flipkart        ⏳ FUTURE (CSS unstable)
│   │   ├── instamart.py          ← Swiggy Instamart ⏳ FUTURE (needs login)
│   │   ├── manager.py            ← Runs all active scrapers, one Chrome
│   │   └── delivery.py           ← Pincode → city → delivery time mapping
│   │
│   ├── pipeline/                 ── PHASES 2 & 3 ────────────────────────
│   │   ├── kafka_pipeline.py     ← Kafka producer + consumer
│   │   │                           offset-snapshot anti-stale-message fix
│   │   └── spark_processor.py    ← Rank · savings · ₹/unit
│   │                               Real Spark 4.1.2 · Pandas fallback
│   │
│   └── database/                 ── PHASE 4 ─────────────────────────────
│       └── db.py                 ← SQLite: searches + prices tables
│
├── data/
│   └── prices.db                 ← SQLite DB (auto-created on first run)
│
└── docs/
    └── architecture/             ← System design docs
```

---

## 🌐 Platform Status

| Platform | Scraping Method | Status |
|----------|----------------|--------|
| **Amazon Fresh** | BeautifulSoup HTML card parse | ✅ Active |
| **Blinkit** | Selenium + HTML price-walk | ✅ Active |
| **BigBasket** | `__NEXT_DATA__` JSON extraction | ✅ Active |
| **Zepto** | localStorage location injection | ✅ Active |
| **Flipkart** | BeautifulSoup HTML | ⏳ Future — CSS class names change frequently |
| **Swiggy Instamart** | Cookie location injection | ⏳ Future — requires Swiggy login session |

---

## 📍 Supported Pincodes

| City | Pincode Prefix | Example |
|------|---------------|---------|
| Bangalore | 560xxx, 562xxx | `560001` |
| Delhi | 110xxx | `110001` |
| Mumbai | 400xxx, 401xxx | `400001` |
| Hyderabad | 500xxx | `500001` |
| Chennai | 600xxx | `600001` |
| Kolkata | 700xxx | `700001` |
| Pune | 411xxx | `411001` |
| Ahmedabad | 380xxx | `380001` |
| Gurugram | 122xxx | `122001` |
| Noida | 201xxx | `201301` |
| Jaipur | 302xxx | `302001` |
| Lucknow | 226xxx | `226001` |
| Chandigarh | 160xxx | `160017` |
| Indore | 452xxx | `452001` |

Any unrecognised pincode uses tier-2 city delivery estimates.

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
    mrp            REAL,
    discount_pct   REAL,
    size_label     TEXT,
    price_per_unit REAL,
    unit_norm      TEXT,
    delivery_mins  INTEGER,
    rank           INTEGER,
    savings        REAL,
    is_best_deal   INTEGER,
    source         TEXT,        -- always "LIVE"
    scraped_at     TEXT,
    processed_at   TEXT,
    pincode        TEXT
);
```

---

## ⚡ Tech Stack

| Layer | Technology | Version | Role |
|-------|-----------|---------|------|
| Scraping | Selenium + webdriver-manager | 4.x | Headless Chrome automation |
| HTML parsing | BeautifulSoup4 + lxml | 4.12 | DOM traversal, price extraction |
| Message queue | Apache Kafka (KRaft) | 3.9.2 | Real-time message streaming |
| Kafka client | kafka-python | 2.0.2 | Python producer/consumer |
| Stream processing | Apache Spark | 4.1.2 | Window functions, ranking |
| Processing fallback | Pandas | 2.x | Same logic without JVM |
| Storage | SQLite | built-in | Persistent search history |
| Dashboard | Streamlit | 1.32+ | Interactive web UI |
| Charts | Plotly Express | 5.x | Bar charts, visualisations |
| Language | Python | 3.10+ | All pipeline components |

---

## 🔧 Troubleshooting

### Chrome not found
```bash
# Selenium auto-downloads ChromeDriver via webdriver-manager
# Make sure Google Chrome is installed on the system
```

### Kafka not connecting
```
[Kafka] Broker not available — no broker running at localhost:9092
→ Using in-memory pass-through (pipeline continues normally)
```
The app works fine without Kafka — it uses in-memory JSON serialise/deserialise as fallback. To get real Kafka, follow Step 3 above.

### Spark not starting
```
Engine: Pandas (Spark fallback)
```
PySpark needs Java 11+. Install from [adoptium.net](https://adoptium.net) and set `JAVA_HOME`. The app works without it using Pandas.

### ₹ symbol shows as `?` on Windows
```powershell
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
streamlit run app.py
```
Or just use `START_APP.bat` which sets this automatically.

---

## ⚠️ Disclaimer

This project is for **educational purposes** — demonstrating a real-time data engineering pipeline using industry-standard tools. Scraping websites may violate their Terms of Service. Use responsibly, respect rate limits, and do not run in a production or commercial environment.

---

*Data Engineering Project — Selenium · Apache Kafka · Apache Spark · SQLite · Streamlit*
