"""
app.py  —  Real-Time Indian Grocery Price Comparison
Run: streamlit run app.py
"""

import sys, os, json, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except: pass

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Grocery Price Compare",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from src.scraper          import ScraperManager
from src.scraper.delivery import get_city
from src.pipeline         import KafkaPipeline, SparkProcessor
from src.database         import Database

# ── session state ─────────────────────────────────────────────────────────────
for k, v in {"results":[], "raw_data":[], "timings":{},
             "logs":[], "last_q":"", "last_pin":"", "searched":False}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── header ────────────────────────────────────────────────────────────────────
st.title("🛒 Real-Time Indian Grocery Price Comparison")
st.caption(
    "Selenium → Apache Kafka → Apache Spark / Pandas → SQLite → Streamlit  |  "
    "Platforms: BigBasket · JioMart · Amazon Fresh · Blinkit · Zepto · Swiggy Instamart"
)

# ── input row ─────────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns([2, 5, 1])
with c1:
    pincode = st.text_input("📍 Pincode", value="560001", max_chars=6)
    city = get_city(pincode.strip()) if pincode.strip().isdigit() else None
    if city:
        st.caption(f"📌 {city}")
with c2:
    query = st.text_input("🛍️ Product Name", value="",
                           placeholder="e.g.  eggs  |  Amul Milk  |  Maggi Noodles")
with c3:
    st.markdown("<br>", unsafe_allow_html=True)
    go = st.button("🔍 Search", use_container_width=True, type="primary")

st.markdown("---")

# ── search handler ────────────────────────────────────────────────────────────
if go:
    q   = query.strip()
    pin = pincode.strip()
    if not q:
        st.warning("⚠️  Enter a product name."); st.stop()
    if not (pin.isdigit() and len(pin) == 6):
        st.warning("⚠️  Enter a valid 6-digit pincode."); st.stop()

    # reset state
    st.session_state.update(results=[], raw_data=[], timings={},
                            logs=[], last_q=q, last_pin=pin, searched=True)

    # live terminal — stays visible during the run
    term_exp = st.expander("📡 Live Operations Terminal", expanded=True)
    term_box = term_exp.empty()

    def log(msg: str):
        st.session_state.logs.append(msg)
        term_box.code("\n".join(st.session_state.logs[-40:]), language="")

    log(f"▶  Query: '{q}'   Pincode: {pin}   City: {get_city(pin) or 'unknown'}")
    log("=" * 62)

    timings = {}

    # Phase 1 — Scraping
    t0 = time.time()
    raw_data = ScraperManager(log).scrape_all(q, pin)
    timings["⚙️  Phase 1 — Scraping (Selenium)"] = round(time.time() - t0, 2)
    st.session_state.raw_data = raw_data

    if not raw_data:
        log("❌  No data returned. Try another product or pincode.")
        st.error("No products found."); st.stop()

    # Phase 2 — Kafka
    t0 = time.time()
    mq_data = KafkaPipeline(log).send_and_receive(raw_data)
    timings["📨  Phase 2 — Kafka (message queue)"] = round(time.time() - t0, 2)

    # Phase 3 — Spark / Pandas
    t0 = time.time()
    processed = SparkProcessor(log).process(mq_data)
    timings["⚡  Phase 3 — Spark / Pandas (processing)"] = round(time.time() - t0, 2)

    # Phase 4 — SQLite
    t0 = time.time()
    saved = Database(log).save(processed, q, pin)
    timings["🗄️  Phase 4 — SQLite (storage)"] = round(time.time() - t0, 2)
    timings["🏁  Total"] = round(sum(timings.values()), 2)

    log("\n" + "=" * 62)
    log(f"✅  Pipeline complete — {len(processed)} records processed, {saved} saved to DB")

    st.session_state.results = processed
    st.session_state.timings = timings

    st.rerun()   # re-render so all sections appear with full data

# ── results ───────────────────────────────────────────────────────────────────
if st.session_state.searched:
    results  = st.session_state.results
    raw_data = st.session_state.raw_data
    timings  = st.session_state.timings
    q        = st.session_state.last_q
    pin      = st.session_state.last_pin
    city_lbl = get_city(pin) or pin

    # ── 1. Terminal (collapsed after search completes) ────────────────────────
    with st.expander("📡 Live Operations Terminal", expanded=False):
        logs = st.session_state.logs
        if logs:
            st.code("\n".join(logs), language="")
        else:
            st.info("Terminal output will appear here after a search.")

    # ── 2. Results table ──────────────────────────────────────────────────────
    with st.expander(
        f"📊 Results — {q}  |  {city_lbl} ({pin})  |  {len(results)} products",
        expanded=True
    ):
        if not results:
            st.warning("No results. Run a new search.")
        else:
            in_stock = [r for r in results if r.get("in_stock", True)]
            if in_stock:
                cheapest = min(in_stock, key=lambda r: r.get("price", 9999))
                fastest  = min(in_stock,
                               key=lambda r: (r.get("delivery_mins") or 9999,
                                              r.get("price", 9999)))
                sc1, sc2 = st.columns(2)
                with sc1:
                    st.success(
                        f"💰 **Cheapest:** {cheapest.get('platform')}  ·  "
                        f"{cheapest.get('product_name')}  ·  ₹{cheapest.get('price')}"
                    )
                with sc2:
                    dm = fastest.get("delivery_mins")
                    dm_str = f" · ~{dm} min" if dm else ""
                    st.info(
                        f"⚡ **Fastest delivery:** {fastest.get('platform')}  ·  "
                        f"₹{fastest.get('price')}{dm_str}"
                    )

            st.markdown("&nbsp;")
            rows = []
            for r in results:
                dm = r.get("delivery_mins")
                rows.append({
                    "Rank"      : r.get("rank", "—"),
                    "Product"   : r.get("product_name", "—"),
                    "Brand"     : r.get("brand") or "—",
                    "Platform"  : r.get("platform", "—"),
                    "Price (₹)" : r.get("price", "—"),
                    "Delivery"  : f"~{dm} min" if dm else "—",
                })

            df = pd.DataFrame(rows)
            try:
                df["_s"] = pd.to_numeric(df["Rank"],      errors="coerce").fillna(99)
                df["_p"] = pd.to_numeric(df["Price (₹)"], errors="coerce").fillna(9999)
                df = df.sort_values(["_s", "_p"]).drop(columns=["_s", "_p"])
            except Exception:
                pass

            st.dataframe(
                df, use_container_width=True, hide_index=True,
                column_config={
                    "Rank"      : st.column_config.NumberColumn(width="small"),
                    "Price (₹)" : st.column_config.NumberColumn(format="₹%.2f"),
                    "Delivery"  : st.column_config.TextColumn(width="small"),
                },
            )
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️  Download CSV", csv,
                               file_name=f"{q}_{pin}.csv", mime="text/csv")

    # ── 3. Price Bar Chart ────────────────────────────────────────────────────
    with st.expander("📈 Price Comparison Chart", expanded=True):
        if not results:
            st.info("Run a search to see the chart.")
        else:
            try:
                import plotly.express as px
                chart_rows = []
                for r in results:
                    p = r.get("price")
                    if p:
                        chart_rows.append({
                            "Product" : str(r.get("product_name", ""))[:32]
                                        + "  [" + str(r.get("platform", "")) + "]",
                            "Price"   : float(p),
                            "Platform": r.get("platform", ""),
                        })
                if chart_rows:
                    cdf = pd.DataFrame(chart_rows).sort_values("Price")
                    fig = px.bar(
                        cdf, x="Price", y="Product", orientation="h",
                        color="Platform", text="Price",
                        title=f'Live price comparison — "{q}"  ({city_lbl})',
                        labels={"Product": ""},
                        height=max(350, len(cdf) * 26),
                    )
                    fig.update_traces(texttemplate="₹%{text:.0f}", textposition="outside")
                    fig.update_layout(
                        yaxis={"categoryorder": "total ascending"},
                        margin={"l": 8, "r": 60, "t": 45, "b": 8},
                    )
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.caption(f"Chart unavailable: {e}")

    # ── 4. Pipeline Execution Times ───────────────────────────────────────────
    with st.expander("⏱️ Pipeline Execution Times", expanded=True):
        if not timings:
            st.info("Run a search to see pipeline timings.")
        else:
            phase_desc = {
                "⚙️  Phase 1 — Scraping (Selenium)"       : "Headless Chrome scrapes all 6 platforms",
                "📨  Phase 2 — Kafka (message queue)"      : "Serialise → broker / in-memory → consume",
                "⚡  Phase 3 — Spark / Pandas (processing)": "Rank, ₹/unit, savings, best-deal flag",
                "🗄️  Phase 4 — SQLite (storage)"           : "Write searches + prices tables to DB",
                "🏁  Total"                                 : "End-to-end wall-clock time",
            }
            t_rows = [
                {"Stage": k, "Description": phase_desc.get(k, ""), "Time (s)": v}
                for k, v in timings.items()
            ]
            st.dataframe(
                pd.DataFrame(t_rows), use_container_width=True, hide_index=True,
                column_config={"Time (s)": st.column_config.NumberColumn(format="%.2f s")},
            )

    # ── 5. Raw Scraped JSON ───────────────────────────────────────────────────
    with st.expander("🔍 Raw Scraped JSON  (before → after processing)", expanded=False):
        if not raw_data:
            st.info("Run a search to see raw JSON.")
        else:
            ca, cb = st.columns(2)
            with ca:
                st.markdown("**🟡 RAW — straight from Selenium**")
                st.caption("Before Kafka / Spark — exactly as scraped")
                raw_s = [
                    {k: v for k, v in r.items()
                     if k in ["platform", "product_name", "price", "brand",
                              "description", "delivery_mins", "scraped_at", "source"]}
                    for r in raw_data[:6]
                ]
                st.code(json.dumps(raw_s, indent=2, default=str), language="json")
            with cb:
                st.markdown("**🟢 PROCESSED — after Spark / Pandas**")
                st.caption("Ranked · savings calculated · best-deal flagged")
                proc_s = [
                    {k: v for k, v in r.items()
                     if k in ["platform", "product_name", "price", "rank",
                              "savings", "is_best_deal", "price_per_unit",
                              "unit_norm", "processed_at"]}
                    for r in results[:6]
                ]
                st.code(json.dumps(proc_s, indent=2, default=str), language="json")

    # ── 6. Search History ─────────────────────────────────────────────────────
    with st.expander("📚 Search History  (SQLite database)", expanded=False):
        try:
            hist = Database(lambda _: None).get_history()
            if hist:
                hdf = pd.DataFrame(hist)[["id", "query", "pincode",
                                          "result_count", "searched_at"]]
                hdf.columns = ["ID", "Query", "Pincode", "Results", "Searched At"]
                st.dataframe(hdf, use_container_width=True, hide_index=True)
            else:
                st.write("No searches yet.")
        except Exception as e:
            st.write(f"Could not load history: {e}")

# ── footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "**Pipeline:** Selenium → Apache Kafka → Apache Spark / Pandas → SQLite → Streamlit  |  "
    "All prices scraped live — no fake data"
)
