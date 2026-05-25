"""
base.py
-------
Base class for Selenium scrapers.
- Headless Chrome via webdriver-manager (auto-downloads correct ChromeDriver)
- Browser-like fingerprint so pages render properly
- NO offline cache — if a scraper finds nothing, it returns []
- Fields returned: platform, product_name, price, brand, description, source
"""

import json
import re
import time
import os
from datetime import datetime
from typing import List, Dict, Callable, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


def build_chrome_driver(log_fn: Callable = print) -> webdriver.Chrome:
    """Create a headless Chrome driver with browser-like settings."""
    from webdriver_manager.chrome import ChromeDriverManager

    opts = Options()
    opts.add_argument("--headless=new")          # modern headless mode
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36")
    opts.add_argument("--lang=en-IN")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--log-level=3")           # suppress Chrome's own logs

    log_fn("  [Chrome] Launching headless Chrome...")
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=opts)

    # Mask webdriver flag (basic anti-bot bypass)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"}
    )
    log_fn("  [Chrome] ✅ Browser ready")
    return driver


class BaseScraper:
    """All platform scrapers inherit from this."""
    platform      = "Unknown"
    delivery_mins = None   # subclasses set this to a real estimate

    def __init__(self, log_callback: Callable):
        self.log = log_callback

    # ── called by manager ──────────────────────────────────────────────────────
    def scrape(self, query: str, driver: webdriver.Chrome) -> List[Dict]:
        self.log(f"\n  ── {self.platform} ──")
        t0 = time.time()
        try:
            results = self._fetch(query, driver)
            elapsed = round(time.time() - t0, 2)
            self.log(f"  [{self.platform}] ✅ {len(results)} products  ({elapsed}s)")
            return results
        except Exception as e:
            elapsed = round(time.time() - t0, 2)
            self.log(f"  [{self.platform}] ❌ {type(e).__name__}: {str(e)[:80]}  ({elapsed}s)")
            return []

    # ── subclasses override this ───────────────────────────────────────────────
    def _fetch(self, query: str, driver: webdriver.Chrome) -> List[Dict]:
        raise NotImplementedError

    # ── helpers ────────────────────────────────────────────────────────────────
    def _get_page(self, driver: webdriver.Chrome, url: str,
                  wait_selector: str, timeout: int = 15) -> BeautifulSoup:
        """Load URL and wait for a CSS selector to appear, then return soup."""
        self.log(f"  [{self.platform}] → {url[:80]}")
        driver.get(url)
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
            )
        except Exception:
            pass   # continue even if wait times out — parse whatever loaded
        time.sleep(1)   # let lazy images / price elements settle
        return BeautifulSoup(driver.page_source, "html.parser")

    def _next_data(self, driver: webdriver.Chrome) -> Optional[dict]:
        """Extract __NEXT_DATA__ JSON from current page (Next.js sites)."""
        try:
            tag = driver.find_element(By.ID, "__NEXT_DATA__")
            return json.loads(tag.get_attribute("innerHTML"))
        except Exception:
            return None

    @staticmethod
    def _clean_price(text: str) -> Optional[float]:
        """Parse price string like '₹65', 'Rs.65', '65.00' → float."""
        if not text:
            return None
        s = re.sub(r"[^\d.]", "", text.replace(",", ""))
        try:
            v = float(s)
            return v if v > 0 else None
        except ValueError:
            return None

    def _build(self, product_name: str, price: float,
               brand: str = "", description: str = "") -> Dict:
        """Return a standardised product record — only real fields."""
        return {
            "platform":     self.platform,
            "product_name": product_name.strip()[:120],
            "price":        round(price, 2),
            "brand":        brand.strip()[:60],
            "description":  description.strip()[:200],
            "source":       "LIVE",
            "scraped_at":   datetime.now().isoformat(),
            # pipeline fields (SparkProcessor will fill rank/savings)
            "mrp":          round(price, 2),
            "discount_pct": 0.0,
            "quantity":     "1",
            "unit":         "pcs",
            "size_label":   "",
            "price_per_unit": round(price, 2),
            "unit_norm":    "per pc",
            "delivery_mins": self.delivery_mins,
            "in_stock":     True,
            "pincode":      "",
        }
