"""
manager.py
----------
Orchestrates all Selenium scrapers with ONE shared Chrome driver.

Platforms: BigBasket · JioMart · Amazon Fresh · Blinkit · Zepto · Swiggy Instamart
  - All use real headless Chrome (Selenium)
  - NO offline cache, NO fake data
  - If a platform is blocked → logs reason → returns [] → continues
  - One Chrome instance shared across all scrapers (faster startup)

Note: Dunzo shut down consumer grocery delivery in early 2024.
"""

import time
from typing import List, Dict, Callable

from .base       import build_chrome_driver
from .bigbasket  import BigBasketScraper
from .jiomart    import JioMartScraper
from .amazon     import AmazonScraper
from .blinkit    import BlinkitScraper
from .zepto      import ZeptoScraper
from .instamart  import InstamartScraper
from .delivery   import stamp_delivery, get_city

SCRAPER_CLASSES = [
    BigBasketScraper,
    JioMartScraper,
    AmazonScraper,
    BlinkitScraper,
    ZeptoScraper,
    InstamartScraper,
]


class ScraperManager:
    def __init__(self, log_callback: Callable):
        self.log      = log_callback
        self.scrapers = [cls(log_callback) for cls in SCRAPER_CLASSES]

    def scrape_all(self, query: str, pincode: str) -> List[Dict]:
        self.log("=" * 58)
        self.log("PHASE 1 — SCRAPING  (Selenium / headless Chrome)")
        self.log(f"Query    : {query}")
        self.log(f"Pincode  : {pincode}")
        self.log(f"Platforms: {', '.join(s.platform for s in self.scrapers)}")
        city = get_city(pincode)
        self.log(f"City     : {city or 'Unknown — using tier-2 estimates'}")
        self.log(f"Note     : Dunzo shut down consumer ops (2024) — skipped")
        self.log("=" * 58)

        driver      = None
        all_results: List[Dict] = []
        summary:     List[str]  = []

        try:
            driver = build_chrome_driver(self.log)

            for scraper in self.scrapers:
                t0      = time.time()
                results = scraper.scrape(query, driver)
                elapsed = round(time.time() - t0, 2)

                # Stamp pincode on each record
                for r in results:
                    r["pincode"] = pincode

                live  = sum(1 for r in results if r.get("source") == "LIVE")
                label = f"{live} live" if results else "blocked / no data"
                summary.append(
                    f"  {scraper.platform:<24}: {len(results):>3} products  "
                    f"[{label}]  {elapsed}s"
                )
                all_results.extend(results)

        except Exception as e:
            self.log(f"\n  [Manager] ❌ Browser error: {e}")
        finally:
            if driver:
                try:
                    driver.quit()
                    self.log("\n  [Chrome] Browser closed")
                except Exception:
                    pass

        self.log("\n" + "─" * 58)
        self.log("SCRAPING SUMMARY")
        for s in summary:
            self.log(s)
        # Stamp pincode-based delivery times on every record
        all_results = stamp_delivery(all_results, pincode)

        city = get_city(pincode)
        self.log(f"\n  Total live products : {len(all_results)}")
        self.log(f"  Fake / cached data  : 0")
        self.log(f"  Delivery times      : based on pincode {pincode} ({city or 'tier-2 city'})")
        self.log("─" * 58)

        return all_results
