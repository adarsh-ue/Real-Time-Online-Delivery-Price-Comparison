"""
blinkit.py
----------
Blinkit Selenium scraper.
URL: https://blinkit.com/s/?q={query}
Blinkit requires a location session — we try, log what we get.
"""

import re
from typing import List, Dict

from selenium import webdriver
from .base import BaseScraper

SEARCH_URL = "https://blinkit.com/s/?q={query}"


class BlinkitScraper(BaseScraper):
    platform      = "Blinkit"
    delivery_mins = 10   # Blinkit ~10 min

    def _fetch(self, query: str, driver: webdriver.Chrome) -> List[Dict]:
        url  = SEARCH_URL.format(query=query.replace(" ", "+"))
        soup = self._get_page(driver, url,
                              wait_selector="[class*='product'],[class*='Product']",
                              timeout=20)

        page_src = driver.page_source.lower()
        if any(k in page_src for k in ["set location", "enter your location",
                                        "add address", "login to continue"]):
            self.log("  [Blinkit] ⚠️  Location/login wall detected")

        # Try __NEXT_DATA__ first
        nd = self._next_data(driver)
        if nd:
            res = self._walk(nd)
            if res:
                return res

        # HTML fallback
        return self._html_prices(soup)

    def _walk(self, nd) -> List[Dict]:
        cands: list = []
        self._dig(nd, cands, 0)
        if not cands:
            return []
        out = []
        for item in max(cands, key=len)[:40]:
            name  = item.get("name") or item.get("product_name") or ""
            price = (item.get("price") or item.get("sp") or item.get("selling_price"))
            price = self._clean_price(str(price)) if price else None
            if name and price:
                out.append(self._build(str(name), price,
                                       brand=str(item.get("brand", "")),
                                       description=str(item.get("description", ""))))
        return out

    def _dig(self, obj, found, depth):
        if depth > 12: return
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            if set(obj[0]) & {"price","sp","name","product_name","mrp"}:
                found.append(obj); return
        if isinstance(obj, dict):
            for v in obj.values(): self._dig(v, found, depth+1)
        elif isinstance(obj, list):
            for i in obj: self._dig(i, found, depth+1)

    def _html_prices(self, soup) -> List[Dict]:
        results, seen = [], set()
        for pel in [t for t in soup.find_all(["span","div","p"])
                    if t.string and re.search(r"₹\s*\d+", t.string)][:40]:
            pv = self._clean_price(pel.get_text(strip=True))
            if not pv: continue
            card, name = pel, ""
            for _ in range(8):
                card = card.parent
                if card is None: break
                ne = (card.find("h4") or card.find("h3") or
                      card.find(attrs={"class": re.compile(r"name|title",re.I)}))
                if ne:
                    name = ne.get_text(strip=True)
                    if len(name) > 4: break
            if name and pv and name not in seen:
                seen.add(name)
                results.append(self._build(name, pv))
        return results
