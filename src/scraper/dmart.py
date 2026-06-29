"""
dmart.py
---------
DMart Ready Selenium scraper.

Search URL:
https://www.dmart.in/search?q={query}

Waits for product cards to load and extracts
product names, prices, brands, and descriptions.
"""

import re
from typing import List, Dict

from selenium import webdriver

from .base import BaseScraper

SEARCH_URL = "https://www.dmart.in/search?q={query}"


class DMartScraper(BaseScraper):
    platform = "DMart"
    delivery_mins = 240  # DMart typically same-day / few hours

    def _fetch(self, query: str, driver: webdriver.Chrome) -> List[Dict]:

        url = SEARCH_URL.format(query=query.replace(" ", "%20"))

        soup = self._get_page(
            driver,
            url,
            wait_selector="div[class*='product'], div[class*='item'], div[class*='card']",
            timeout=25,
        )

        # Strategy 1: Try structured JSON
        data = self._extract_json_products(driver)

        if data:
            self.log(f"  [DMart] Parsed {len(data)} products via JSON")
            return data

        # Strategy 2: Parse rendered HTML
        data = self._parse_html(soup)

        if data:
            self.log(f"  [DMart] Parsed {len(data)} products via HTML")
            return data

        self.log("  [DMart] 0 products found")
        return []

    def _extract_json_products(self, driver) -> List[Dict]:
        """
        Attempts to find JSON product data
        embedded in script tags.
        """

        results = []

        try:
            scripts = driver.find_elements("tag name", "script")

            for script in scripts:
                txt = script.get_attribute("innerHTML") or ""

                if "product" not in txt.lower():
                    continue

                prices = re.findall(
                    r'"price"\s*:\s*"?(\d+(?:\.\d+)?)',
                    txt
                )

                names = re.findall(
                    r'"name"\s*:\s*"([^"]+)"',
                    txt
                )

                for name, price in zip(names, prices):
                    results.append(
                        self._build(
                            name=name,
                            price=float(price)
                        )
                    )

                if results:
                    return results[:40]

        except Exception:
            pass

        return []

    def _parse_html(self, soup) -> List[Dict]:

        results = []
        seen = set()

        # Find all price elements
        price_elements = [
            tag
            for tag in soup.find_all(
                ["span", "div", "p", "strong"]
            )
            if re.search(r"₹\s*\d+", tag.get_text(" ", strip=True))
        ]

        for price_el in price_elements[:100]:

            price = self._clean_price(
                price_el.get_text(strip=True)
            )

            if not price:
                continue

            card = price_el
            name = ""
            brand = ""
            description = ""

            # Walk up DOM tree
            for _ in range(8):

                card = card.parent

                if card is None:
                    break

                name_el = (
                    card.find(
                        ["h2", "h3", "h4"]
                    )
                    or card.find(
                        "a",
                        title=True
                    )
                    or card.find(
                        attrs={
                            "class": re.compile(
                                r"name|title|product",
                                re.I
                            )
                        }
                    )
                )

                if name_el:
                    name = name_el.get_text(
                        strip=True
                    )

                    if len(name) > 3:
                        break

            if not name:
                continue

            key = (name, price)

            if key in seen:
                continue

            seen.add(key)

            results.append(
                self._build(
                    name=name,
                    price=price,
                    brand=brand,
                    description=description,
                )
            )

            if len(results) >= 40:
                break

        return results
