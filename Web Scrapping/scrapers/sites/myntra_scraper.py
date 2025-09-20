from typing import List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import time
import random
import logging
from scrapers.scraper_base import ProductScraper

class MyntraScraper(ProductScraper):
    def __init__(self, domain: str = "www.myntra.com", delay_range=None):
        self.domain = domain
        self.delay_range = delay_range if delay_range else (2, 4)

    def search_products(self, query: str, pages: int, fetch_details: bool = True) -> List[Dict]:
        """
        You can search for any category or keyword supported by Myntra's search, e.g., 'mobiles', 'shoes', etc.
        """
        import requests
        session = requests.Session()
        all_products = []
        logging.info(f"[MyntraScraper] Search query: '{query}'")
        for page in range(1, pages + 1):
            search_url = f"https://{self.domain}/shop/{query}?p={page}"
            logging.info(f"[MyntraScraper] Fetching search page {page}: {search_url}")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.google.com/"
            }
            try:
                resp = session.get(search_url, headers=headers, timeout=15)
                if resp.status_code == 200:
                    html = resp.text
                else:
                    html = None
                    logging.warning(f"[MyntraScraper] Non-200 status code {resp.status_code} for page {page}")
                    snippet = resp.text[:500].replace('\n', ' ').replace('\r', ' ')
                    logging.warning(f"[MyntraScraper] Response content (first 500 chars): {snippet}")
            except Exception as e:
                logging.warning(f"[MyntraScraper] Request error on page {page}: {e}")
                html = None
            if not html:
                logging.warning(f"[MyntraScraper] No HTML returned for page {page}")
                continue
            products = self.parse_search_page(html)
            logging.info(f"[MyntraScraper] Found {len(products)} products on page {page}")
            all_products.extend(products)
            delay = random.uniform(self.delay_range[0], self.delay_range[1])
            logging.info(f"[MyntraScraper] Sleeping for {delay:.2f} seconds (delay_range={self.delay_range})")
            time.sleep(delay)
        logging.info(f"[MyntraScraper] Total products scraped: {len(all_products)}")
        return all_products

    def parse_search_page(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        results = []
        items = soup.find_all("li", {"class": "product-base"})
        logging.info(f"[MyntraScraper] Found {len(items)} items in search page.")
        for it in items:
            title_tag = it.find("h3", class_="product-brand")
            title = title_tag.get_text(strip=True) if title_tag else None
            name_tag = it.find("h4", class_="product-product")
            name = name_tag.get_text(strip=True) if name_tag else None
            price_tag = it.find("span", class_="product-discountedPrice")
            if not price_tag:
                price_tag = it.find("span", class_="product-strike")
            price = price_tag.get_text(strip=True) if price_tag else None
            link_tag = it.find("a", href=True)
            product_url = None
            if link_tag:
                href = link_tag.get("href")
                product_url = urljoin(f"https://{self.domain}", href)
            if title and name:
                results.append({
                    "brand": title,
                    "name": name,
                    "price": price,
                    "product_url": product_url,
                    "source_domain": self.domain,
                })
        logging.info(f"[MyntraScraper] Parsed {len(results)} valid products from search page.")
        return results
