from typing import List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import time
from scrapers.scraper_base import ProductScraper

class FlipkartScraper(ProductScraper):
    def __init__(self, domain: str = "www.flipkart.com"):
        self.domain = domain

    def parse_search_page(self, html: str) -> List[Dict]:
        import logging
        soup = BeautifulSoup(html, "html.parser")
        results = []
        items = soup.find_all("div", {"class": "_1AtVbE col-12-12"})
        logging.info(f"[FlipkartScraper] Found {len(items)} items in search page.")
        for it in items:
            title_tag = it.find("div", class_="_4rR01T")
            title = title_tag.get_text(strip=True) if title_tag else None
            price_tag = it.find("div", class_="_30jeq3 _1_WHN1")
            price = price_tag.get_text(strip=True) if price_tag else None
            rating_tag = it.find("div", class_="_3LWZlK")
            rating = rating_tag.get_text(strip=True) if rating_tag else None
            link_tag = it.find("a", class_="_1fQZEK", href=True)
            product_url = None
            if link_tag:
                href = link_tag.get("href")
                product_url = urljoin(f"https://{self.domain}", href)
            if title:
                results.append({
                    "title": title,
                    "price": price,
                    "rating": rating,
                    "product_url": product_url,
                    "source_domain": self.domain,
                })
        logging.info(f"[FlipkartScraper] Parsed {len(results)} valid products from search page.")
        return results

    def search_products(self, query: str, pages: int, fetch_details: bool = True) -> List[Dict]:
        import requests
        import logging
        from fake_useragent import UserAgent
        session = requests.Session()
        ua = UserAgent()
        all_products = []
        for page in range(1, pages + 1):
            search_url = f"https://{self.domain}/search?q={query}&page={page}"
            logging.info(f"[FlipkartScraper] Fetching search page {page}: {search_url}")
            headers = {
                "User-Agent": ua.random,
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
                    logging.warning(f"[FlipkartScraper] Non-200 status code {resp.status_code} for page {page}")
                    # Log a snippet of the response content for diagnosis
                    snippet = resp.text[:500].replace('\n', ' ').replace('\r', ' ')
                    logging.warning(f"[FlipkartScraper] Response content (first 500 chars): {snippet}")
            except Exception as e:
                logging.warning(f"[FlipkartScraper] Request error on page {page}: {e}")
                html = None
            if not html:
                logging.warning(f"[FlipkartScraper] No HTML returned for page {page}")
                continue
            products = self.parse_search_page(html)
            logging.info(f"[FlipkartScraper] Found {len(products)} products on page {page}")
            all_products.extend(products)
            time.sleep(2)
        logging.info(f"[FlipkartScraper] Total products scraped: {len(all_products)}")
        return all_products
