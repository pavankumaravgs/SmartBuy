from typing import List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import time
import random
import logging
from scrapers.scraper_base import ProductScraper

class AmazonScraper(ProductScraper):
    def __init__(self, domain: str = "www.amazon.in", delay_range=None):
        self.domain = domain
        self.delay_range = delay_range if delay_range else (2, 4)

    def parse_search_page(self, html: str) -> List[Dict]:
        import logging
        soup = BeautifulSoup(html, "html.parser")
        results = []
        items = soup.find_all("div", {"data-asin": True, "data-component-type": "s-search-result"})
        logging.info(f"[AmazonScraper] Found {len(items)} items in search page.")
        for it in items:
            asin = it.get("data-asin", "").strip()
            if not asin:
                continue
            title_tag = it.find("span", class_="a-size-medium")
            title = title_tag.get_text(strip=True) if title_tag else None
            price_tag = it.find("span", class_="a-offscreen")
            price = price_tag.get_text(strip=True) if price_tag else None
            rating_tag = it.find("span", {"class": "a-icon-alt"})
            rating = rating_tag.get_text(strip=True) if rating_tag else None
            review_tag = it.find("span", {"class": "a-size-base", "dir": "auto"})
            if not review_tag:
                review_link = it.select_one("a.a-size-small.a-link-normal")
                review_tag = review_link
            review_count = review_tag.get_text(strip=True) if review_tag else None
            img_tag = it.find("img", class_="s-image")
            image = img_tag.get("src") if img_tag else None
            link_tag = it.find("a", class_="a-link-normal", href=True)
            product_url = None
            if link_tag:
                href = link_tag.get("href")
                product_url = urljoin(f"https://{self.domain}", href)
            results.append({
                "asin": asin,
                "title": title,
                "price": price,
                "rating": rating,
                "review_count": review_count,
                "image": image,
                "product_url": product_url,
                "source_domain": self.domain,
            })
        logging.info(f"[AmazonScraper] Parsed {len(results)} valid products from search page.")
        return results

    def parse_product_detail(self, html: str) -> Dict:
        soup = BeautifulSoup(html, "html.parser")
        data = {}
        bullets = soup.select("#feature-bullets ul li")
        if bullets:
            data["bullets"] = [b.get_text(strip=True) for b in bullets]
        desc = soup.find(id="productDescription")
        if desc:
            data["description"] = desc.get_text(strip=True)
        avail = soup.select_one("#availability .a-color-success, #availability .a-size-medium")
        if avail:
            data["availability"] = avail.get_text(strip=True)
        asin = None
        details = soup.select("#detailBullets_feature_div li, #productDetails_techSpec_section_1 tr, #prodDetails table tr")
        for d in details:
            text = d.get_text(" ", strip=True)
            if "ASIN" in text or "ASIN" == text.split(":")[0].strip():
                parts = text.split(":")
                if len(parts) >= 2:
                    asin = parts[-1].strip()
                    break
        if asin:
            data["asin_detail"] = asin
        return data

    def search_products(self, query: str, pages: int, fetch_details: bool = True) -> List[Dict]:
        import requests
        import logging
        session = requests.Session()
        all_products = []
        for page in range(1, pages + 1):
            search_url = f"https://{self.domain}/s?k={query}&page={page}"
            logging.info(f"[AmazonScraper] Fetching search page {page}: {search_url}")
            html = self.fetch_url(search_url, session)
            if not html:
                logging.warning(f"[AmazonScraper] No HTML returned for page {page}")
                continue
            products = self.parse_search_page(html)
            logging.info(f"[AmazonScraper] Found {len(products)} products on page {page}")
            if fetch_details:
                for p in products:
                    if p.get("product_url"):
                        delay = random.uniform(self.delay_range[0], self.delay_range[1])
                        logging.info(f"[AmazonScraper] Sleeping for {delay:.2f} seconds before detail fetch (delay_range={self.delay_range})")
                        time.sleep(delay)
                        detail_html = self.fetch_url(p["product_url"], session)
                        if detail_html:
                            details = self.parse_product_detail(detail_html)
                            p.update(details)
            all_products.extend(products)
            delay = random.uniform(self.delay_range[0], self.delay_range[1])
            logging.info(f"[AmazonScraper] Sleeping for {delay:.2f} seconds after page fetch (delay_range={self.delay_range})")
            time.sleep(delay)
        logging.info(f"[AmazonScraper] Total products scraped: {len(all_products)}")
        return all_products
