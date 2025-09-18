#!/usr/bin/env python3
"""
amazon_iphone_to_kafka.py
Scrape Amazon search results for 'iphone' and push found product details to Kafka.

Usage:
    python amazon_iphone_to_kafka.py --pages 3 --kafka-bootstrap localhost:9092 --topic amazon_iphone_products

Notes:
- Use responsibly. Obey Amazon's terms of service and rate limits.
- This is for educational/demo use.
"""

import argparse
import json
import random
import time
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------- CONFIG ----------
DEFAULT_DOMAIN = "www.amazon.in"   # Change to "www.amazon.com" if you want .com
SEARCH_QUERY = "iphone"
DEFAULT_TOPIC = "amazon"
DEFAULT_KAFKA_BOOTSTRAP = "192.168.0.100:29092"
MAX_RETRIES = 1
# ----------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
ua = UserAgent()

def get_headers() -> Dict[str, str]:
    return {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8",
        "Referer": "https://www.google.com/"
    }

def fetch_url(url: str, session: requests.Session, retries: int = MAX_RETRIES, timeout: int = 15) -> Optional[str]:
    for attempt in range(1, retries + 1):
        try:
            headers = get_headers()
            # print(url)
            resp = session.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.text
            else:
                logging.warning("Non-200 status %s for %s", resp.status_code, url)
        except requests.RequestException as e:
            logging.warning("Request error (attempt %d/%d) for %s: %s", attempt, retries, url, e)
        time.sleep(1 + random.random() * 2)
    logging.error("Failed to fetch %s after %d attempts", url, retries)
    return None

def parse_search_page(html: str, domain: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    # search results are in divs with data-asin
    items = soup.find_all("div", {"data-asin": True, "data-component-type": "s-search-result"})
    for it in items:
        asin = it.get("data-asin", "").strip()
        if not asin:
            continue
        # title
        title_tag = it.find("span", class_="a-size-medium")
        title = title_tag.get_text(strip=True) if title_tag else None
        # price
        price_tag = it.find("span", class_="a-offscreen")
        price = price_tag.get_text(strip=True) if price_tag else None
        # rating
        rating_tag = it.find("span", {"class": "a-icon-alt"})
        rating = rating_tag.get_text(strip=True) if rating_tag else None
        # review count
        review_tag = it.find("span", {"class": "a-size-base", "dir": "auto"})
        # fallback: link with reviews
        if not review_tag:
            review_link = it.select_one("a.a-size-small.a-link-normal")
            review_tag = review_link
        review_count = review_tag.get_text(strip=True) if review_tag else None
        # image
        img_tag = it.find("img", class_="s-image")
        image = img_tag.get("src") if img_tag else None
        # product url
        link_tag = it.find("a", class_="a-link-normal", href=True)
        product_url = None
        if link_tag:
            href = link_tag.get("href")
            product_url = urljoin(f"https://{domain}", href)
        results.append({
            "asin": asin,
            "title": title,
            "price": price,
            "rating": rating,
            "review_count": review_count,
            "image": image,
            "product_url": product_url,
            "source_domain": domain,
        })
    return results

def parse_product_detail(html: str) -> Dict:
    """Extract additional details from product page (description, availability)."""
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    # description / bullets
    bullets = soup.select("#feature-bullets ul li")
    if bullets:
        data["bullets"] = [b.get_text(strip=True) for b in bullets]
    # product description
    desc = soup.find(id="productDescription")
    if desc:
        data["description"] = desc.get_text(strip=True)
    # availability
    avail = soup.select_one("#availability .a-color-success, #availability .a-size-medium")
    if avail:
        data["availability"] = avail.get_text(strip=True)
    # ASIN (from details)
    asin = None
    details = soup.select("#detailBullets_feature_div li, #productDetails_techSpec_section_1 tr, #prodDetails table tr")
    for d in details:
        text = d.get_text(" ", strip=True)
        if "ASIN" in text or "ASIN" == text.split(":")[0].strip():
            # crude extraction
            parts = text.split(":")
            if len(parts) >= 2:
                asin = parts[-1].strip()
                break
    if asin:
        data["asin_detail"] = asin
    return data

def init_kafka_producer(bootstrap_servers: str):
    logging.info("Initializing Kafka producer to %s", bootstrap_servers)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=5,
        retries=1
    )
    return producer

def push_to_kafka(producer, topic: str, message: Dict):
    try:
        print(message)
        future = producer.send(topic, value=message)
        record_metadata = future.get(timeout=10)
        logging.info("Sent to Kafka topic=%s partition=%s offset=%s", record_metadata.topic, record_metadata.partition, record_metadata.offset)
    except KafkaError as e:
        logging.error("Error sending to Kafka: %s", e)

def scrape_amazon(domain: str, pages: int, session: requests.Session, fetch_details: bool = True) -> List[Dict]:
    all_products = []
    for page in range(1, pages + 1):
        search_url = f"https://{domain}/s?k={SEARCH_QUERY}&page={page}"
        logging.info("Fetching search page %d: %s", page, search_url)
        html = fetch_url(search_url, session)
        if not html:
            continue
        products = parse_search_page(html, domain)
        products = products[:1]
        logging.info("Found %d items on page %d", len(products), page)
        # optionally get product details page
        if fetch_details:
            for p in products:
                if p.get("product_url"):
                    time.sleep(3)  # polite delay
                    detail_html = fetch_url(p["product_url"], session)
                    if detail_html:
                        details = parse_product_detail(detail_html)
                        p.update(details)
        all_products.extend(products)
        # random delay between pages
        time.sleep(2)
    return all_products

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=2, help="Number of search result pages to scrape")
    parser.add_argument("--kafka-bootstrap", type=str, default=DEFAULT_KAFKA_BOOTSTRAP, help="Kafka bootstrap servers (comma separated)")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="Kafka topic to push messages to")
    parser.add_argument("--domain", type=str, default=DEFAULT_DOMAIN, help="Amazon domain to scrape (e.g., www.amazon.in)")
    parser.add_argument("--no-details", action="store_true", help="Do not fetch individual product detail pages")
    parser.add_argument("--dry-run", action="store_true", help="Do not push to Kafka, just print found products")
    args = parser.parse_args()

    session = requests.Session()
    # Optionally you can set session.proxies here if using proxies
    # session.proxies.update({"http": "http://user:pass@proxy:port", "https": "https://user:pass@proxy:port"})

    products = scrape_amazon(domain=args.domain, pages=args.pages, session=session, fetch_details=not args.no_details)
    logging.info("Total products scraped: %d", len(products))

    if args.dry_run:
        print(json.dumps(products, indent=2, ensure_ascii=False))
        return

    producer = init_kafka_producer(args.kafka_bootstrap)


    for p in products:
        # Prepare message for C# class: ProductId (asin), Price (double), Timestamp (ms)
        product_id = p.get("asin")
        # Extract price as float/double
        price_str = p.get("price")
        price = None
        if price_str:
            # Remove currency symbols and commas
            import re
            price_num = re.sub(r"[^0-9.]", "", price_str.replace(",", ""))
            try:
                price = float(price_num)
            except Exception:
                price = None
        timestamp = int(time.time() * 1000)  # ms
        message = {
            "ProductId": product_id,
            "Price": price if price is not None else 0.0,
            "Timestamp": timestamp
        }
        push_to_kafka(producer, args.topic, message)

    # ensure all buffered messages are sent
    logging.info("Flushing Kafka producer...")
    producer.flush()
    producer.close()
    logging.info("Done.")

if __name__ == "__main__":
    main()