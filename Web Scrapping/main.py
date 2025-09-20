"""
main.py
Entry point for running product scrapers and pushing results to Kafka.
"""
import time
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapers.kafka_client import KafkaClient
from scrapers.scraper_factory import ScraperFactory

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def run_scraper_for_site(site_config, kafka_bootstrap, category):
    import threading
    thread_name = threading.current_thread().name
    logging.info(f"[Thread {thread_name}] Starting scraper for {site_config['name']} ({site_config['url']}) category '{category}'")
    delay_range = site_config.get("delay_range")
    scraper = ScraperFactory.get_scraper(site_config["url"], delay_range=delay_range, category=category)
    logging.info(f"[Thread {thread_name}] Searching products for query '{category}' on {site_config['name']}")
    products = scraper.search_products(
        query=category,
        pages=site_config["pages"],
        fetch_details=site_config.get("fetch_details", True)
    )
    logging.info(f"[Thread {thread_name}] [{site_config['name']}] Total products scraped: {len(products)} for category '{category}'")
    if products:
        topic = f"{site_config['topic']}_{category}"
        kafka_client = KafkaClient(kafka_bootstrap)
        kafka_client.process_and_push_products(products, topic)
        logging.info(f"[Thread {thread_name}] Finished processing for {site_config['name']} category '{category}'")
    else:
        logging.info(f"[Thread {thread_name}] No products found for {site_config['name']} category '{category}'. Skipping Kafka push.")

def main():
    # Load config
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    with open("categories.json", "r", encoding="utf-8") as f:
        categories = json.load(f)["categories"]

    kafka_bootstrap = config["kafka"]["bootstrap_servers"]
    sites = config["sites"]

    # Run all (site, category) pairs in parallel
    tasks = [(site, category) for site in sites for category in categories]
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        futures = [executor.submit(run_scraper_for_site, site, kafka_bootstrap, category) for site, category in tasks]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in scraper thread: {e}")


if __name__ == "__main__":
    main()
