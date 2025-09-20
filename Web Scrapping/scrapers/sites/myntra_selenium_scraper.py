from typing import List, Dict
import time
import random
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapers.scraper_base import ProductScraper

class MyntraSeleniumScraper(ProductScraper):
    def __init__(self, category: str = "shirts", domain: str = "www.myntra.com", delay_range=None, pages: int = 2, headless: bool = True):
        self.domain = domain
        self.category = category
        self.delay_range = delay_range if delay_range else (2, 4)
        self.pages = pages
        self.headless = headless

    def _get_driver(self):
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        return driver

    def search_products(self, query: str, pages: int, fetch_details: bool = True) -> List[Dict]:
        url = f"https://{self.domain}/{self.category}"
        logging.info(f"[MyntraSeleniumScraper] Navigating to: {url}")
        driver = self._get_driver()
        driver.get(url)
        all_products = []
        try:
            for page in range(1, pages + 1):
                logging.info(f"[MyntraSeleniumScraper] Scraping page {page}")
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "li.product-base"))
                    )
                except TimeoutException:
                    logging.warning("[MyntraSeleniumScraper] Timeout waiting for products to load on page {page}")
                    break
                time.sleep(random.uniform(self.delay_range[0], self.delay_range[1]))
                products = self._parse_products(driver)
                logging.info(f"[MyntraSeleniumScraper] Found {len(products)} products on page {page}")
                all_products.extend(products)
                # Try to click the 'Next' button if it exists
                try:
                    next_btn = driver.find_element(By.XPATH, "//li[@class='pagination-next']/a")
                    driver.execute_script("arguments[0].scrollIntoView();", next_btn)
                    next_btn.click()
                except NoSuchElementException:
                    logging.info("[MyntraSeleniumScraper] No more pages found.")
                    break
        finally:
            driver.quit()
        logging.info(f"[MyntraSeleniumScraper] Total products scraped: {len(all_products)}")
        return all_products

    def _parse_products(self, driver) -> List[Dict]:
        products = []
        items = driver.find_elements(By.CSS_SELECTOR, "li.product-base")
        for it in items:
            try:
                brand = it.find_element(By.CSS_SELECTOR, "h3.product-brand").text
                name = it.find_element(By.CSS_SELECTOR, "h4.product-product").text
                price_tag = None
                try:
                    price_tag = it.find_element(By.CSS_SELECTOR, "span.product-discountedPrice")
                except NoSuchElementException:
                    try:
                        price_tag = it.find_element(By.CSS_SELECTOR, "span.product-strike")
                    except NoSuchElementException:
                        price_tag = None
                price = price_tag.text if price_tag else None
                link_tag = it.find_element(By.TAG_NAME, "a")
                product_url = link_tag.get_attribute("href") if link_tag else None
                products.append({
                    "brand": brand,
                    "name": name,
                    "price": price,
                    "product_url": product_url,
                    "source_domain": self.domain,
                })
            except Exception as e:
                logging.warning(f"[MyntraSeleniumScraper] Error parsing product: {e}")
        return products
