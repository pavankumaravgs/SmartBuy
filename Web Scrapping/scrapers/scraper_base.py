from abc import ABC, abstractmethod
from typing import List, Dict
import requests
import random
import time
from fake_useragent import UserAgent

ua = UserAgent()

class ProductScraper(ABC):
    @abstractmethod
    def search_products(self, query: str, pages: int, fetch_details: bool = True) -> List[Dict]:
        pass

    @staticmethod
    def get_headers() -> dict:
        return {
            "User-Agent": ua.random,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8",
            "Referer": "https://www.google.com/"
        }

    def fetch_url(self, url: str, session: requests.Session, retries: int = 1, timeout: int = 15) -> str:
        for attempt in range(1, retries + 1):
            try:
                headers = self.get_headers()
                resp = session.get(url, headers=headers, timeout=timeout)
                if resp.status_code == 200:
                    return resp.text
            except requests.RequestException:
                pass
            time.sleep(1 + random.random() * 2)
        return None
