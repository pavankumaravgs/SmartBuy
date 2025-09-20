from scrapers.amazon_scraper import AmazonScraper
from scrapers.flipkart_scraper import FlipkartScraper
from scrapers.scraper_base import ProductScraper

class ScraperFactory:
    @staticmethod
    def get_scraper(url: str) -> ProductScraper:
        if "amazon" in url:
            return AmazonScraper(url)
        elif "flipkart" in url:
            return FlipkartScraper(url)
        else:
            raise ValueError("Unsupported site")
