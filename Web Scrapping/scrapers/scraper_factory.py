from scrapers.sites.amazon_scraper import AmazonScraper
from scrapers.sites.flipkart_scraper import FlipkartScraper
from scrapers.sites.myntra_scraper import MyntraScraper
from scrapers.sites.myntra_selenium_scraper import MyntraSeleniumScraper
from scrapers.scraper_base import ProductScraper

class ScraperFactory:
    @staticmethod
    def get_scraper(url: str, delay_range=None, category: str = None) -> ProductScraper:
        if "amazon" in url:
            return AmazonScraper(url, delay_range=delay_range)
        elif "flipkart" in url:
            return FlipkartScraper(url, delay_range=delay_range)
        elif "myntra" in url:
            # Use API scraper for shirts, fallback to HTML for others
            if category and category.lower() == "shirts":
                return MyntraSeleniumScraper(category="shirts", domain=url, delay_range=delay_range)
            return MyntraScraper(url, delay_range=delay_range)
        else:
            raise ValueError("Unsupported site")
