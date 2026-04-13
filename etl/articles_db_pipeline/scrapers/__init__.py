"""Web scraping module for TaxiDrivers.it articles extraction."""
from .base_scraper import ScraperBase, ScraperObserver, ScrapingProgress
from .archive_scraper import ArchiveScraper
from .article_detail_scraper import ArticleDetailScraper
from .selectors import TaxiDriversSelectors

__all__ = [
    'ScraperBase',
    'ScraperObserver',
    'ScrapingProgress',
    'ArchiveScraper',
    'ArticleDetailScraper',
    'TaxiDriversSelectors'
]
