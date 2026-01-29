"""
Content Scraper for Text Mining Pipeline
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

import requests
from bs4 import BeautifulSoup
import json
import logging
from typing import List, Optional
from datetime import datetime
from pathlib import Path
import time

from etl.text_mining.config import config
from etl.text_mining.events import (
    GA4SampleReadyEvent,
    ArticleScrapedContent,
    ArticleHTMLScrapedEvent
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ContentScraper:
    """
    Scrapes HTML content from article URLs with detailed error tracking
    
    Responsibilities:
    - Download HTML from article pages
    - Extract content using CSS selectors
    - Track and log error statistics by type
    - Save raw HTML to filesystem
    - Create ArticleHTMLScrapedEvent
    """
    
    def __init__(
        self,
        domain: str = None,
        delay: float = None,
        content_selector: str = None,
        data_dir: str = None
    ):
        self.domain = domain or config.SCRAPER_DOMAIN
        self.delay = delay or config.SCRAPER_DELAY
        self.content_selector = content_selector or config.SCRAPER_CONTENT_SELECTOR
        self.data_dir = Path(data_dir or config.SCRAPED_DATA_DIR)
        
        # Track error statistics across all scraping attempts
        self.error_stats = {
            "ssl_errors": 0,
            "timeout_errors": 0,
            "connection_errors": 0,
            "http_errors": 0,
            "selector_not_found": 0,
            "other_errors": 0
        }
        
        # Track successful scrapes
        self.success_count = 0
        
        # Create data directory if it doesn't exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"ContentScraper initialized: domain={self.domain}, selector={self.content_selector}")
    
    def scrape_sample(self, event: GA4SampleReadyEvent) -> ArticleHTMLScrapedEvent:
        """
        Scrape articles from a GA4SampleReadyEvent with comprehensive error tracking
        
        Args:
            event: GA4SampleReadyEvent containing article pagepaths
            
        Returns:
            ArticleHTMLScrapedEvent with path to scraped data and statistics
        """
        logger.info(f"Starting scrape for sample_id={event.sample_id}, {len(event.articles)} articles")
        
        # Reset error stats for this sample
        self._reset_stats()
        
        scraped_articles = []
        start_time = time.time()
        
        for idx, article in enumerate(event.articles, 1):
            logger.info(f"Scraping {idx}/{len(event.articles)}: {article.pagepath}")
            
            content = self._scrape_article(article.pagepath)
            
            if content:
                scraped_articles.append(content)
                self.success_count += 1
            
            # Rate limiting between requests
            if idx < len(event.articles):
                time.sleep(self.delay)
        
        # Calculate statistics
        duration = time.time() - start_time
        total_attempts = len(event.articles)
        total_errors = sum(self.error_stats.values())
        success_rate = (self.success_count / total_attempts * 100) if total_attempts > 0 else 0
        
        # Log comprehensive statistics
        logger.info(f"Scraping statistics for sample {event.sample_id}:")
        logger.info(f"  Total articles: {total_attempts}")
        logger.info(f"  Successful: {self.success_count} ({success_rate:.1f}%)")
        logger.info(f"  Failed: {total_errors} ({100-success_rate:.1f}%)")
        logger.info(f"  Duration: {duration:.2f}s ({duration/total_attempts:.2f}s per article)")
        
        if total_errors > 0:
            logger.warning(f"Error breakdown:")
            for error_type, count in sorted(self.error_stats.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    percentage = (count / total_errors * 100)
                    logger.warning(f"  - {error_type}: {count} ({percentage:.1f}% of errors)")
        
        # Save to JSON file
        output_path = self.data_dir / f"sample_{event.sample_id}.json"
        self._save_scraped_data(scraped_articles, output_path)
        
        # Create completion event
        completion_event = ArticleHTMLScrapedEvent(
            sample_id=event.sample_id,
            json_path=str(output_path),
            articles_count=len(scraped_articles)
        )
        
        logger.info(f"Scraping complete: {len(scraped_articles)} articles saved to {output_path}")
        
        return completion_event
    
    def _scrape_article(self, pagepath: str) -> Optional[ArticleScrapedContent]:
        """
        Scrape a single article with detailed error handling and logging
        
        Args:
            pagepath: Article path (e.g., /2025/01/15/article-title/)
            
        Returns:
            ArticleScrapedContent or None if failed
        """
        url = self.domain + pagepath if pagepath.startswith("/") else f"{self.domain}/{pagepath}"
        
        try:
            logger.debug(f"Requesting URL: {url}")
            response = requests.get(url, timeout=30)
            logger.debug(f"Response status: {response.status_code}, size: {len(response.text)} bytes")
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extract content using selector
            content_element = soup.select_one(self.content_selector)
            
            if content_element:
                html_content = str(content_element)
                logger.debug(f"Successfully extracted content: {len(html_content)} chars")
                
                return ArticleScrapedContent(
                    pagepath=pagepath,
                    html_content=html_content
                )
            else:
                self.error_stats["selector_not_found"] += 1
                logger.warning(f"Content selector '{self.content_selector}' not found in {url}")
                available_ids = [elem.get('id') for elem in soup.find_all(id=True) if elem.get('id')][:10]
                logger.debug(f"Available IDs on page: {available_ids}")
                return None
                
        except requests.exceptions.SSLError as e:
            self.error_stats["ssl_errors"] += 1
            error_msg = str(e)
            if "CERTIFICATE_VERIFY_FAILED" in error_msg:
                logger.error(f"SSL Certificate verification failed for {url}")
                logger.debug(f"SSL Error: self-signed certificate in chain")
            else:
                logger.error(f"SSL Error scraping {url}: {type(e).__name__}")
                logger.debug(f"Full SSL error: {error_msg}")
            return None
            
        except requests.exceptions.Timeout as e:
            self.error_stats["timeout_errors"] += 1
            logger.error(f"Timeout (30s) scraping {url}")
            logger.debug(f"Timeout details: {str(e)}")
            return None
            
        except requests.exceptions.ConnectionError as e:
            self.error_stats["connection_errors"] += 1
            error_msg = str(e)
            if "Max retries exceeded" in error_msg:
                logger.error(f"Connection failed (max retries) for {url}")
            else:
                logger.error(f"Connection Error scraping {url}")
            logger.debug(f"Connection error details: {error_msg[:200]}")
            return None
            
        except requests.exceptions.HTTPError as e:
            self.error_stats["http_errors"] += 1
            status_code = e.response.status_code if hasattr(e, 'response') else 'unknown'
            logger.error(f"HTTP {status_code} Error scraping {url}")
            logger.debug(f"HTTP error details: {str(e)}")
            return None
            
        except Exception as e:
            self.error_stats["other_errors"] += 1
            logger.error(f"Unexpected error scraping {url}: {type(e).__name__} - {str(e)}")
            logger.exception("Full traceback for unexpected error:")
            return None
    
    def _reset_stats(self):
        """Reset statistics for a new sample"""
        self.error_stats = {k: 0 for k in self.error_stats}
        self.success_count = 0
    
    def _save_scraped_data(self, articles: List[ArticleScrapedContent], output_path: Path):
        """
        Save scraped articles to JSON file
        
        Args:
            articles: List of scraped article content
            output_path: Path to save JSON file
        """
        data = {
            "scraped_at": datetime.utcnow().isoformat(),
            "articles_count": len(articles),
            "articles": [article.model_dump(mode='json') for article in articles]
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(articles)} articles to {output_path}")
    
    def get_stats_summary(self) -> dict:
        """Get current scraping statistics"""
        return {
            "success_count": self.success_count,
            "error_stats": self.error_stats.copy(),
            "total_errors": sum(self.error_stats.values())
        }


if __name__ == "__main__":
    # Test scraping
    from etl.text_mining.extractors.ga4_sample_extractor import GA4SampleExtractor
    
    # Extract sample
    extractor = GA4SampleExtractor()
    ga4_event = extractor.extract_sample(sample_size=3)
    
    # Scrape content
    scraper = ContentScraper()
    scraped_event = scraper.scrape_sample(ga4_event)
    
    print(scraped_event.model_dump_json(indent=2))
    print(f"\nStats: {scraper.get_stats_summary()}")
