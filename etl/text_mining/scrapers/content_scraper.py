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
from typing import List
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
    Scrapes HTML content from article URLs
    
    Responsibilities:
    - Download HTML from article pages
    - Extract content using CSS selectors
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
        
        # Create data directory if it doesn't exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"ContentScraper initialized: domain={self.domain}, selector={self.content_selector}")
    
    def scrape_sample(self, event: GA4SampleReadyEvent) -> ArticleHTMLScrapedEvent:
        """
        Scrape articles from a GA4SampleReadyEvent
        
        Args:
            event: GA4SampleReadyEvent containing article pagepaths
            
        Returns:
            ArticleHTMLScrapedEvent with path to scraped data
        """
        logger.info(f"Starting scrape for sample_id={event.sample_id}, {len(event.articles)} articles")
        
        scraped_articles = []
        
        for idx, article in enumerate(event.articles, 1):
            logger.info(f"Scraping {idx}/{len(event.articles)}: {article.pagepath}")
            
            content = self._scrape_article(article.pagepath)
            
            if content:
                scraped_articles.append(content)
            
            # Rate limiting
            if idx < len(event.articles):
                time.sleep(self.delay)
        
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
    
    def _scrape_article(self, pagepath: str) -> ArticleScrapedContent:
        """
        Scrape a single article
        
        Args:
            pagepath: Article path (e.g., /2025/01/15/article-title/)
            
        Returns:
            ArticleScrapedContent or None if failed
        """
        url = self.domain + pagepath if pagepath.startswith("/") else f"{self.domain}/{pagepath}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extract content using selector
            content_element = soup.select_one(self.content_selector)
            
            if content_element:
                html_content = str(content_element)
                
                return ArticleScrapedContent(
                    pagepath=pagepath,
                    html_content=html_content
                )
            else:
                logger.warning(f"Content selector '{self.content_selector}' not found in {url}")
                return None
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
    
    def _save_scraped_data(self, articles: List[ArticleScrapedContent], output_path: Path):
        """Save scraped articles to JSON file"""
        data = {
            "scraped_at": datetime.utcnow().isoformat(),
            "articles": [article.model_dump() for article in articles]
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(articles)} articles to {output_path}")


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
