"""Article metadata extraction module."""
import sys
import os
import time
from typing import List, Optional, Tuple
from datetime import datetime
import concurrent.futures
from requests.exceptions import Timeout, RequestException
from logging import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from scrape_content.ArticleScraper import ArticleScraper
from scrape_content.ArticleProcessor import ArticleProcessor
from etl.articles_pipeline.config.database import (
    DOMAIN, MAX_WORKERS_SCRAPING, SCRAPING_DELAY, SCRAPING_TIMEOUT, MAX_RETRIES, RETRY_DELAY
)
from etl.articles_pipeline.models.article import ArticleMetadata

class MetadataExtractor:
    """Extract article metadata from website content."""
    
    def __init__(self, domain: str = DOMAIN, max_workers: int = MAX_WORKERS_SCRAPING):
        self.domain = domain
        self.max_workers = max_workers
        self.scraper = ArticleScraper(domain=domain)
        self.processor = ArticleProcessor()
        logger.info(f"Initialized MetadataExtractor for domain {domain}")
    
    def extract_batch(
        self,
        page_paths: List[str],
        max_retries: int = MAX_RETRIES
    ) -> List[ArticleMetadata]:
        """Extract metadata for a batch of articles in parallel.
        
        Args:
            page_paths: List of article page paths
            max_retries: Maximum number of retries per article
            
        Returns:
            List of ArticleMetadata objects
        """
        logger.info(f"Extracting metadata for {len(page_paths)} articles")
        
        def extract_single(path: str) -> ArticleMetadata:
            return self._extract_single_with_retry(path, max_retries)
        
        metadata_list = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                # Submit all tasks
                future_to_path = {executor.submit(extract_single, path): path for path in page_paths}
                
                # Collect results
                for future in concurrent.futures.as_completed(future_to_path):
                    path = future_to_path[future]
                    try:
                        metadata = future.result()
                        metadata_list.append(metadata)
                        logger.debug(f"✓ Extracted metadata for {path}")
                    except Exception as e:
                        logger.error(f"✗ Failed to extract metadata for {path}: {str(e)}")
                        # Add empty metadata to maintain order
                        metadata_list.append(ArticleMetadata())
                        
            except Exception as e:
                logger.error(f"Batch metadata extraction failed: {str(e)}")
                raise
        
        logger.success(f"Metadata extraction completed: {len(metadata_list)} results")
        return metadata_list
    
    def _extract_single_with_retry(
        self,
        page_path: str,
        max_retries: int = MAX_RETRIES
    ) -> ArticleMetadata:
        """Extract metadata for a single article with retry logic."""
        url = self._build_url(page_path)
        
        for attempt in range(max_retries + 1):
            try:
                metadata = self._extract_single(page_path)
                return metadata
                
            except (Timeout, RequestException) as e:
                if attempt < max_retries:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}, retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries + 1} attempts failed for {url}: {str(e)}")
                    return ArticleMetadata()
                    
            except Exception as e:
                logger.error(f"Unexpected error extracting {url}: {str(e)}")
                return ArticleMetadata()
    
    def _extract_single(self, page_path: str) -> ArticleMetadata:
        """Extract metadata for a single article."""
        # Add delay between requests to be respectful
        time.sleep(SCRAPING_DELAY)
        
        # Fetch HTML content
        html = self.scraper.fetch_html(page_path)
        if html is None:
            return ArticleMetadata()
        
        # Process HTML to extract metadata
        result = self.processor.process(
            html,
            features=["publication_date", "author", "title"],
            path=page_path
        )
        
        content = result.get("content_metadata", {})
        
        # Parse publication date
        pub_date = None
        if content.get("publication_date"):
            try:
                pub_date = datetime.strptime(content["publication_date"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pub_date = None
        
        return ArticleMetadata(
            title=content.get("title"),
            author=content.get("author"),
            publication_date=pub_date
        )
    
    def _build_url(self, page_path: str) -> str:
        """Build full URL from page path."""
        if page_path.startswith("/"):
            return self.domain + page_path
        else:
            return f"{self.domain}/{page_path}"
    
    def extract_single_metadata(self, page_path: str) -> ArticleMetadata:
        """Extract metadata for a single article (public interface)."""
        return self._extract_single_with_retry(page_path)