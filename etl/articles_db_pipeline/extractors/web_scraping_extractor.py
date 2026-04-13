"""Web scraping extractor for TaxiDrivers.it articles."""
import sys
import os
from typing import List, Optional
from datetime import datetime
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from etl.articles_db_pipeline.scrapers import (
    ArchiveScraper,
    ArticleDetailScraper,
    ScraperObserver,
    ScrapingProgress
)
from etl.articles_db_pipeline.models.scraped_article import (
    ScrapedArchiveArticle,
    ScrapedArticleDetail,
    EnrichedScrapedArticle,
    ScrapingBatchResult
)


class ScrapingExtractor:
    """Extract article data from TaxiDrivers.it website using web scraping."""
    
    def __init__(
        self,
        base_url: str = "https://www.taxidrivers.it",
        delay_between_requests: float = 2.0,
        batch_size: int = 100,
        batch_pause_duration: int = 120,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """Initialize scraping extractor.
        
        Args:
            base_url: Base URL of the website
            delay_between_requests: Delay in seconds between requests (default: 2.0)
            batch_size: Number of articles to process before pausing (default: 100)
            batch_pause_duration: Pause duration in seconds after each batch (default: 120)
            timeout: Request timeout in seconds (default: 30)
            max_retries: Maximum retry attempts (default: 3)
        """
        self.base_url = base_url
        
        # Initialize scrapers
        self.archive_scraper = ArchiveScraper(
            base_url=base_url,
            delay_between_requests=delay_between_requests,
            timeout=timeout,
            max_retries=max_retries
        )
        
        self.detail_scraper = ArticleDetailScraper(
            base_url=base_url,
            delay_between_requests=delay_between_requests,
            batch_size=batch_size,
            batch_pause_duration=batch_pause_duration,
            timeout=timeout,
            max_retries=max_retries
        )
        
        logger.info(
            f"Initialized ScrapingExtractor with batch_size={batch_size}, "
            f"batch_pause={batch_pause_duration}s, delay={delay_between_requests}s"
        )
    
    def add_observer(self, observer: ScraperObserver) -> None:
        """Add observer to monitor scraping progress.
        
        Args:
            observer: ScraperObserver implementation
        """
        self.archive_scraper.add_observer(observer)
        self.detail_scraper.add_observer(observer)
    
    def extract_archive_articles(
        self,
        archive_url: str = "/archivio"
    ) -> List[ScrapedArchiveArticle]:
        """Extract article listings from archive page.
        
        Args:
            archive_url: Archive page URL path (default: /archivio)
            
        Returns:
            List of ScrapedArchiveArticle objects
        """
        logger.info(f"Extracting articles from archive: {archive_url}")
        
        raw_articles = self.archive_scraper.scrape(archive_url)
        
        # Convert to Pydantic models
        articles = []
        for article_data in raw_articles:
            try:
                article = ScrapedArchiveArticle(**article_data)
                articles.append(article)
            except Exception as e:
                logger.warning(f"Failed to parse archive article: {str(e)}")
                continue
        
        logger.success(f"Extracted {len(articles)} articles from archive")
        
        return articles
    
    def extract_article_details(
        self,
        article_urls: List[str]
    ) -> List[ScrapedArticleDetail]:
        """Extract detailed content from multiple article pages.
        
        Args:
            article_urls: List of article URLs or page paths
            
        Returns:
            List of ScrapedArticleDetail objects
        """
        logger.info(f"Extracting details for {len(article_urls)} articles")
        
        raw_details = self.detail_scraper.scrape(article_urls)
        
        # Convert to Pydantic models
        details = []
        for detail_data in raw_details:
            try:
                detail = ScrapedArticleDetail(**detail_data)
                details.append(detail)
            except Exception as e:
                logger.warning(f"Failed to parse article detail: {str(e)}")
                continue
        
        logger.success(f"Extracted details for {len(details)} articles")
        
        return details
    
    def extract_full_articles(
        self,
        archive_url: str = "/archivio",
        limit: Optional[int] = None
    ) -> ScrapingBatchResult:
        """Extract complete article data (archive + details).
        
        This is the main extraction method that:
        1. Scrapes the archive page for article listings
        2. Scrapes each article page for detailed content
        3. Combines the data into enriched article objects
        
        Args:
            archive_url: Archive page URL path (default: /archivio)
            limit: Optional limit on number of articles to process
            
        Returns:
            ScrapingBatchResult with enriched article data
        """
        batch = ScrapingBatchResult(
            articles=[],
            total_scraped=0,
            successful=0,
            failed=0
        )
        
        try:
            # Step 1: Extract archive listings
            logger.info("=" * 80)
            logger.info("STEP 1: Extracting article listings from archive")
            logger.info("=" * 80)
            
            archive_articles = self.extract_archive_articles(archive_url)
            
            if not archive_articles:
                logger.warning("No articles found in archive")
                return batch
            
            # Apply limit if specified
            if limit:
                archive_articles = archive_articles[:limit]
                logger.info(f"Limited to {limit} articles")
            
            batch.total_scraped = len(archive_articles)
            
            # Step 2: Extract article details
            logger.info("=" * 80)
            logger.info(f"STEP 2: Extracting details for {len(archive_articles)} articles")
            logger.info("=" * 80)
            
            article_urls = [article.page_path for article in archive_articles]
            article_details = self.extract_article_details(article_urls)
            
            # Create lookup dictionary for details
            details_dict = {detail.page_path: detail for detail in article_details}
            
            # Step 3: Merge archive and detail data
            logger.info("=" * 80)
            logger.info("STEP 3: Merging archive and detail data")
            logger.info("=" * 80)
            
            for archive_article in archive_articles:
                try:
                    detail = details_dict.get(archive_article.page_path)
                    
                    enriched = EnrichedScrapedArticle(
                        page_path=archive_article.page_path,
                        url=archive_article.url,
                        published_text=archive_article.published_text,
                        # Prefer detail data over archive data for these fields
                        title=detail.title if detail and detail.title else archive_article.title,
                        subtitle=detail.subtitle if detail else None,
                        author=detail.author if detail else None,
                        category=archive_article.category,
                        publication_date=detail.publication_date if detail else None,
                        body_html=detail.body_html if detail else None,
                        body_text=detail.body_text if detail else None,
                        archive_scraped_at=archive_article.scraped_at,
                        detail_scraped_at=detail.scraped_at if detail else None
                    )
                    
                    batch.articles.append(enriched)
                    batch.successful += 1
                    
                except Exception as e:
                    logger.error(f"Failed to merge article {archive_article.page_path}: {str(e)}")
                    batch.failed += 1
                    continue
            
            batch.mark_completed()
            
            logger.success("=" * 80)
            logger.success(f"EXTRACTION COMPLETED")
            logger.success(f"Total: {batch.total_scraped} | Successful: {batch.successful} | Failed: {batch.failed}")
            logger.success(f"Duration: {batch.duration_seconds:.1f} seconds")
            logger.success("=" * 80)
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
        finally:
            # Cleanup
            self.close()
        
        return batch
    
    def close(self):
        """Close scrapers and cleanup resources."""
        try:
            self.archive_scraper.close()
            self.detail_scraper.close()
            logger.info("Scrapers closed successfully")
        except Exception as e:
            logger.warning(f"Error closing scrapers: {str(e)}")
