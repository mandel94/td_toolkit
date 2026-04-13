#!/usr/bin/env python
"""Complete scraping and loading pipeline for TaxiDrivers articles into database."""
import sys
import os
from datetime import datetime
from typing import List, Optional
from pathlib import Path
import urllib3

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from loguru import logger
from etl.articles_db_pipeline.scrapers.archive_scraper import ArchiveScraper
from etl.articles_db_pipeline.scrapers.article_detail_scraper import ArticleDetailScraper
from etl.articles_db_pipeline.loaders.scraped_articles_loader import ScrapedArticlesLoader
from etl.articles_db_pipeline.models.scraped_article import EnrichedScrapedArticle
from etl.articles_db_pipeline.config.database import DATABASE_URL

# Suppress SSL warnings for verified-off requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Configure logging
logger.remove()
log_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
    "<level>{message}</level>"
)
logger.add(sys.stdout, format=log_format, level="INFO")

# Log file
log_dir = Path(__file__).parent / "output" / "scraping"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger.add(str(log_file), format=log_format, level="DEBUG")


class ArticleScrapingPipeline:
    """Pipeline to scrape articles from TaxiDrivers.it and load into database."""
    
    def __init__(
        self,
        base_url: str = "https://www.taxidrivers.it",
        database_url: str = DATABASE_URL,
        delay_between_requests: float = 1.0,
        verify_ssl: bool = False
    ):
        """Initialize the scraping pipeline.
        
        Args:
            base_url: Base URL of the website
            database_url: PostgreSQL connection URL
            delay_between_requests: Delay in seconds between requests
        """
        self.base_url = base_url
        self.database_url = database_url
        
        # Initialize scrapers
        self.archive_scraper = ArchiveScraper(
            base_url=base_url,
            delay_between_requests=delay_between_requests,
            verify_ssl=verify_ssl
        )
        self.detail_scraper = ArticleDetailScraper(
            base_url=base_url,
            delay_between_requests=delay_between_requests,
            verify_ssl=verify_ssl
        )
        
        # Initialize loader
        self.loader = ScrapedArticlesLoader(database_url=database_url)
        
        logger.info(f"Initialized ArticleScrapingPipeline for {base_url}")
    
    def scrape_and_enrich_articles(
        self,
        archive_url: str = "/archivio",
        limit: Optional[int] = None
    ) -> List[EnrichedScrapedArticle]:
        """Scrape articles from archive and enrich with details.
        
        Args:
            archive_url: URL of the archive page
            limit: Maximum number of articles to scrape (None = all)
            
        Returns:
            List of EnrichedScrapedArticle objects
        """
        # Step 1: Scrape archive page
        logger.info("=" * 70)
        logger.info("[1/3] SCRAPING ARCHIVE PAGE (FIRST PAGE ONLY)")
        logger.info("=" * 70)
        
        archive_articles = self.archive_scraper.scrape_archive_page(archive_url)
        logger.info(f"Found {len(archive_articles)} articles in first archive page")
        
        if limit:
            archive_articles = archive_articles[:limit]
            logger.info(f"Limited to {limit} articles")
        
        # Step 2: Enrich articles with details
        logger.info("\n" + "=" * 70)
        logger.info("[2/3] SCRAPING ARTICLE DETAILS")
        logger.info("=" * 70)
        
        enriched_articles: List[EnrichedScrapedArticle] = []
        failed_urls = []
        
        for idx, archive_article in enumerate(archive_articles, 1):
            try:
                logger.info(f"[{idx}/{len(archive_articles)}] Processing: {archive_article['page_path']}")
                
                # Scrape article detail
                detail = self.detail_scraper.scrape_article_detail(archive_article['url'])
                
                if detail:
                    # Merge archive and detail data
                    enriched = EnrichedScrapedArticle(
                        page_path=archive_article['page_path'],
                        url=archive_article['url'],
                        published_text=archive_article.get('published_text'),
                        # Detail data (overrides archive data if present)
                        title=detail.get('title') or archive_article.get('title'),
                        subtitle=detail.get('subtitle'),
                        author=detail.get('author'),
                        category=archive_article.get('category') or detail.get('category'),
                        publication_date=detail.get('publication_date'),
                        body_html=detail.get('body_html'),
                        body_text=detail.get('body_text'),
                        # Timestamps
                        archive_scraped_at=archive_article.get('scraped_at'),
                        detail_scraped_at=detail.get('scraped_at'),
                    )
                    enriched_articles.append(enriched)
                    logger.success(f"  OK Enriched: {enriched.page_path}")
                else:
                    # Use archive data only if detail scraping fails
                    enriched = EnrichedScrapedArticle(
                        page_path=archive_article['page_path'],
                        url=archive_article['url'],
                        published_text=archive_article.get('published_text'),
                        title=archive_article.get('title'),
                        category=archive_article.get('category'),
                        archive_scraped_at=archive_article.get('scraped_at'),
                    )
                    enriched_articles.append(enriched)
                    logger.warning("  WARN Detail scraping failed, using archive data only")
                    failed_urls.append(archive_article['url'])
                
            except Exception as e:
                logger.error(f"  ERROR Failed to process {archive_article.get('page_path')}: {str(e)}")
                failed_urls.append(archive_article.get('url', 'unknown'))
                continue
        
        logger.info(f"\nEnriched {len(enriched_articles)} articles")
        if failed_urls:
            logger.warning(f"WARN Failed to scrape details for {len(failed_urls)} URLs")
        
        return enriched_articles
    
    def load_articles_to_database(
        self,
        articles: List[EnrichedScrapedArticle],
        batch_size: int = 10
    ) -> dict:
        """Load scraped articles into database.
        
        Args:
            articles: List of EnrichedScrapedArticle objects
            batch_size: Number of articles per batch
            
        Returns:
            Dictionary with load statistics
        """
        logger.info("\n" + "=" * 70)
        logger.info("[3/3] LOADING ARTICLES TO DATABASE")
        logger.info("=" * 70)
        
        # Test connection first
        if not self.loader.test_connection():
            logger.error("ERROR Database connection failed")
            return {
                'status': 'failed',
                'total': len(articles),
                'loaded': 0,
                'failed': len(articles),
                'error': 'Database connection failed'
            }
        
        logger.info("Database connection successful")
        logger.info(f"Loading {len(articles)} articles...")
        
        stats = {
            'total': len(articles),
            'loaded': 0,
            'failed': 0,
            'errors': []
        }
        
        for idx, article in enumerate(articles, 1):
            try:
                success = self.loader.load_scraped_article(article)
                if success:
                    stats['loaded'] += 1
                    logger.success(f"[{idx}/{len(articles)}] OK Loaded: {article.page_path}")
                else:
                    stats['failed'] += 1
                    logger.warning(f"[{idx}/{len(articles)}] WARN Duplicate or failed: {article.page_path}")
                    
            except Exception as e:
                stats['failed'] += 1
                error_msg = str(e)
                stats['errors'].append({'article': article.page_path, 'error': error_msg})
                logger.error(f"[{idx}/{len(articles)}] ERROR loading {article.page_path}: {error_msg}")
        
        stats['status'] = 'completed'
        return stats
    
    def run_full_pipeline(
        self,
        archive_url: str = "/archivio",
        limit: Optional[int] = None,
        skip_loading: bool = False
    ) -> dict:
        """Run complete scraping and loading pipeline.
        
        Args:
            archive_url: URL of the archive page
            limit: Maximum number of articles to scrape
            skip_loading: Skip database loading step (useful for testing)
            
        Returns:
            Dictionary with pipeline execution results
        """
        logger.info("\n" + "=" * 70)
        logger.info("ARTICLES SCRAPING AND LOADING PIPELINE")
        logger.info("=" * 70)
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"Database URL: {self.database_url}")
        logger.info(f"Archive URL: {archive_url}")
        logger.info(f"Limit: {limit if limit else 'All articles'}")
        logger.info("=" * 70)
        
        start_time = datetime.now()
        
        # Step 1-2: Scrape and enrich
        try:
            articles = self.scrape_and_enrich_articles(
                archive_url=archive_url,
                limit=limit
            )
        except Exception as e:
            logger.error(f"Scraping pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'stage': 'scraping',
                'error': str(e),
                'articles_scraped': 0
            }
        
        # Step 3: Load to database
        if skip_loading:
            logger.info("\nSkipping database loading (test mode)")
            load_stats = {
                'status': 'skipped',
                'total': len(articles),
                'loaded': 0,
                'failed': 0
            }
        else:
            try:
                load_stats = self.load_articles_to_database(articles)
            except Exception as e:
                logger.error(f"Loading pipeline failed: {str(e)}")
                load_stats = {
                    'status': 'failed',
                    'total': len(articles),
                    'loaded': 0,
                    'failed': len(articles),
                    'error': str(e)
                }
        
        # Final summary
        duration = datetime.now() - start_time
        logger.info("\n" + "=" * 70)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Duration: {duration.total_seconds():.1f} seconds")
        logger.info(f"Articles scraped: {len(articles)}")
        logger.info(f"Articles loaded: {load_stats.get('loaded', 0)}")
        logger.info(f"Articles failed: {load_stats.get('failed', 0)}")
        
        if load_stats.get('errors'):
            logger.warning(f"\nErrors encountered:")
            for error in load_stats['errors'][:5]:  # Show first 5 errors
                logger.warning(f"  - {error['article']}: {error['error']}")
            if len(load_stats['errors']) > 5:
                logger.warning(f"  ... and {len(load_stats['errors']) - 5} more errors")
        
        logger.info("=" * 70)
        
        if load_stats['status'] == 'completed' and load_stats['loaded'] == len(articles):
            logger.success("PIPELINE COMPLETED SUCCESSFULLY")
        elif load_stats['status'] == 'skipped':
            logger.info("PIPELINE COMPLETED (Loading skipped)")
        else:
            logger.warning("PIPELINE COMPLETED WITH ISSUES")
        
        logger.info("=" * 70)
        
        return {
            'status': load_stats['status'],
            'duration_seconds': duration.total_seconds(),
            'articles_scraped': len(articles),
            'articles_loaded': load_stats.get('loaded', 0),
            'articles_failed': load_stats.get('failed', 0),
            'details': load_stats
        }


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Scrape articles from TaxiDrivers.it and load into database"
    )
    parser.add_argument(
        '--archive-url',
        default='/archivio',
        help='Archive page URL path (default: /archivio)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of articles to scrape (default: all)'
    )
    parser.add_argument(
        '--skip-loading',
        action='store_true',
        help='Skip database loading (test mode)'
    )
    parser.add_argument(
        '--base-url',
        default='https://www.taxidrivers.it',
        help='Base URL of the website'
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = ArticleScrapingPipeline(base_url=args.base_url)
    
    # Run pipeline
    result = pipeline.run_full_pipeline(
        archive_url=args.archive_url,
        limit=args.limit,
        skip_loading=args.skip_loading
    )
    
    # Exit with appropriate code
    sys.exit(0 if result['status'] in ['completed', 'skipped'] else 1)


if __name__ == "__main__":
    main()
