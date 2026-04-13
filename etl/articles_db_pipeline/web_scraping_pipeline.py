"""Web scraping ETL pipeline for TaxiDrivers.it articles."""
import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from etl.articles_db_pipeline.extractors.web_scraping_extractor import ScrapingExtractor
from etl.articles_db_pipeline.loaders.scraped_articles_loader import ScrapedArticlesLoader
from etl.articles_db_pipeline.config.database import DATABASE_URL


class WebScrapingPipeline:
    """Complete ETL pipeline for web scraping TaxiDrivers.it articles."""
    
    def __init__(
        self,
        base_url: str = "https://www.taxidrivers.it",
        database_url: str = DATABASE_URL,
        delay_between_requests: float = 2.0,
        batch_size: int = 100,
        batch_pause_duration: int = 120,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """Initialize web scraping pipeline.
        
        Args:
            base_url: Base URL of the website
            database_url: PostgreSQL connection URL
            delay_between_requests: Delay in seconds between requests (default: 2.0)
            batch_size: Number of articles to process before pausing (default: 100)
            batch_pause_duration: Pause duration in seconds after each batch (default: 120)
            timeout: Request timeout in seconds (default: 30)
            max_retries: Maximum retry attempts (default: 3)
        """
        # Initialize extractor
        self.extractor = ScrapingExtractor(
            base_url=base_url,
            delay_between_requests=delay_between_requests,
            batch_size=batch_size,
            batch_pause_duration=batch_pause_duration,
            timeout=timeout,
            max_retries=max_retries
        )
        
        # Initialize loader
        self.loader = ScrapedArticlesLoader(database_url=database_url)
        
        logger.info("Initialized WebScrapingPipeline")
    
    def run_full_pipeline(
        self,
        archive_url: str = "/archivio",
        limit: Optional[int] = None,
        update_dim_tables: bool = True
    ) -> Dict[str, Any]:
        """Run the complete web scraping ETL pipeline.
        
        This pipeline:
        1. Scrapes the archive page for article listings
        2. Scrapes each article page for detailed content
        3. Loads the data into PostgreSQL
        4. Optionally updates dimensional tables
        
        Args:
            archive_url: Archive page URL path (default: /archivio)
            limit: Optional limit on number of articles to process
            update_dim_tables: Whether to update dimensional tables (default: True)
            
        Returns:
            Dictionary with pipeline execution statistics
        """
        pipeline_start = datetime.now()
        
        logger.info("=" * 80)
        logger.info("STARTING WEB SCRAPING ETL PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Archive URL: {archive_url}")
        logger.info(f"Article limit: {limit or 'None (all articles)'}")
        logger.info(f"Update dimensions: {update_dim_tables}")
        logger.info("=" * 80)
        
        results = {
            'success': False,
            'started_at': pipeline_start.isoformat(),
            'completed_at': None,
            'duration_seconds': None,
            'extraction': {},
            'loading': {},
            'dimensional_update': {},
            'errors': []
        }
        
        try:
            # Test database connection first
            if not self.loader.test_connection():
                raise Exception("Database connection test failed")
            
            # STEP 1 & 2: Extract articles (archive + details)
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 1: DATA EXTRACTION")
            logger.info("=" * 80)
            
            batch = self.extractor.extract_full_articles(
                archive_url=archive_url,
                limit=limit
            )
            
            results['extraction'] = {
                'batch_id': batch.batch_id,
                'total_scraped': batch.total_scraped,
                'successful': batch.successful,
                'failed': batch.failed,
                'duration_seconds': batch.duration_seconds
            }
            
            if not batch.articles:
                logger.warning("No articles extracted, pipeline stopping")
                return results
            
            # STEP 3: Load into database
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 2: DATA LOADING")
            logger.info("=" * 80)
            
            load_stats = self.loader.load_batch(batch)
            results['loading'] = load_stats
            
            # STEP 4: Update dimensional tables (optional)
            if update_dim_tables:
                logger.info("")
                logger.info("=" * 80)
                logger.info("PHASE 3: DIMENSIONAL MODEL UPDATE")
                logger.info("=" * 80)
                
                dim_stats = self.loader.update_dim_tables_from_scraped()
                results['dimensional_update'] = dim_stats
            
            # Mark as successful
            results['success'] = True
            
            pipeline_end = datetime.now()
            results['completed_at'] = pipeline_end.isoformat()
            results['duration_seconds'] = (pipeline_end - pipeline_start).total_seconds()
            
            # Log summary
            logger.info("")
            logger.info("=" * 80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Duration: {results['duration_seconds']:.1f} seconds")
            logger.info(f"Articles extracted: {results['extraction']['successful']}")
            logger.info(f"Articles loaded: {results['loading']['loaded']}")
            if update_dim_tables:
                logger.info(f"Dimensions updated: {results['dimensional_update']}")
            logger.info("=" * 80)
            
        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            results['success'] = False
            raise
        
        return results
    
    def run_update_only(self) -> Dict[str, Any]:
        """Run only the dimensional table update from existing scraped data.
        
        This is useful when you want to sync scraped data to dimensional model
        without running the full scraping process.
        
        Returns:
            Dictionary with update statistics
        """
        logger.info("Running dimensional table update only")
        
        try:
            dim_stats = self.loader.update_dim_tables_from_scraped()
            logger.success(f"Dimensional tables updated: {dim_stats}")
            return {
                'success': True,
                'dimensional_update': dim_stats
            }
        except Exception as e:
            logger.error(f"Update failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current status of the database."""
        try:
            total_articles = self.loader.get_scraped_articles_count()
            latest_scrape = self.loader.get_latest_scrape_date()
            
            return {
                'database_connected': self.loader.test_connection(),
                'total_scraped_articles': total_articles,
                'latest_scrape_date': latest_scrape.isoformat() if latest_scrape else None
            }
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {str(e)}")
            return {
                'database_connected': False,
                'error': str(e)
            }
