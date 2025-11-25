"""Main ETL pipeline for articles data processing."""
import sys
import os
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from logging import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from etl.articles_pipeline.extractors.ga4_extractor import GA4Extractor
from etl.articles_pipeline.transformers.article_transformer import ArticleTransformer
from etl.articles_pipeline.loaders.database_loader import DatabaseLoader
from etl.articles_pipeline.models.article import ArticleBatch
from etl.articles_pipeline.config.database import MIN_PAGE_VIEWS_THRESHOLD, BATCH_SIZE, ETL_START_DATE

class ArticlesETLPipeline:
    """Main ETL pipeline for processing article analytics data."""
    
    def __init__(
        self,
        ga4_property_id: str = None,
        database_url: str = None
    ):
        """Initialize the ETL pipeline components."""
        
        # Initialize extractors
        self.ga4_extractor = GA4Extractor(property_id=ga4_property_id) if ga4_property_id else GA4Extractor()
        
        # Initialize transformer
        self.transformer = ArticleTransformer()
        
        # Initialize loader
        self.loader = DatabaseLoader(database_url=database_url) if database_url else DatabaseLoader()
        
        logger.info("Initialized ArticlesETLPipeline")
    
    def run_full_pipeline(
        self,
        start_date: str = None,
        end_date: str = None,
        min_page_views: int = MIN_PAGE_VIEWS_THRESHOLD,
        batch_size: int = BATCH_SIZE,
        upsert: bool = True
    ) -> Dict[str, Any]:
        """Run the complete ETL pipeline.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            min_page_views: Minimum page views threshold
            batch_size: Batch size for database operations
            upsert: Whether to update existing records
            extract_metadata: Whether to extract article metadata
            
        Returns:
            Dictionary with pipeline execution results
        """
        pipeline_start = datetime.utcnow()
        
        # Use default date range if not provided
        if not start_date or not end_date:
            start_date, end_date = self.ga4_extractor.get_default_date_range()
            logger.info(f"Using default date range: {start_date} to {end_date}")
        
        logger.info(f"Starting ETL pipeline for period {start_date} to {end_date}")
        
        try:
            # Validate inputs
            if not self._validate_date_range(start_date, end_date):
                raise ValueError("Invalid date range")
            
            # Test database connection
            if not self.loader.test_connection():
                raise ConnectionError("Database connection failed")
            
            # EXTRACT: Get GA4 data
            logger.info("=== EXTRACT PHASE ===")
            raw_articles = self.ga4_extractor.extract(
                start_date=start_date,
                end_date=end_date,
                min_page_views=min_page_views
            )
            
            if not raw_articles:
                logger.warning("No articles extracted from GA4")
                return {
                    'status': 'completed',
                    'message': 'No articles to process',
                    'stats': {'extracted': 0, 'loaded': 0},
                    'duration': (datetime.utcnow() - pipeline_start).total_seconds()
                }
            
            # TRANSFORM: Process articles (no metadata extraction)
            logger.info("=== TRANSFORM PHASE ===")
            processed_articles = self.transformer.transform_batch(raw_articles)
            
            # Validate processed articles
            valid_articles = []
            for article in processed_articles:
                if self.transformer.validate_processed_article(article):
                    valid_articles.append(article)
                else:
                    logger.warning(f"Skipping invalid article: {article.page_path}")
            
            logger.info(f"Validated {len(valid_articles)}/{len(processed_articles)} articles")
            
            # LOAD: Insert into database
            logger.info("=== LOAD PHASE ===")
            load_stats = self.loader.load_batch(
                articles=valid_articles,
                batch_size=batch_size,
                upsert=upsert
            )
            
            # Pipeline completion
            pipeline_end = datetime.utcnow()
            duration = (pipeline_end - pipeline_start).total_seconds()
            
            result = {
                'status': 'completed',
                'message': 'Pipeline executed successfully',
                'period': {'start_date': start_date, 'end_date': end_date},
                'stats': {
                    'extracted': len(raw_articles),
                    'transformed': len(processed_articles),
                    'validated': len(valid_articles),
                    **load_stats
                },
                'duration_seconds': duration,
                'completed_at': pipeline_end.isoformat()
            }
            
            logger.success(f"ETL pipeline completed in {duration:.2f}s")
            logger.success(f"Final stats: {result['stats']}")
            
            return result
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'message': str(e),
                'duration_seconds': (datetime.utcnow() - pipeline_start).total_seconds()
            }
    
    def run_extract_only(
        self,
        start_date: str,
        end_date: str,
        min_page_views: int = MIN_PAGE_VIEWS_THRESHOLD
    ) -> Dict[str, Any]:
        """Run only the extraction phase (for testing)."""
        logger.info(f"Running extract-only for period {start_date} to {end_date}")
        
        try:
            raw_articles = self.ga4_extractor.extract(
                start_date=start_date,
                end_date=end_date,
                min_page_views=min_page_views
            )
            
            return {
                'status': 'completed',
                'message': 'Extraction completed',
                'extracted_count': len(raw_articles),
                'sample_articles': [article.dict() for article in raw_articles[:3]]
            }
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            return {'status': 'failed', 'message': str(e)}
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline and database status."""
        try:
            # Test connections
            db_connection = self.loader.test_connection()
            
            # Get database stats
            db_stats = self.loader.get_database_stats() if db_connection else {}
            
            return {
                'database_connected': db_connection,
                'database_stats': db_stats,
                'components': {
                    'ga4_extractor': 'initialized',
                    'transformer': 'initialized',
                    'loader': 'initialized'
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {str(e)}")
            return {'error': str(e)}
    
    def _validate_date_range(self, start_date: str, end_date: str) -> bool:
        """Validate date range format and logic."""
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            if start > end:
                logger.error("Start date must be before end date")
                return False
            
            if end > date.today():
                logger.error("End date cannot be in the future")
                return False
            
            # Check if date range is too large (more than 90 days)
            if (end - start).days > 90:
                logger.warning("Date range is larger than 90 days, consider breaking into smaller chunks")
            
            return True
            
        except ValueError as e:
            logger.error(f"Invalid date format: {str(e)}")
            return False
    
    def cleanup_old_data(self, days_to_keep: int = 90) -> Dict[str, Any]:
        """Clean up old data from the database."""
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            deleted_count = self.loader.delete_articles_by_date_range(
                start_date=date(2020, 1, 1),  # Very old date
                end_date=cutoff_date
            )
            
            return {
                'status': 'completed',
                'deleted_count': deleted_count,
                'cutoff_date': cutoff_date.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Cleanup failed: {str(e)}")
            return {'status': 'failed', 'message': str(e)}