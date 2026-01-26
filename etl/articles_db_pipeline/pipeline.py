"""Main ETL pipeline for weekly articles data processing (dimensional model)."""
import sys
import os
from typing import Dict, Any
from datetime import datetime, date
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from etl.articles_db_pipeline.extractors.ga4_extractor import GA4Extractor
from etl.articles_db_pipeline.transformers.article_transformer import ArticleTransformer
from etl.articles_db_pipeline.loaders.database_loader import DatabaseLoader
from etl.articles_db_pipeline.config.database import GA4_PROPERTY_ID, DATABASE_URL, MIN_PAGE_VIEWS_THRESHOLD


class WeeklyArticlesETLPipeline:
    """Main ETL pipeline for processing weekly article analytics into dimensional model."""
    
    def __init__(
        self,
        ga4_property_id: str = GA4_PROPERTY_ID,
        database_url: str = DATABASE_URL
    ):
        """Initialize the ETL pipeline components."""
        
        # Initialize extractors
        self.ga4_extractor = GA4Extractor(property_id=ga4_property_id)
        
        # Initialize transformer
        self.transformer = ArticleTransformer()
        
        # Initialize loader
        self.loader = DatabaseLoader(database_url=database_url)
        
        logger.info("Initialized WeeklyArticlesETLPipeline for dimensional model")
    
    def run_full_pipeline(
        self,
        start_date: str = '2025-01-01',
        end_date: str = None,
        min_page_views: int = MIN_PAGE_VIEWS_THRESHOLD
    ) -> Dict[str, Any]:
        """Run the complete ETL pipeline for weekly data.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (default: 2025-01-01)
            end_date: End date in YYYY-MM-DD format (default: today)
            min_page_views: Minimum page views threshold per week
            
        Returns:
            Dictionary with pipeline execution results
        """
        pipeline_start = datetime.utcnow()
        
        # Set end date to today if not provided
        if not end_date:
            end_date = date.today().strftime('%Y-%m-%d')
        
        logger.info(f"=" * 80)
        logger.info(f"Starting Weekly ETL Pipeline for period {start_date} to {end_date}")
        logger.info(f"=" * 80)
        
        try:
            # Test database connection
            if not self.loader.test_connection():
                raise ConnectionError("Database connection failed")
            
            # EXTRACT: Get GA4 weekly data
            logger.info("=" * 80)
            logger.info("PHASE 1: EXTRACT")
            logger.info("=" * 80)
            
            raw_weekly_data = self.ga4_extractor.extract_weekly_data(
                start_date=start_date,
                end_date=end_date,
                min_page_views=min_page_views
            )
            
            if not raw_weekly_data:
                logger.warning("No weekly data extracted from GA4")
                return {
                    'status': 'completed',
                    'message': 'No data to process',
                    'stats': {'extracted': 0, 'loaded': 0},
                    'duration_seconds': (datetime.utcnow() - pipeline_start).total_seconds()
                }
            
            logger.success(f"Extracted {len(raw_weekly_data)} weekly records")
            
            # TRANSFORM: Process into dimensional format
            logger.info("=" * 80)
            logger.info("PHASE 2: TRANSFORM")
            logger.info("=" * 80)
            
            processed_batch = self.transformer.transform_weekly_batch(raw_weekly_data)
            
            logger.success(f"Transformed data into dimensional format:")
            logger.info(f"  - Weeks: {len(processed_batch.weeks)}")
            logger.info(f"  - Articles: {len(processed_batch.articles)}")
            logger.info(f"  - Authors: {len(processed_batch.authors)}")
            logger.info(f"  - Categories: {len(processed_batch.categories)}")
            logger.info(f"  - Metrics: {len(processed_batch.metrics)}")
            
            # LOAD: Insert into dimensional tables
            logger.info("=" * 80)
            logger.info("PHASE 3: LOAD")
            logger.info("=" * 80)
            
            load_stats = self.loader.load_weekly_batch(processed_batch)
            
            logger.success(f"Data loaded successfully:")
            logger.info(f"  - Weeks loaded: {load_stats['weeks_loaded']}")
            logger.info(f"  - Articles loaded: {load_stats['articles_loaded']}")
            logger.info(f"  - Authors loaded: {load_stats['authors_loaded']}")
            logger.info(f"  - Categories loaded: {load_stats['categories_loaded']}")
            logger.info(f"  - Metrics loaded: {load_stats['metrics_loaded']}")
            
            # Calculate duration
            duration = (datetime.utcnow() - pipeline_start).total_seconds()
            
            logger.info("=" * 80)
            logger.success(f"Pipeline completed successfully in {duration:.2f} seconds")
            logger.info("=" * 80)
            
            return {
                'status': 'success',
                'start_date': start_date,
                'end_date': end_date,
                'stats': {
                    'extracted_records': len(raw_weekly_data),
                    'weeks': len(processed_batch.weeks),
                    'articles': len(processed_batch.articles),
                    'authors': len(processed_batch.authors),
                    'categories': len(processed_batch.categories),
                    'metrics_loaded': load_stats['metrics_loaded']
                },
                'load_details': load_stats,
                'duration_seconds': duration
            }
            
        except Exception as e:
            duration = (datetime.utcnow() - pipeline_start).total_seconds()
            logger.error(f"Pipeline failed after {duration:.2f} seconds: {str(e)}")
            logger.exception(e)
            
            return {
                'status': 'failed',
                'error': str(e),
                'duration_seconds': duration
            }
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get current database statistics."""
        logger.info("Retrieving database statistics")
        return self.loader.get_database_stats()