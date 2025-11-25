"""GA4 data extraction module."""
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import pandas as pd
from logging import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from ga4_api.ga4_api import Ga4Client
from etl.page_and_screen_etl import PageAndScreenETLFactory
from etl.articles_pipeline.config.database import (
    GA4_PROPERTY_ID, GA4_DIMENSIONS, GA4_METRICS, MIN_PAGE_VIEWS_THRESHOLD, ETL_START_DATE
)
from etl.articles_pipeline.models.article import RawArticleData

class GA4Extractor:
    """Extract article performance data from Google Analytics 4."""
    
    def __init__(self, property_id: str = GA4_PROPERTY_ID):
        self.property_id = property_id
        self.ga4_client = Ga4Client()
        logger.info(f"Initialized GA4Extractor for property {property_id}")
    
    def extract(
        self,
        start_date: str,
        end_date: str,
        dimensions: List[str] = None,
        metrics: List[str] = None,
        min_page_views: int = MIN_PAGE_VIEWS_THRESHOLD
    ) -> List[RawArticleData]:
        """Extract raw article data from GA4.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            dimensions: GA4 dimensions to query
            metrics: GA4 metrics to query
            min_page_views: Minimum page views threshold
            
        Returns:
            List of RawArticleData objects
        """
        dimensions = dimensions or GA4_DIMENSIONS
        metrics = metrics or GA4_METRICS
        
        logger.info(f"Extracting GA4 data for period {start_date} to {end_date}")
        logger.info(f"Dimensions: {dimensions}")
        logger.info(f"Metrics: {metrics}")
        
        try:
            # Extract raw data from GA4
            df = self.ga4_client.run_query(
                property_id=self.property_id,
                dimensions=dimensions,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date
            )
            
            logger.info(f"Raw GA4 data extracted: {len(df)} rows")
            
            # Apply ETL transformations (cleanup)
            df = self._apply_etl_transformations(df)
            
            logger.info(f"After ETL transformations: {len(df)} rows")
            
            # Filter by minimum page views
            if 'screenPageViews' in df.columns:
                df['screenPageViews'] = pd.to_numeric(df['screenPageViews'], errors='coerce').fillna(0)
                df = df[df['screenPageViews'] >= min_page_views]
                logger.info(f"After filtering (min {min_page_views} page views): {len(df)} rows")
            
            # Convert to RawArticleData objects
            raw_articles = self._df_to_raw_articles(df)
            
            logger.success(f"Successfully extracted {len(raw_articles)} articles")
            return raw_articles
            
        except Exception as e:
            logger.error(f"Failed to extract GA4 data: {str(e)}")
            raise
    
    def _apply_etl_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply standard ETL transformations to clean the data."""
        try:
            etl = PageAndScreenETLFactory.get_etl('en', df=df)
            etl.apply_transformations()
            return etl.df
        except Exception as e:
            logger.warning(f"ETL transformations failed: {str(e)}, using raw data")
            return df
    
    def _df_to_raw_articles(self, df: pd.DataFrame) -> List[RawArticleData]:
        """Convert DataFrame to list of RawArticleData objects."""
        raw_articles = []
        
        for _, row in df.iterrows():
            try:
                # Handle missing columns with defaults
                article_data = {
                    'pagePath': row.get('pagePath'),
                    'screenPageViews': int(float(row.get('screenPageViews', 0))),
                    'sessions': int(float(row.get('sessions', 0))),
                    'engagedSessions': int(float(row.get('engagedSessions', 0))),
                    'engagementRate': float(row.get('engagementRate', 0)),
                    'averageSessionDuration': float(row.get('averageSessionDuration', 0))
                }
                
                raw_article = RawArticleData(**article_data)
                raw_articles.append(raw_article)
                
            except Exception as e:
                logger.warning(f"Failed to process row: {row.to_dict()}, error: {str(e)}")
                continue
        
        return raw_articles
    
    def get_default_date_range(self) -> tuple:
        """Get default date range from January 1, 2025 to today."""
        start_date = ETL_START_DATE
        end_date = date.today().strftime('%Y-%m-%d')
        return start_date, end_date
    
    def validate_date_range(self, start_date: str, end_date: str) -> bool:
        """Validate date range format and logic."""
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            if start > end:
                raise ValueError("Start date must be before end date")
            
            if end > date.today():
                raise ValueError("End date cannot be in the future")
                
            return True
            
        except ValueError as e:
            logger.error(f"Invalid date range: {str(e)}")
            return False