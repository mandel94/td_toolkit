"""GA4 data extraction module for weekly metrics."""
import sys
import os
from typing import List, Dict, Any
from datetime import datetime, date
import pandas as pd
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from ga4_api.ga4_api import Ga4Client
from etl.page_and_screen_etl import PageAndScreenETLFactory
from etl.articles_db_pipeline.config.database import GA4_PROPERTY_ID, MIN_PAGE_VIEWS_THRESHOLD
from etl.articles_db_pipeline.models.article import RawWeeklyData


class GA4Extractor:
    """Extract weekly article performance data from Google Analytics 4."""
    
    def __init__(self, property_id: str = GA4_PROPERTY_ID):
        self.property_id = property_id
        self.ga4_client = Ga4Client()
        logger.info(f"Initialized GA4Extractor for property {property_id}")
    
    def extract_weekly_data(
        self,
        start_date: str = '2025-01-01',
        end_date: str = None,
        min_page_views: int = MIN_PAGE_VIEWS_THRESHOLD
    ) -> List[RawWeeklyData]:
        """Extract weekly article data from GA4.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (default: 2025-01-01)
            end_date: End date in YYYY-MM-DD format (default: today)
            min_page_views: Minimum page views threshold per week
            
        Returns:
            List of RawWeeklyData objects
        """
        if not end_date:
            end_date = date.today().strftime('%Y-%m-%d')
        
        # GA4 dimensions and metrics for weekly data
        dimensions = ['pagePath', 'year', 'week']
        metrics = [
            'screenPageViews',
            'sessions',
            'engagedSessions',
            'engagementRate',
            'averageSessionDuration'
        ]
        
        logger.info(f"Extracting GA4 weekly data for period {start_date} to {end_date}")
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
                logger.info(f"After filtering (min {min_page_views} page views per week): {len(df)} rows")
            
            # Convert year and week to integers
            df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(2025).astype(int)
            df['week'] = pd.to_numeric(df['week'], errors='coerce').fillna(1).astype(int)
            
            # Convert to RawWeeklyData objects
            raw_weekly_data = self._df_to_raw_weekly(df)
            
            logger.success(f"Successfully extracted {len(raw_weekly_data)} weekly article records")
            return raw_weekly_data
            
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
    
    def _df_to_raw_weekly(self, df: pd.DataFrame) -> List[RawWeeklyData]:
        """Convert DataFrame to list of RawWeeklyData objects."""
        raw_weekly_data = []
        
        for _, row in df.iterrows():
            try:
                # Handle missing columns with defaults
                weekly_data = {
                    'pagePath': row.get('pagePath'),
                    'year': int(row.get('year', 2025)),
                    'week': int(row.get('week', 1)),
                    'screenPageViews': int(float(row.get('screenPageViews', 0))),
                    'sessions': int(float(row.get('sessions', 0))),
                    'engagedSessions': int(float(row.get('engagedSessions', 0))),
                    'engagementRate': float(row.get('engagementRate', 0)),
                    'averageSessionDuration': float(row.get('averageSessionDuration', 0))
                }
                
                raw_record = RawWeeklyData(**weekly_data)
                raw_weekly_data.append(raw_record)
                
            except Exception as e:
                logger.warning(f"Failed to process row: {row.to_dict()}, error: {str(e)}")
                continue
        
        return raw_weekly_data