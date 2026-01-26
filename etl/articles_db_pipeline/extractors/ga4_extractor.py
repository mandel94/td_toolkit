"""GA4 data extraction module for weekly metrics."""
import sys
import os
from typing import List, Dict, Any, Tuple
from datetime import datetime, date, timedelta
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
        min_page_views: int = MIN_PAGE_VIEWS_THRESHOLD,
        validate_week_alignment: bool = True
    ) -> List[RawWeeklyData]:
        """Extract weekly article data from GA4.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (default: 2025-01-01)
            end_date: End date in YYYY-MM-DD format (default: today)
            min_page_views: Minimum page views threshold per week
            validate_week_alignment: If True, validates date range aligns with ISO weeks
            
        Returns:
            List of RawWeeklyData objects
            
        Raises:
            ValueError: If date range doesn't align with ISO week boundaries
        """
        if not end_date:
            end_date = date.today().strftime('%Y-%m-%d')
        
        # Validate week alignment if requested
        if validate_week_alignment:
            self._validate_week_alignment(start_date, end_date)
        
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
    
    def _validate_week_alignment(self, start_date: str, end_date: str) -> None:
        """Validate that date range aligns with ISO week boundaries (Monday-Sunday).
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Raises:
            ValueError: If dates don't align with week boundaries
        """
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Check if start_date is a Monday (ISO weekday 1)
        if start.weekday() != 0:
            monday = start - timedelta(days=start.weekday())
            raise ValueError(
                f"Start date {start_date} ({start.strftime('%A')}) is not a Monday. "
                f"ISO weeks start on Monday. Nearest Monday is {monday.strftime('%Y-%m-%d')}."
            )
        
        # Check if end_date is a Sunday (ISO weekday 6)
        if end.weekday() != 6:
            sunday = end + timedelta(days=(6 - end.weekday()))
            raise ValueError(
                f"End date {end_date} ({end.strftime('%A')}) is not a Sunday. "
                f"ISO weeks end on Sunday. Nearest Sunday is {sunday.strftime('%Y-%m-%d')}."
            )
        
        # Check that the date range is exactly N complete weeks
        days_diff = (end - start).days + 1  # +1 to include both days
        if days_diff % 7 != 0:
            raise ValueError(
                f"Date range {start_date} to {end_date} spans {days_diff} days, "
                f"which is not a multiple of 7. Must cover complete weeks only."
            )
        
        num_weeks = days_diff // 7
        logger.info(f"✓ Date range validation passed: {num_weeks} complete ISO week(s)")
    
    def get_week_boundaries(self, year: int, week: int) -> Tuple[date, date]:
        """Get the start (Monday) and end (Sunday) dates for a given ISO week.
        
        Args:
            year: ISO year
            week: ISO week number (1-53)
            
        Returns:
            Tuple of (start_date, end_date) as date objects
        """
        # ISO week 1 is the week with the first Thursday of the year
        jan_4 = date(year, 1, 4)
        week_1_monday = jan_4 - timedelta(days=jan_4.weekday())
        week_start = week_1_monday + timedelta(weeks=week - 1)
        week_end = week_start + timedelta(days=6)
        
        return week_start, week_end
    
    def suggest_aligned_dates(self, target_date: str) -> Dict[str, str]:
        """Suggest week-aligned dates for a given target date.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            Dictionary with suggested start and end dates
        """
        target = datetime.strptime(target_date, '%Y-%m-%d').date()
        
        # Find the Monday of the week containing target_date
        monday = target - timedelta(days=target.weekday())
        # Find the Sunday of the week containing target_date
        sunday = target + timedelta(days=(6 - target.weekday()))
        
        return {
            'week_start': monday.strftime('%Y-%m-%d'),
            'week_end': sunday.strftime('%Y-%m-%d'),
            'iso_year': monday.isocalendar()[0],
            'iso_week': monday.isocalendar()[1]
        }