"""
Data Repositories - Repository Pattern
Isolates data access logic from business logic
Following 2025 best practices for data layer separation
"""
from typing import Optional
from datetime import datetime, timedelta
import pandas as pd
from data.ga4_client import GA4ClientFacade
from config import ga4_config


class AnalyticsRepository:
    """
    Repository for analytics data
    Provides domain-specific data access methods
    """
    
    def __init__(self, ga4_client: Optional[GA4ClientFacade] = None):
        """
        Initialize repository
        
        Args:
            ga4_client: GA4 client instance (optional, creates default if not provided)
        """
        if ga4_client is None:
            ga4_client = GA4ClientFacade(
                property_id=ga4_config.property_id,
                credentials_path=ga4_config.credentials_path
            )
        self.ga4_client = ga4_client
    
    def get_page_views_by_period(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get page views for a specific period
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with date and screenPageViews columns
        """
        df = self.ga4_client.fetch_page_views_trend(
            start_date=start_date,
            end_date=end_date,
            metrics=["screenPageViews"],
            dimensions=["date"]
        )
        
        # Ensure proper sorting
        df = df.sort_values('date').reset_index(drop=True)
        
        return df
    
    def get_detailed_metrics_by_period(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get detailed metrics for a period
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with multiple metrics
        """
        df = self.ga4_client.fetch_page_views_trend(
            start_date=start_date,
            end_date=end_date,
            metrics=["screenPageViews", "activeUsers", "sessions"],
            dimensions=["date"]
        )
        
        df = df.sort_values('date').reset_index(drop=True)
        
        return df
    
    def get_comparison_data(
        self,
        start_date: datetime,
        end_date: datetime,
        comparison_type: str = "WoW"
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Get current and comparison period data
        
        Args:
            start_date: Start date of current period
            end_date: End date of current period
            comparison_type: Type of comparison (WoW, MoM, YoY)
            
        Returns:
            Tuple of (current_data, comparison_data)
        """
        current_data = self.get_page_views_by_period(start_date, end_date)
        comparison_data = self.ga4_client.fetch_comparison_period(
            start_date, end_date, comparison_type
        )
        
        return current_data, comparison_data


class CachedAnalyticsRepository(AnalyticsRepository):
    """
    Repository with caching for improved performance
    Implements simple in-memory cache
    """
    
    def __init__(self, ga4_client: Optional[GA4ClientFacade] = None):
        super().__init__(ga4_client)
        self._cache: dict = {}
        self._cache_ttl = timedelta(hours=1)
    
    def get_page_views_by_period(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get page views with caching
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with cached or fresh data
        """
        cache_key = f"pageviews_{start_date.date()}_{end_date.date()}"
        
        if cache_key in self._cache:
            cached_data, cached_time = self._cache[cache_key]
            if datetime.now() - cached_time < self._cache_ttl:
                return cached_data.copy()
        
        # Fetch fresh data
        df = super().get_page_views_by_period(start_date, end_date)
        
        # Cache it
        self._cache[cache_key] = (df.copy(), datetime.now())
        
        return df
    
    def clear_cache(self):
        """Clear all cached data"""
        self._cache = {}
