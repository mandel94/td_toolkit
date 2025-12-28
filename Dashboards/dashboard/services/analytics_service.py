"""
Analytics Service - Service Layer Pattern
Contains business logic for analytics operations
Following 2025 best practices for service architecture
"""
from typing import Optional, Literal
from datetime import datetime, timedelta
import pandas as pd
from data.repositories import AnalyticsRepository
from strategies.aggregation import AggregationFactory


class AnalyticsService:
    """
    Service layer for analytics operations
    Coordinates between repositories and business logic
    """
    
    def __init__(self, repository: AnalyticsRepository):
        """
        Initialize analytics service
        
        Args:
            repository: Analytics repository instance
        """
        self.repository = repository
    
    def get_trend_data(
        self,
        start_date: datetime,
        end_date: datetime,
        granularity: Literal["daily", "weekly", "monthly"] = "daily"
    ) -> pd.DataFrame:
        """
        Get trend data with specified granularity
        
        Args:
            start_date: Start date
            end_date: End date
            granularity: Time granularity
            
        Returns:
            DataFrame with aggregated trend data
        """
        # Fetch raw data
        df = self.repository.get_page_views_by_period(start_date, end_date)
        
        # Apply aggregation strategy
        strategy = AggregationFactory.create_strategy(granularity)
        aggregated_df = strategy.aggregate(df)
        
        return aggregated_df
    
    def get_comparison_metrics(
        self,
        start_date: datetime,
        end_date: datetime,
        comparison_type: Literal["WoW", "MoM", "YoY"] = "WoW"
    ) -> dict:
        """
        Get comparison metrics between current and previous period
        
        Args:
            start_date: Start date of current period
            end_date: End date of current period
            comparison_type: Type of comparison
            
        Returns:
            Dictionary with comparison metrics
        """
        current_data, comparison_data = self.repository.get_comparison_data(
            start_date, end_date, comparison_type
        )
        
        current_total = current_data['screenPageViews'].sum()
        comparison_total = comparison_data['screenPageViews'].sum()
        
        # Calculate percent change
        if comparison_total > 0:
            pct_change = ((current_total - comparison_total) / comparison_total) * 100
        else:
            pct_change = 0
        
        # Determine direction
        if pct_change > 5:
            direction = "growth"
        elif pct_change < -5:
            direction = "decline"
        else:
            direction = "stable"
        
        return {
            "current_total": current_total,
            "comparison_total": comparison_total,
            "absolute_change": current_total - comparison_total,
            "percent_change": pct_change,
            "direction": direction,
            "comparison_type": comparison_type
        }
    
    def get_date_range_summary(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> dict:
        """
        Get summary statistics for a date range
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Dictionary with summary statistics
        """
        df = self.repository.get_page_views_by_period(start_date, end_date)
        
        if df.empty:
            return {
                "total_views": 0,
                "daily_average": 0,
                "max_views": 0,
                "min_views": 0,
                "days_count": 0
            }
        
        return {
            "total_views": int(df['screenPageViews'].sum()),
            "daily_average": float(df['screenPageViews'].mean()),
            "max_views": int(df['screenPageViews'].max()),
            "min_views": int(df['screenPageViews'].min()),
            "days_count": len(df)
        }
