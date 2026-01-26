"""
Aggregation Strategies - Strategy Pattern
Provides flexible time aggregation logic
Following 2025 best practices for extensible design
"""
from abc import ABC, abstractmethod
from typing import Literal
import pandas as pd


class AggregationStrategy(ABC):
    """
    Abstract base class for aggregation strategies
    """
    
    @abstractmethod
    def aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate data according to strategy
        
        Args:
            df: DataFrame with date and metrics columns
            
        Returns:
            Aggregated DataFrame
        """
        pass
    
    @abstractmethod
    def get_period_label(self) -> str:
        """Get human-readable period label"""
        pass


class DailyAggregation(AggregationStrategy):
    """Daily aggregation - no aggregation needed"""
    
    def aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return data as-is (already daily)"""
        return df.copy()
    
    def get_period_label(self) -> str:
        return "Daily"


class WeeklyAggregation(AggregationStrategy):
    """Weekly aggregation strategy"""
    
    def aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate to weekly level"""
        df_copy = df.copy()
        df_copy['week'] = df_copy['date'].dt.to_period('W')
        
        # Group by week and sum metrics
        numeric_cols = df_copy.select_dtypes(include=['number']).columns
        
        agg_dict = {col: 'sum' for col in numeric_cols}
        agg_dict['date'] = 'first'  # Keep first date of week
        
        weekly_df = df_copy.groupby('week').agg(agg_dict).reset_index(drop=True)
        
        return weekly_df
    
    def get_period_label(self) -> str:
        return "Weekly"


class MonthlyAggregation(AggregationStrategy):
    """Monthly aggregation strategy"""
    
    def aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate to monthly level"""
        df_copy = df.copy()
        df_copy['month'] = df_copy['date'].dt.to_period('M')
        
        # Group by month and sum metrics
        numeric_cols = df_copy.select_dtypes(include=['number']).columns
        
        agg_dict = {col: 'sum' for col in numeric_cols}
        agg_dict['date'] = 'first'  # Keep first date of month
        
        monthly_df = df_copy.groupby('month').agg(agg_dict).reset_index(drop=True)
        
        return monthly_df
    
    def get_period_label(self) -> str:
        return "Monthly"


class AggregationFactory:
    """
    Factory for creating aggregation strategies
    Following Factory pattern for object creation
    """
    
    @staticmethod
    def create_strategy(
        granularity: Literal["daily", "weekly", "monthly"]
    ) -> AggregationStrategy:
        """
        Create aggregation strategy based on granularity
        
        Args:
            granularity: Time granularity (daily, weekly, monthly)
            
        Returns:
            Appropriate aggregation strategy instance
            
        Raises:
            ValueError: If granularity is not supported
        """
        strategies = {
            "daily": DailyAggregation,
            "weekly": WeeklyAggregation,
            "monthly": MonthlyAggregation
        }
        
        if granularity not in strategies:
            raise ValueError(
                f"Unknown granularity: {granularity}. "
                f"Must be one of {list(strategies.keys())}"
            )
        
        return strategies[granularity]()
