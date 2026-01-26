"""
Trend Service - Business Logic for Trend Analysis
Implements smoothing, seasonality detection, and trend direction
Following 2025 best practices for analytics services
"""
from typing import Optional, Literal
import pandas as pd
import numpy as np


class TrendService:
    """
    Service for trend analysis and smoothing
    """
    
    def __init__(self):
        """Initialize trend service"""
        pass
    
    def add_moving_averages(
        self,
        df: pd.DataFrame,
        metric_column: str = "screenPageViews",
        windows: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        Add moving average columns to dataframe
        
        Args:
            df: Input dataframe with metric column
            metric_column: Name of metric column
            windows: Dictionary of window names and sizes (e.g., {"7d": 7})
            
        Returns:
            DataFrame with additional moving average columns
        """
        if windows is None:
            windows = {"7d": 7, "14d": 14, "30d": 30}
        
        df_copy = df.copy()
        
        for window_name, window_size in windows.items():
            ma_col_name = f"{metric_column}_ma_{window_name}"
            df_copy[ma_col_name] = (
                df_copy[metric_column]
                .rolling(window=window_size, min_periods=1)
                .mean()
            )
        
        return df_copy
    
    def detect_trend_direction(
        self,
        df: pd.DataFrame,
        metric_column: str = "screenPageViews",
        lookback_days: int = 14
    ) -> Literal["growth", "decline", "stable"]:
        """
        Detect overall trend direction
        
        Args:
            df: Input dataframe with date and metric columns
            metric_column: Name of metric column
            lookback_days: Number of days to analyze
            
        Returns:
            Trend direction: growth, decline, or stable
        """
        if len(df) < lookback_days:
            lookback_days = len(df)
        
        if lookback_days < 2:
            return "stable"
        
        # Take last N days
        recent_data = df.tail(lookback_days)
        
        # Calculate linear trend
        x = np.arange(len(recent_data))
        y = recent_data[metric_column].values
        
        # Simple linear regression
        if len(x) > 1:
            slope = np.polyfit(x, y, 1)[0]
            mean_value = y.mean()
            
            if mean_value > 0:
                slope_pct = (slope / mean_value) * 100
                
                if slope_pct > 1:
                    return "growth"
                elif slope_pct < -1:
                    return "decline"
        
        return "stable"
    
    def identify_seasonality_pattern(
        self,
        df: pd.DataFrame,
        metric_column: str = "screenPageViews"
    ) -> dict:
        """
        Identify weekly seasonality patterns
        
        Args:
            df: Input dataframe with date and metric columns
            metric_column: Name of metric column
            
        Returns:
            Dictionary with seasonality insights
        """
        df_copy = df.copy()
        df_copy['weekday'] = df_copy['date'].dt.day_name()
        
        # Calculate average by weekday
        weekday_avg = df_copy.groupby('weekday')[metric_column].mean()
        
        # Reorder to start from Monday
        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                        'Friday', 'Saturday', 'Sunday']
        weekday_avg = weekday_avg.reindex(
            [day for day in weekday_order if day in weekday_avg.index]
        )
        
        # Find strongest and weakest days
        strongest_day = weekday_avg.idxmax()
        weakest_day = weekday_avg.idxmin()
        
        # Calculate variance coefficient
        cv = (weekday_avg.std() / weekday_avg.mean()) * 100 if weekday_avg.mean() > 0 else 0
        
        return {
            "weekday_averages": weekday_avg.to_dict(),
            "strongest_day": strongest_day,
            "weakest_day": weakest_day,
            "variation_coefficient": cv,
            "has_strong_pattern": cv > 15  # More than 15% variation indicates pattern
        }
    
    def calculate_period_growth_rate(
        self,
        df: pd.DataFrame,
        metric_column: str = "screenPageViews"
    ) -> float:
        """
        Calculate compound growth rate over period
        
        Args:
            df: Input dataframe with metric column
            metric_column: Name of metric column
            
        Returns:
            Growth rate as percentage
        """
        if len(df) < 2:
            return 0.0
        
        first_value = df[metric_column].iloc[0]
        last_value = df[metric_column].iloc[-1]
        
        if first_value > 0:
            return ((last_value - first_value) / first_value) * 100
        
        return 0.0
    
    def smooth_anomalies(
        self,
        df: pd.DataFrame,
        metric_column: str = "screenPageViews",
        std_threshold: float = 3.0
    ) -> pd.DataFrame:
        """
        Smooth extreme anomalies in data
        
        Args:
            df: Input dataframe
            metric_column: Name of metric column
            std_threshold: Number of standard deviations for anomaly detection
            
        Returns:
            DataFrame with smoothed values
        """
        df_copy = df.copy()
        
        mean = df_copy[metric_column].mean()
        std = df_copy[metric_column].std()
        
        # Identify anomalies
        upper_bound = mean + (std_threshold * std)
        lower_bound = mean - (std_threshold * std)
        
        # Replace anomalies with rolling average
        mask = (df_copy[metric_column] > upper_bound) | (df_copy[metric_column] < lower_bound)
        
        if mask.any():
            rolling_avg = df_copy[metric_column].rolling(window=7, min_periods=1, center=True).mean()
            df_copy.loc[mask, f"{metric_column}_smoothed"] = rolling_avg[mask]
            df_copy[f"{metric_column}_smoothed"].fillna(df_copy[metric_column], inplace=True)
        else:
            df_copy[f"{metric_column}_smoothed"] = df_copy[metric_column]
        
        return df_copy
