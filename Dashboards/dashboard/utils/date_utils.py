"""
Date Utilities
Helper functions for date operations
Following 2025 best practices for utility modules
"""
from datetime import datetime, timedelta
from typing import Tuple, Literal


def get_default_date_range(days: int = 90) -> Tuple[datetime, datetime]:
    """
    Get default date range (last N days)
    
    Args:
        days: Number of days to go back
        
    Returns:
        Tuple of (start_date, end_date)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date


def calculate_comparison_period(
    start_date: datetime,
    end_date: datetime,
    comparison_type: Literal["WoW", "MoM", "YoY"]
) -> Tuple[datetime, datetime]:
    """
    Calculate comparison period dates
    
    Args:
        start_date: Start date of current period
        end_date: End date of current period
        comparison_type: Type of comparison
        
    Returns:
        Tuple of (comparison_start_date, comparison_end_date)
    """
    period_length = (end_date - start_date).days
    
    if comparison_type == "WoW":
        offset = 7
    elif comparison_type == "MoM":
        offset = 30
    elif comparison_type == "YoY":
        offset = 365
    else:
        raise ValueError(f"Unknown comparison type: {comparison_type}")
    
    comp_start = start_date - timedelta(days=offset)
    comp_end = end_date - timedelta(days=offset)
    
    return comp_start, comp_end


def format_date_range(start_date: datetime, end_date: datetime) -> str:
    """
    Format date range for display
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Formatted string
    """
    return f"{start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"


def get_period_label(granularity: Literal["daily", "weekly", "monthly"]) -> str:
    """
    Get Italian label for granularity
    
    Args:
        granularity: Time granularity
        
    Returns:
        Italian label
    """
    labels = {
        "daily": "Giornaliero",
        "weekly": "Settimanale",
        "monthly": "Mensile"
    }
    return labels.get(granularity, granularity)
