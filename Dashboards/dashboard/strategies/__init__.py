"""Strategies package"""
from .aggregation import (
    AggregationStrategy,
    DailyAggregation,
    WeeklyAggregation,
    MonthlyAggregation,
    AggregationFactory
)

__all__ = [
    "AggregationStrategy",
    "DailyAggregation",
    "WeeklyAggregation",
    "MonthlyAggregation",
    "AggregationFactory"
]
