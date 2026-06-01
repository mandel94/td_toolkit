"""
Content Scoring Module
======================

This module implements a modular, configurable system for calculating
editorial content scores based on multiple engagement metrics.

Design Patterns Implemented:
- Strategy Pattern: Flexible, interchangeable weighting strategies
- Factory Pattern: Centralized strategy creation and management

Key Features:
- Flexible data input (DataFrame, dict, file paths)
- Multiple weighting strategies (balanced, quality-focused, volume-focused, etc.)
- Automatic normalization and scaling
- Article segmentation and categorization
- Anomaly detection and validation

Usage:
    from etl.content_scoring import ContentScoreCalculator, create_strategy
    
    # Use predefined strategy
    calculator = ContentScoreCalculator(config=ContentScoringConfig(strategy_name='quality'))
    scored_df = calculator.calculate(df)
    
    # Or create custom strategy
    strategy = create_strategy('custom', reach_weight=0.4, engagement_weight=0.4, depth_weight=0.2)
"""

__version__ = "2.0.0"  # Strategy Pattern implementation
__author__ = "Analytics Team"

from .calculator import ContentScoreCalculator
from .segmentation import ContentScoreSegmentation
from .config import ContentScoringConfig, DEFAULT_CONFIG
from .validators import ContentScoreValidator
from .ga4_score_config import Ga4ScoringConfig, DEFAULT_GA4_CONFIG
from .ga4_score_calculator import Ga4EditorialScoreCalculator
from .weighting_strategies import (
    WeightingStrategy,
    WeightingStrategyFactory,
    create_strategy,
    list_available_strategies,
    BalancedStrategy,
    QualityFocusedStrategy,
    VolumeFocusedStrategy,
    EngagementDrivenStrategy,
    DeepDiveStrategy,
    ViralOptimizedStrategy,
    CustomStrategy
)

__all__ = [
    "ContentScoreCalculator",
    "ContentScoreSegmentation",
    "ContentScoringConfig",
    "ContentScoreValidator",
    "DEFAULT_CONFIG",
    "WeightingStrategy",
    "WeightingStrategyFactory",
    "create_strategy",
    "list_available_strategies",
    "BalancedStrategy",
    "QualityFocusedStrategy",
    "VolumeFocusedStrategy",
    "EngagementDrivenStrategy",
    "DeepDiveStrategy",
    "ViralOptimizedStrategy",
    "CustomStrategy",
]
