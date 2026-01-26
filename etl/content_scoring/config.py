"""
Content Scoring Configuration
==============================

Editorial ranking system configuration.
Optimized for stable, robust ranking across batches.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional


@dataclass
class ContentScoringConfig:
    """
    Configuration for editorial ranking system.
    
    Produces rank-based scores (0-100) stable across batches.
    Features are orthogonal to avoid double counting.
    
    Design Pattern: Strategy Pattern via strategy_name parameter.
    Allows switching between different weighting strategies dynamically.
    """
    
    # Weighting strategy (Strategy Pattern)
    # Options: 'balanced', 'quality', 'volume', 'engagement', 'deep-dive', 'viral', 'custom'
    strategy_name: str = 'balanced'
    
    # Feature weights (only used if strategy_name='custom', must sum to 1.0)
    reach_weight: float = 0.35
    engagement_weight: float = 0.35
    depth_weight: float = 0.30
    
    # Metric mappings
    metrics_mapping: Dict[str, str] = field(default_factory=lambda: {
        'views': 'screenPageViews',
        'engagement_rate': 'engagementRate',
        'session_duration': 'averageSessionDuration'
    })
    
    # Feature engineering
    log_transform_views: bool = True
    
    # Outlier handling (winsorization)
    winsorize_enabled: bool = True
    lower_percentile: float = 0.05
    upper_percentile: float = 0.95
    
    # Missing data handling
    missing_rank_percentile: float = 0.50
    
    # Missing data handling
    missing_rank_percentile: float = 0.50
    
    # Domain validation
    engagement_min: float = 0.0
    engagement_max: float = 1.0
    duration_min: float = 0.0
    views_min: int = 0
    
    # Output settings
    score_column_name: str = 'editorial_score'
    rank_column_name: str = 'editorial_rank'
    segment_column_name: str = 'content_segment'
    include_feature_ranks: bool = True
    
    # Segmentation thresholds (percentile-based)
    top_performer_percentile: float = 0.80
    underperforming_percentile: float = 0.40
    high_engagement_percentile: float = 0.70
    low_traffic_percentile: float = 0.30
    
    def __post_init__(self):
        """Validate configuration."""
        # Validate strategy name
        valid_strategies = [
            'balanced', 'quality', 'volume', 'engagement',
            'deep-dive', 'viral', 'custom'
        ]
        if self.strategy_name not in valid_strategies:
            raise ValueError(
                f"Invalid strategy '{self.strategy_name}'. "
                f"Must be one of: {valid_strategies}"
            )
        
        # Validate weights only for custom strategy
        if self.strategy_name == 'custom':
            total_weight = self.reach_weight + self.engagement_weight + self.depth_weight
            if not (0.99 <= total_weight <= 1.01):
                raise ValueError(
                    f"Weights must sum to 1.0 for custom strategy. Current sum: {total_weight}. "
                    f"Adjust reach_weight ({self.reach_weight}), "
                    f"engagement_weight ({self.engagement_weight}), or "
                    f"depth_weight ({self.depth_weight})"
                )
        
        if not (0.0 <= self.lower_percentile < self.upper_percentile <= 1.0):
            raise ValueError(
                f"Percentiles must satisfy 0 <= lower < upper <= 1. "
                f"Got: lower={self.lower_percentile}, upper={self.upper_percentile}"
            )
        
        if not (0.0 <= self.missing_rank_percentile <= 1.0):
            raise ValueError(
                f"missing_rank_percentile must be in [0, 1]. "
                f"Got: {self.missing_rank_percentile}"
            )
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ContentScoringConfig':
        """Create configuration from dictionary."""
        return cls(**config_dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'reach_weight': self.reach_weight,
            'engagement_weight': self.engagement_weight,
            'depth_weight': self.depth_weight,
            'metrics_mapping': self.metrics_mapping,
            'log_transform_views': self.log_transform_views,
            'winsorize_enabled': self.winsorize_enabled,
            'lower_percentile': self.lower_percentile,
            'upper_percentile': self.upper_percentile,
            'missing_rank_percentile': self.missing_rank_percentile,
            'engagement_min': self.engagement_min,
            'engagement_max': self.engagement_max,
            'duration_min': self.duration_min,
            'views_min': self.views_min,
            'score_column_name': self.score_column_name,
            'rank_column_name': self.rank_column_name,
            'segment_column_name': self.segment_column_name,
            'include_feature_ranks': self.include_feature_ranks,
            'top_performer_percentile': self.top_performer_percentile,
            'underperforming_percentile': self.underperforming_percentile,
            'high_engagement_percentile': self.high_engagement_percentile,
            'low_traffic_percentile': self.low_traffic_percentile,
        }
    
    def get_metric_name(self, metric_type: str) -> str:
        """Get the actual column name for a metric type."""
        return self.metrics_mapping.get(metric_type, metric_type)


DEFAULT_CONFIG = ContentScoringConfig()

