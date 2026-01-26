"""
Example Configuration Files for Content Scoring
================================================

This module provides example configurations for different use cases.
"""

from etl.content_scoring import ContentScoringConfig


# ============================================================================
# Standard Configuration (Default)
# ============================================================================

STANDARD_CONFIG = ContentScoringConfig(
    # Weights: Focus balanced on reach, loyalty, and efficiency
    reach_weight=0.30,
    loyalty_weight=0.40,
    efficiency_weight=0.30,
    
    # Data quality thresholds
    min_views_threshold=10,
    impute_nulls=True,
    null_value=0.0,
    
    # Normalization
    log_transform_views=True,
    normalize_method='minmax',
    scale_range=(0, 100),
    
    # Segmentation thresholds
    top_performer_threshold=80.0,
    underperforming_threshold=40.0,
    high_engagement_threshold=0.50,
    low_traffic_threshold=100,
    
    # Output
    include_component_scores=True
)


# ============================================================================
# Quality-Focused Configuration
# ============================================================================
# Use this when engagement quality is more important than volume

QUALITY_FOCUSED_CONFIG = ContentScoringConfig(
    # Higher weight on loyalty and efficiency
    reach_weight=0.20,
    loyalty_weight=0.50,
    efficiency_weight=0.30,
    
    # Stricter thresholds
    min_views_threshold=25,
    high_engagement_threshold=0.60,
    
    # Segmentation favors quality
    top_performer_threshold=75.0,
    underperforming_threshold=35.0,
)


# ============================================================================
# Volume-Focused Configuration
# ============================================================================
# Use this when reach and traffic volume are the primary goals

VOLUME_FOCUSED_CONFIG = ContentScoringConfig(
    # Higher weight on reach
    reach_weight=0.50,
    loyalty_weight=0.30,
    efficiency_weight=0.20,
    
    # Lower quality thresholds
    min_views_threshold=5,
    high_engagement_threshold=0.30,
    
    # More aggressive log scaling for views
    log_transform_views=True,
)


# ============================================================================
# Viral Content Configuration
# ============================================================================
# Optimized for identifying potential viral content

VIRAL_CONFIG = ContentScoringConfig(
    # Emphasis on reach and engagement
    reach_weight=0.45,
    loyalty_weight=0.45,
    efficiency_weight=0.10,
    
    # Low threshold to catch emerging content
    min_views_threshold=1,
    
    # Different segmentation for viral detection
    top_performer_threshold=85.0,
    high_engagement_threshold=0.55,
    low_traffic_threshold=50,
)


# ============================================================================
# SEO-Focused Configuration
# ============================================================================
# For content that prioritizes search engine performance

SEO_FOCUSED_CONFIG = ContentScoringConfig(
    # Balance between reach and efficiency (low bounce = good SEO)
    reach_weight=0.35,
    loyalty_weight=0.25,
    efficiency_weight=0.40,
    
    # Higher quality bar
    min_views_threshold=20,
    
    # Segmentation emphasizes efficiency
    top_performer_threshold=80.0,
    underperforming_threshold=45.0,
)


# ============================================================================
# Niche/Specialized Content Configuration
# ============================================================================
# For specialized content with smaller but engaged audiences

NICHE_CONFIG = ContentScoringConfig(
    # Strongly favor engagement over volume
    reach_weight=0.15,
    loyalty_weight=0.60,
    efficiency_weight=0.25,
    
    # Very low traffic threshold (niche audiences)
    min_views_threshold=3,
    low_traffic_threshold=30,
    
    # Adjusted segmentation for niche content
    high_engagement_threshold=0.70,
    top_performer_threshold=75.0,
)


# ============================================================================
# Experimental/Testing Configuration
# ============================================================================
# For testing new weighting schemes

EXPERIMENTAL_CONFIG = ContentScoringConfig(
    reach_weight=0.33,
    loyalty_weight=0.34,
    efficiency_weight=0.33,
    
    # Strict validation
    min_views_threshold=15,
    
    # More granular scoring
    scale_range=(0, 1000),
    
    # Include all diagnostic columns
    include_component_scores=True,
)


# ============================================================================
# Configuration Selection Helper
# ============================================================================

AVAILABLE_CONFIGS = {
    'standard': STANDARD_CONFIG,
    'quality': QUALITY_FOCUSED_CONFIG,
    'volume': VOLUME_FOCUSED_CONFIG,
    'viral': VIRAL_CONFIG,
    'seo': SEO_FOCUSED_CONFIG,
    'niche': NICHE_CONFIG,
    'experimental': EXPERIMENTAL_CONFIG,
}


def get_config(config_name: str = 'standard') -> ContentScoringConfig:
    """
    Get a predefined configuration by name.
    
    Args:
        config_name: Name of configuration ('standard', 'quality', 'volume', 
                     'viral', 'seo', 'niche', 'experimental')
    
    Returns:
        ContentScoringConfig instance
        
    Raises:
        ValueError: If config_name is not recognized
    """
    if config_name not in AVAILABLE_CONFIGS:
        raise ValueError(
            f"Unknown config '{config_name}'. "
            f"Available configs: {list(AVAILABLE_CONFIGS.keys())}"
        )
    
    return AVAILABLE_CONFIGS[config_name]


# ============================================================================
# Usage Examples
# ============================================================================

"""
Example 1: Use a predefined configuration
------------------------------------------

from etl.content_scoring import ContentScoreCalculator
from etl.content_scoring.examples import get_config

# Get quality-focused config
config = get_config('quality')

# Use it with calculator
calculator = ContentScoreCalculator(config=config)
scored_df = calculator.calculate(df)


Example 2: Customize an existing configuration
------------------------------------------------

from etl.content_scoring import ContentScoringConfig
from etl.content_scoring.examples import QUALITY_FOCUSED_CONFIG

# Start with quality config and customize
custom_config = ContentScoringConfig(
    **{**QUALITY_FOCUSED_CONFIG.to_dict(), 
       'reach_weight': 0.25,  # Adjust this weight
       'min_views_threshold': 30}  # And this threshold
)

calculator = ContentScoreCalculator(config=custom_config)


Example 3: Create your own configuration
-----------------------------------------

from etl.content_scoring import ContentScoringConfig, ContentScoreCalculator

my_config = ContentScoringConfig(
    reach_weight=0.40,
    loyalty_weight=0.35,
    efficiency_weight=0.25,
    min_views_threshold=15,
    top_performer_threshold=82.0,
)

calculator = ContentScoreCalculator(config=my_config)
scored_df = calculator.calculate(df)


Example 4: Load configuration from file
----------------------------------------

import json
from etl.content_scoring import ContentScoringConfig

# Save config to JSON
with open('my_config.json', 'w') as f:
    json.dump(my_config.to_dict(), f, indent=2)

# Load config from JSON
with open('my_config.json', 'r') as f:
    config_dict = json.load(f)

loaded_config = ContentScoringConfig.from_dict(config_dict)
calculator = ContentScoreCalculator(config=loaded_config)
"""
