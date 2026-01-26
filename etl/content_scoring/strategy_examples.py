"""
Weighting Strategy Usage Examples
==================================

This file demonstrates how to use different weighting strategies
with the content scoring system.

Design Patterns Demonstrated:
- Strategy Pattern: Runtime selection of different algorithms
- Factory Pattern: Centralized object creation
"""

import pandas as pd
from etl.content_scoring import (
    ContentScoreCalculator,
    ContentScoringConfig,
    create_strategy,
    list_available_strategies
)


def example_1_list_strategies():
    """Example 1: List all available strategies."""
    print("=" * 70)
    print("EXAMPLE 1: List Available Strategies")
    print("=" * 70)
    
    list_available_strategies()


def example_2_balanced_strategy():
    """Example 2: Use balanced strategy (default)."""
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Balanced Strategy (Default)")
    print("=" * 70)
    
    # Create sample data
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    # Use balanced strategy (default)
    config = ContentScoringConfig(strategy_name='balanced')
    calculator = ContentScoreCalculator(config)
    
    result = calculator.calculate(df)
    print("\nTop 3 articles (Balanced Strategy):")
    print(result[['editorial_rank', 'editorial_score']].head(3))


def example_3_quality_focused():
    """Example 3: Quality-focused strategy for premium content."""
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Quality-Focused Strategy")
    print("=" * 70)
    print("Best for: In-depth reviews, analysis, brand-building content")
    
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    # Quality-focused: emphasizes engagement (45%) and depth (35%)
    config = ContentScoringConfig(strategy_name='quality')
    calculator = ContentScoreCalculator(config)
    
    result = calculator.calculate(df)
    print("\nTop 3 articles (Quality-Focused):")
    print(result[['editorial_rank', 'editorial_score']].head(3))


def example_4_volume_focused():
    """Example 4: Volume-focused strategy for traffic objectives."""
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Volume-Focused Strategy")
    print("=" * 70)
    print("Best for: Breaking news, trending topics, SEO-driven content")
    
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    # Volume-focused: emphasizes reach (55%)
    config = ContentScoringConfig(strategy_name='volume')
    calculator = ContentScoreCalculator(config)
    
    result = calculator.calculate(df)
    print("\nTop 3 articles (Volume-Focused):")
    print(result[['editorial_rank', 'editorial_score']].head(3))


def example_5_engagement_driven():
    """Example 5: Engagement-driven for viral potential."""
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Engagement-Driven Strategy")
    print("=" * 70)
    print("Best for: Social media content, viral potential")
    
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    # Engagement-driven: maximum emphasis on engagement (55%)
    config = ContentScoringConfig(strategy_name='engagement')
    calculator = ContentScoreCalculator(config)
    
    result = calculator.calculate(df)
    print("\nTop 3 articles (Engagement-Driven):")
    print(result[['editorial_rank', 'editorial_score']].head(3))


def example_6_deep_dive():
    """Example 6: Deep-dive strategy for long-form content."""
    print("\n" + "=" * 70)
    print("EXAMPLE 6: Deep-Dive Strategy")
    print("=" * 70)
    print("Best for: Investigations, documentaries, essays")
    
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    # Deep-dive: maximum emphasis on depth (45%)
    config = ContentScoringConfig(strategy_name='deep-dive')
    calculator = ContentScoreCalculator(config)
    
    result = calculator.calculate(df)
    print("\nTop 3 articles (Deep-Dive):")
    print(result[['editorial_rank', 'editorial_score']].head(3))


def example_7_viral_optimized():
    """Example 7: Viral-optimized for shareability."""
    print("\n" + "=" * 70)
    print("EXAMPLE 7: Viral-Optimized Strategy")
    print("=" * 70)
    print("Best for: Memes, lists, quick reads")
    
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    # Viral-optimized: reach and engagement (45% each), minimal depth (10%)
    config = ContentScoringConfig(strategy_name='viral')
    calculator = ContentScoreCalculator(config)
    
    result = calculator.calculate(df)
    print("\nTop 3 articles (Viral-Optimized):")
    print(result[['editorial_rank', 'editorial_score']].head(3))


def example_8_custom_strategy():
    """Example 8: Custom strategy with user-defined weights."""
    print("\n" + "=" * 70)
    print("EXAMPLE 8: Custom Strategy")
    print("=" * 70)
    print("Custom weights: Reach=40%, Engagement=40%, Depth=20%")
    
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    # Custom strategy with specific weights
    config = ContentScoringConfig(
        strategy_name='custom',
        reach_weight=0.40,
        engagement_weight=0.40,
        depth_weight=0.20
    )
    calculator = ContentScoreCalculator(config)
    
    result = calculator.calculate(df)
    print("\nTop 3 articles (Custom Strategy):")
    print(result[['editorial_rank', 'editorial_score']].head(3))


def example_9_compare_strategies():
    """Example 9: Compare different strategies on same data."""
    print("\n" + "=" * 70)
    print("EXAMPLE 9: Strategy Comparison")
    print("=" * 70)
    
    # Sample data
    data = {
        'screenPageViews': [10000, 5000, 8000],
        'engagementRate': [0.45, 0.85, 0.65],
        'averageSessionDuration': [90, 240, 150]
    }
    df = pd.DataFrame(data)
    
    strategies = ['balanced', 'quality', 'volume', 'engagement']
    
    print("\nArticle Rankings Across Different Strategies:")
    print("-" * 70)
    
    for strategy_name in strategies:
        config = ContentScoringConfig(strategy_name=strategy_name)
        calculator = ContentScoreCalculator(config)
        result = calculator.calculate(df)
        
        print(f"\n{strategy_name.upper()} Strategy:")
        weights = calculator.strategy.get_weights()
        print(f"  Weights: R={weights['reach']:.0%}, E={weights['engagement']:.0%}, D={weights['depth']:.0%}")
        print(f"  Rankings: {result['editorial_rank'].tolist()}")
        print(f"  Scores: {result['editorial_score'].tolist()}")


def example_10_switch_strategy_runtime():
    """Example 10: Switch strategies at runtime."""
    print("\n" + "=" * 70)
    print("EXAMPLE 10: Runtime Strategy Switching")
    print("=" * 70)
    print("Design Pattern: Strategy Pattern allows runtime algorithm changes")
    
    data = {
        'screenPageViews': [5000, 10000, 3000],
        'engagementRate': [0.70, 0.60, 0.80],
        'averageSessionDuration': [150, 120, 200]
    }
    df = pd.DataFrame(data)
    
    # Start with balanced strategy
    calculator = ContentScoreCalculator(
        ContentScoringConfig(strategy_name='balanced')
    )
    
    print("\n1. Initially using BALANCED strategy:")
    result1 = calculator.calculate(df.copy())
    print(f"   Top article rank: {result1['editorial_rank'].min()}")
    
    # Switch to quality strategy
    calculator.config.strategy_name = 'quality'
    calculator._initialize_strategy()
    
    print("\n2. Switched to QUALITY strategy:")
    result2 = calculator.calculate(df.copy())
    print(f"   Top article rank: {result2['editorial_rank'].min()}")
    
    print("\n✅ Strategy successfully changed at runtime!")


# =============================================================================
# Main Execution
# =============================================================================

if __name__ == "__main__":
    print("\n")
    print("🎯 " * 35)
    print("WEIGHTING STRATEGY EXAMPLES")
    print("Design Patterns: Strategy Pattern + Factory Pattern")
    print("🎯 " * 35)
    
    # Run all examples
    example_1_list_strategies()
    example_2_balanced_strategy()
    example_3_quality_focused()
    example_4_volume_focused()
    example_5_engagement_driven()
    example_6_deep_dive()
    example_7_viral_optimized()
    example_8_custom_strategy()
    example_9_compare_strategies()
    example_10_switch_strategy_runtime()
    
    print("\n" + "=" * 70)
    print("✅ All examples completed successfully!")
    print("=" * 70)
