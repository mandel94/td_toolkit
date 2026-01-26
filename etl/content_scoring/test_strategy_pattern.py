"""
Quick Test: Weighting Strategies
=================================

Quick verification that the Strategy Pattern implementation works correctly.
"""

import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from etl.content_scoring import (
    ContentScoreCalculator,
    ContentScoringConfig,
    create_strategy,
    list_available_strategies
)


def test_strategy_pattern():
    """Test Strategy Pattern implementation."""
    print("=" * 70)
    print("Testing Strategy Pattern Implementation")
    print("=" * 70)
    
    # Create sample data
    data = {
        'screenPageViews': [1000, 5000, 500, 10000, 2000],
        'engagementRate': [0.65, 0.75, 0.45, 0.80, 0.55],
        'averageSessionDuration': [120, 180, 60, 240, 90]
    }
    df = pd.DataFrame(data)
    
    print("\n✓ Sample data created")
    
    # Test 1: List strategies
    print("\n1. Testing strategy listing...")
    list_available_strategies()
    print("✓ Strategy listing works")
    
    # Test 2: Balanced strategy
    print("\n2. Testing Balanced strategy...")
    config = ContentScoringConfig(strategy_name='balanced')
    calculator = ContentScoreCalculator(config)
    result = calculator.calculate(df)
    print(f"✓ Balanced strategy: mean score = {result['editorial_score'].mean():.2f}")
    
    # Test 3: Quality strategy
    print("\n3. Testing Quality-Focused strategy...")
    config = ContentScoringConfig(strategy_name='quality')
    calculator = ContentScoreCalculator(config)
    result = calculator.calculate(df)
    print(f"✓ Quality strategy: mean score = {result['editorial_score'].mean():.2f}")
    
    # Test 4: Volume strategy
    print("\n4. Testing Volume-Focused strategy...")
    config = ContentScoringConfig(strategy_name='volume')
    calculator = ContentScoreCalculator(config)
    result = calculator.calculate(df)
    print(f"✓ Volume strategy: mean score = {result['editorial_score'].mean():.2f}")
    
    # Test 5: Custom strategy
    print("\n5. Testing Custom strategy...")
    config = ContentScoringConfig(
        strategy_name='custom',
        reach_weight=0.40,
        engagement_weight=0.40,
        depth_weight=0.20
    )
    calculator = ContentScoreCalculator(config)
    result = calculator.calculate(df)
    print(f"✓ Custom strategy: mean score = {result['editorial_score'].mean():.2f}")
    
    # Test 6: Strategy switching
    print("\n6. Testing runtime strategy switching...")
    calculator = ContentScoreCalculator(
        ContentScoringConfig(strategy_name='balanced')
    )
    result1 = calculator.calculate(df.copy())
    
    calculator.config.strategy_name = 'quality'
    calculator._initialize_strategy()
    result2 = calculator.calculate(df.copy())
    
    assert not result1['editorial_score'].equals(result2['editorial_score'])
    print("✓ Runtime strategy switching works")
    
    # Test 7: Verify weights
    print("\n7. Testing strategy weights...")
    balanced = create_strategy('balanced')
    weights = balanced.get_weights()
    assert abs(sum(weights.values()) - 1.0) < 0.01
    print(f"✓ Weights sum to 1.0: {weights}")
    
    print("\n" + "=" * 70)
    print("✅ All tests passed! Strategy Pattern working correctly.")
    print("=" * 70)
    
    return True


if __name__ == "__main__":
    try:
        test_strategy_pattern()
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
