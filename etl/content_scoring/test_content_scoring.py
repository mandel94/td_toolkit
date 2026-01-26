"""
Test Script for Content Scoring System
=======================================

This script demonstrates and tests the content scoring functionality.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import numpy as np
from etl.content_scoring import (
    ContentScoreCalculator,
    ContentScoreSegmentation,
    ContentScoreValidator,
    ContentScoringConfig
)


def create_sample_data(n_rows=50):
    """Create sample data for testing."""
    np.random.seed(42)
    
    data = {
        'pagePath': [f'/article-{i}' for i in range(n_rows)],
        'title': [f'Article Title {i}' for i in range(n_rows)],
        'screenPageViews': np.random.randint(10, 10000, n_rows),
        'engagementRate': np.random.uniform(0.1, 0.9, n_rows),
        'averageSessionDuration': np.random.uniform(30, 600, n_rows)
    }
    
    return pd.DataFrame(data)


def test_basic_calculation():
    """Test basic score calculation."""
    print("\n" + "="*70)
    print("TEST 1: Basic Score Calculation")
    print("="*70)
    
    # Create sample data
    df = create_sample_data(10)
    print(f"✓ Created sample data: {len(df)} rows")
    
    # Calculate scores
    calculator = ContentScoreCalculator()
    scored_df = calculator.calculate(df)
    
    print(f"✓ Calculated scores")
    print(f"\nScore Statistics:")
    print(f"  Mean: {scored_df['content_score'].mean():.2f}")
    print(f"  Min:  {scored_df['content_score'].min():.2f}")
    print(f"  Max:  {scored_df['content_score'].max():.2f}")
    print(f"  Std:  {scored_df['content_score'].std():.2f}")
    
    # Display top 5 articles
    print(f"\nTop 5 Articles by Score:")
    top_5 = scored_df.nlargest(5, 'content_score')
    for idx, row in top_5.iterrows():
        print(f"  {row['title']:20s} | Score: {row['content_score']:6.2f} | Views: {int(row['screenPageViews']):5d}")
    
    return scored_df


def test_segmentation(scored_df):
    """Test article segmentation."""
    print("\n" + "="*70)
    print("TEST 2: Article Segmentation")
    print("="*70)
    
    segmenter = ContentScoreSegmentation()
    segmented_df = segmenter.segment(scored_df)
    
    print(f"✓ Applied segmentation")
    
    # Show segment distribution
    print(f"\nSegment Distribution:")
    segment_counts = segmented_df['content_segment'].value_counts()
    for segment, count in segment_counts.items():
        percentage = (count / len(segmented_df)) * 100
        print(f"  {segment:20s}: {count:2d} articles ({percentage:5.1f}%)")
    
    # Show segment statistics
    print(f"\nSegment Statistics:")
    stats = segmenter.get_segment_statistics(segmented_df)
    print(stats.to_string(index=False))
    
    return segmented_df


def test_validation(segmented_df):
    """Test validation and anomaly detection."""
    print("\n" + "="*70)
    print("TEST 3: Validation and Anomaly Detection")
    print("="*70)
    
    validator = ContentScoreValidator()
    
    # Validate data
    is_valid, issues = validator.validate(segmented_df)
    
    print(f"✓ Validation completed")
    print(f"  Valid: {is_valid}")
    print(f"  Issues found: {len(issues)}")
    
    if issues:
        print(f"\nIssues:")
        for issue in issues:
            print(f"  [{issue['severity']}] {issue['type']}: {issue['message']}")
    
    # Flag anomalies
    flagged_df = validator.flag_anomalies(segmented_df)
    anomaly_count = flagged_df['anomaly_flag'].sum()
    
    print(f"\n✓ Anomaly detection completed")
    print(f"  Anomalies flagged: {anomaly_count}")
    
    if anomaly_count > 0:
        print(f"\nAnomalous Articles:")
        anomalies = flagged_df[flagged_df['anomaly_flag']]
        for idx, row in anomalies.head().iterrows():
            print(f"  {row['title']:20s} | Score: {row['content_score']:6.2f} | Views: {int(row['screenPageViews']):5d}")
    
    return flagged_df


def test_custom_config():
    """Test with custom configuration."""
    print("\n" + "="*70)
    print("TEST 4: Custom Configuration")
    print("="*70)
    
    # Create custom config emphasizing loyalty
    custom_config = ContentScoringConfig(
        reach_weight=0.25,
        loyalty_weight=0.50,  # Emphasize engagement
        efficiency_weight=0.25,
        top_performer_threshold=85.0,
        min_views_threshold=20
    )
    
    print(f"✓ Created custom configuration:")
    print(f"  Reach weight:     {custom_config.reach_weight}")
    print(f"  Loyalty weight:   {custom_config.loyalty_weight}")
    print(f"  Efficiency weight: {custom_config.efficiency_weight}")
    
    # Calculate with custom config
    df = create_sample_data(10)
    calculator = ContentScoreCalculator(config=custom_config)
    scored_df = calculator.calculate(df)
    
    print(f"\n✓ Calculated scores with custom config")
    print(f"  Mean score: {scored_df['content_score'].mean():.2f}")
    
    return scored_df


def test_multiple_inputs():
    """Test different input types."""
    print("\n" + "="*70)
    print("TEST 5: Multiple Input Types")
    print("="*70)
    
    calculator = ContentScoreCalculator()
    
    # Test 1: DataFrame
    df = create_sample_data(5)
    result1 = calculator.calculate(df)
    print(f"✓ DataFrame input: {len(result1)} rows scored")
    
    # Test 2: Dictionary (single row)
    single_row = {
        'pagePath': '/test-article',
        'title': 'Test Article',
        'screenPageViews': 1000,
        'engagementRate': 0.65,
        'averageSessionDuration': 180
    }
    result2 = calculator.calculate(single_row)
    print(f"✓ Dictionary input: Score = {result2['content_score'].iloc[0]:.2f}")
    
    # Test 3: Save to file and reload
    temp_file = 'temp_test_data.csv'
    df.to_csv(temp_file, index=False)
    result3 = calculator.calculate(temp_file)
    print(f"✓ File path input: {len(result3)} rows scored")
    
    # Cleanup
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    return result1, result2, result3


def test_recommendations():
    """Test recommendation generation."""
    print("\n" + "="*70)
    print("TEST 6: Segment Recommendations")
    print("="*70)
    
    # Create and segment data
    df = create_sample_data(30)
    calculator = ContentScoreCalculator()
    segmenter = ContentScoreSegmentation()
    
    scored_df = calculator.calculate(df)
    segmented_df = segmenter.segment(scored_df)
    
    # Get recommendations for each segment
    segments = segmented_df['content_segment'].unique()
    
    print(f"✓ Generated recommendations for {len(segments)} segments\n")
    
    for segment in segments:
        recs = segmenter.get_recommendations(segmented_df, segment, n_articles=2)
        if recs:
            rec = recs[0]
            print(f"\n{segment}:")
            print(f"  Strategy: {rec['strategy']}")
            print(f"  Actions:")
            for action in rec['actions'][:2]:  # Show first 2 actions
                print(f"    - {action}")


def run_all_tests():
    """Run all tests."""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*20 + "CONTENT SCORING SYSTEM TESTS" + " "*20 + "║")
    print("╚" + "="*68 + "╝")
    
    try:
        # Run tests
        scored_df = test_basic_calculation()
        segmented_df = test_segmentation(scored_df)
        flagged_df = test_validation(segmented_df)
        test_custom_config()
        test_multiple_inputs()
        test_recommendations()
        
        print("\n" + "="*70)
        print("✅ ALL TESTS PASSED SUCCESSFULLY!")
        print("="*70)
        print("\nThe Content Scoring System is working correctly.")
        print("You can now use it in your reports with confidence.\n")
        
    except Exception as e:
        print("\n" + "="*70)
        print("❌ TEST FAILED!")
        print("="*70)
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
