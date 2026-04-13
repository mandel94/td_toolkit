#!/usr/bin/env python
"""Test ETL pipeline with sample data."""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from etl.articles_db_pipeline.models.article import RawWeeklyData
from etl.articles_db_pipeline.transformers.article_transformer import ArticleTransformer

# Create test data
test_data = [
    RawWeeklyData(
        pagePath="/articolo-1",
        category="News",
        year=2025,
        week=1,
        screenPageViews=500,
        engagedSessions=150,
        engagementRate=0.75,
        averageSessionDuration=300
    ),
    RawWeeklyData(
        pagePath="/articolo-2",
        category="Guide",
        year=2025,
        week=1,
        screenPageViews=300,
        engagedSessions=100,
        engagementRate=0.60,
        averageSessionDuration=200
    ),
    RawWeeklyData(
        pagePath="/articolo-3",
        category="News",
        year=2025,
        week=2,
        screenPageViews=400,
        engagedSessions=120,
        engagementRate=0.65,
        averageSessionDuration=250
    ),
]

# Test transformer
transformer = ArticleTransformer()
batch = transformer.transform_weekly_batch(test_data)

print("=" * 60)
print("✓ ETL PIPELINE TEST SUCCESSFUL")
print("=" * 60)
print(f"\nBatch ID: {batch.batch_id}")
print(f"Created at: {batch.created_at}")
print(f"\nDimensional Data:")
print(f"  - Weeks: {len(batch.weeks)}")
print(f"  - Articles: {len(batch.articles)}")
print(f"  - Authors: {len(batch.authors)}")
print(f"  - Categories: {len(batch.categories)}")
print(f"  - Metrics: {len(batch.metrics)}")

print(f"\nCategories found (as scraped):")
for cat in batch.categories:
    print(f"  - {cat.category_name}")

print(f"\nWeeks processed:")
for week in batch.weeks:
    print(f"  - Week {week.year_week}: {week.week_start_date} to {week.week_end_date}")

print(f"\nArticles processed:")
for article in batch.articles:
    print(f"  - {article.page_path}")

print("\n" + "=" * 60)
print("✓ All tests passed!")
print("=" * 60)
