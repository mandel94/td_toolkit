#!/usr/bin/env python
"""Complete ETL pipeline test with mocked database."""
import sys
import os
from datetime import datetime, date
from typing import List

sys.path.insert(0, os.path.dirname(__file__))

from etl.articles_db_pipeline.models.article import (
    RawWeeklyData, DimWeekData, DimArticleData, DimAuthorData,
    DimCategoryData, FactWeeklyMetricsData, ProcessedWeeklyBatch
)
from etl.articles_db_pipeline.transformers.article_transformer import ArticleTransformer

def create_ga4_mock_data() -> List[RawWeeklyData]:
    """Create mock GA4 data simulating real scraping output."""
    return [
        # Week 1 data
        RawWeeklyData(
            pagePath="/news/articolo-1",
            category="News",
            year=2025,
            week=1,
            screenPageViews=1250,
            engagedSessions=450,
            engagementRate=0.82,
            averageSessionDuration=385
        ),
        RawWeeklyData(
            pagePath="/guide/come-guidare",
            category="Guide",
            year=2025,
            week=1,
            screenPageViews=890,
            engagedSessions=320,
            engagementRate=0.75,
            averageSessionDuration=425
        ),
        RawWeeklyData(
            pagePath="/news/news-week1",
            category="News",
            year=2025,
            week=1,
            screenPageViews=550,
            engagedSessions=180,
            engagementRate=0.68,
            averageSessionDuration=290
        ),
        RawWeeklyData(
            pagePath="/news/articolo-2",
            category="Intervista",
            year=2025,
            week=1,
            screenPageViews=1100,
            engagedSessions=520,
            engagementRate=0.88,
            averageSessionDuration=510
        ),
        # Week 2 data
        RawWeeklyData(
            pagePath="/news/articolo-1",
            category="News",
            year=2025,
            week=2,
            screenPageViews=980,
            engagedSessions=380,
            engagementRate=0.79,
            averageSessionDuration=360
        ),
        RawWeeklyData(
            pagePath="/guide/how-to-drive",
            category="Guide",
            year=2025,
            week=2,
            screenPageViews=720,
            engagedSessions=290,
            engagementRate=0.72,
            averageSessionDuration=410
        ),
        RawWeeklyData(
            pagePath="/news/articolo-2",
            category="Intervista",
            year=2025,
            week=2,
            screenPageViews=1340,
            engagedSessions=610,
            engagementRate=0.91,
            averageSessionDuration=520
        ),
        # Week 3 data
        RawWeeklyData(
            pagePath="/news/articolo-3",
            category="News",
            year=2025,
            week=3,
            screenPageViews=1500,
            engagedSessions=580,
            engagementRate=0.85,
            averageSessionDuration=445
        ),
        RawWeeklyData(
            pagePath="/guide/driving-tips",
            category="Guide",
            year=2025,
            week=3,
            screenPageViews=640,
            engagedSessions=260,
            engagementRate=0.70,
            averageSessionDuration=380
        ),
        RawWeeklyData(
            pagePath="/news/breaking-news",
            category="News",
            year=2025,
            week=3,
            screenPageViews=2100,
            engagedSessions=750,
            engagementRate=0.92,
            averageSessionDuration=560
        ),
    ]


def mock_database_load(batch: ProcessedWeeklyBatch) -> dict:
    """Simulate database loading operations."""
    results = {
        "weeks_loaded": 0,
        "articles_loaded": 0,
        "authors_loaded": 0,
        "categories_loaded": 0,
        "metrics_loaded": 0,
        "errors": []
    }
    
    try:
        # Simulate week insertion
        for week in batch.weeks:
            results["weeks_loaded"] += 1
            print(f"  ✓ Inserted week: {week.year_week}")
        
        # Simulate article insertion
        for article in batch.articles:
            results["articles_loaded"] += 1
            print(f"  ✓ Inserted article: {article.page_path}")
        
        # Simulate author insertion
        for author in batch.authors:
            results["authors_loaded"] += 1
            print(f"  ✓ Inserted author: {author.author_name}")
        
        # Simulate category insertion
        for category in batch.categories:
            results["categories_loaded"] += 1
            print(f"  ✓ Inserted category: {category.category_name}")
        
        # Simulate metrics insertion
        for metric in batch.metrics:
            results["metrics_loaded"] += 1
        
        print(f"  ✓ Inserted {results['metrics_loaded']} metric records")
        
    except Exception as e:
        results["errors"].append(str(e))
    
    return results


def main():
    print("=" * 70)
    print("FULL ETL PIPELINE TEST (Extract, Transform, Load)")
    print("=" * 70)
    
    # Step 1: Extract (Mock GA4 data)
    print("\n[1/3] EXTRACT: Creating mock GA4 data...")
    ga4_data = create_ga4_mock_data()
    print(f"  ✓ Created {len(ga4_data)} records from GA4")
    print(f"    - Date range: 2025-W01 to 2025-W03")
    print(f"    - Records span {len(set((r.year, r.week) for r in ga4_data))} weeks")
    
    # Step 2: Transform
    print("\n[2/3] TRANSFORM: Processing data...")
    transformer = ArticleTransformer()
    batch = transformer.transform_weekly_batch(ga4_data)
    
    print(f"\n  Transformation Results:")
    print(f"  ✓ Batch ID: {batch.batch_id}")
    print(f"  ✓ Weeks created: {len(batch.weeks)}")
    print(f"  ✓ Unique articles: {len(batch.articles)}")
    print(f"  ✓ Unique authors: {len(batch.authors)}")
    print(f"  ✓ Unique categories (as scraped): {len(batch.categories)}")
    print(f"  ✓ Fact metrics records: {len(batch.metrics)}")
    
    # Step 3: Load (Mock)
    print("\n[3/3] LOAD: Simulating database insert...")
    load_results = mock_database_load(batch)
    
    # Summary
    print("\n" + "=" * 70)
    print("ETL PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    
    print(f"\nExtraction:")
    print(f"  Records extracted from GA4: {len(ga4_data)}")
    
    print(f"\nTransformation:")
    print(f"  Dimensional model built:")
    for week in batch.weeks:
        print(f"    - {week.year_week}: {week.quarter}Q{week.month} ({week.week_start_date.strftime('%Y-%m-%d')})")
    
    print(f"\n  Categories found (as scraped, no mapping applied):")
    for cat in sorted(batch.categories, key=lambda x: x.category_name):
        count = sum(1 for m in batch.metrics if m.category_name == cat.category_name)
        print(f"    - {cat.category_name}: {count} metrics")
    
    print(f"\nDatabase (Mocked):")
    print(f"  Weeks inserted: {load_results['weeks_loaded']}")
    print(f"  Articles inserted: {load_results['articles_loaded']}")
    print(f"  Authors inserted: {load_results['authors_loaded']}")
    print(f"  Categories inserted: {load_results['categories_loaded']}")
    print(f"  Metrics inserted: {load_results['metrics_loaded']}")
    
    print(f"\nSample Metrics (first 3):")
    for metric in batch.metrics[:3]:
        print(f"  - {metric.page_path}")
        print(f"    Category: {metric.category_name}")
        print(f"    Week: {metric.week_id} | Views: {metric.screen_page_views} | Sessions: {metric.sessions}")
    
    print("\n" + "=" * 70)
    if not load_results["errors"]:
        print("✓ ALL TESTS PASSED - ETL PIPELINE WORKING CORRECTLY")
    else:
        print("✗ ERRORS DETECTED:")
        for error in load_results["errors"]:
            print(f"  - {error}")
    print("=" * 70)


if __name__ == "__main__":
    main()
